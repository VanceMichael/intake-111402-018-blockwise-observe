import {randomBytes} from 'node:crypto';
import {SourceError} from './transport.mjs';

const OBSERVE_MASK = 0xffffff; // RFC 7641：Observe 为 24 位环回序号
const MAX_SZX = 6;

/** 环回 24 位空间上的有符号距离：正值表示 next 比 current 新，负值表示更旧。 */
export function observeDistance(next, current) {
  if (!Number.isInteger(next) || !Number.isInteger(current)) throw new Error('observe number required');
  if (next < 0 || next > OBSERVE_MASK || current < 0 || current > OBSERVE_MASK) throw new Error('observe out of 24-bit range');
  let delta = (next - current) & OBSERVE_MASK;
  if (delta & 0x800000) delta -= 0x1000000;
  return delta;
}

export function isNewerObserve(next, current) {
  if (current === undefined || current === null) return true;
  return observeDistance(next, current) > 0;
}

function blockSize(szx) {
  if (!Number.isInteger(szx) || szx < 0 || szx > MAX_SZX) throw new Error(`invalid szx ${szx}`);
  return 2 ** (szx + 4);
}

/** 分块流出现非连续缺口（例如中途换了分块语义）。 */
export class BlockGapError extends Error {
  constructor(message) {super(message);this.name = 'BlockGapError';}
}

const REASONS = {
  collecting: '表示仍在分块领取中，尚未收齐，不对外发布',
  fresh: '表示已完整收齐且在 Max-Age 有效期内',
  stale: '已收齐的表示超过 Max-Age，且尚未收到新的观察通知',
  cancelled: '服务器明确取消了本资源订阅（4.04/注销），不再有通知',
  empty: '订阅有效但暂未收到任何观察通知'
};

/**
 * 单个 CoAP 观察资源的采集状态机。
 * 身份四元组：订阅令牌 token、环回观察序号 observe、表示 ETag、字节偏移 offset，
 * 只有同一表示的全部分块收齐后才调用 onRecord 发布。
 */
export class ResourceObserver {
  #client;
  #uri;
  #token;
  #pollMs;
  #onRecord;
  #preferredSzx;

  #timer = null;
  #inFlight = false;
  #stopped = false;
  #pollWake = null;

  #serverEpoch;
  #cancelled = false;
  #cancellationReason = null;
  #resubscribing = false;

  #baselineObserve = null;   // 订阅确认响应携带的序号（新订阅的基线，不参与新旧比较）
  #latestObserve = null;     // 已接受的最新观察序号（环回比较）
  #assembly = null;          // 当前正在领取的表示
  #published = null;         // 最近一次完整发布的表示快照
  #duplicates = 0;
  #late = 0;
  #gapRetries = 0;
  #lastError = null;

  #waiters = new Set();

  constructor(client, uri, options = {}) {
    this.#client = client;
    this.#uri = uri;
    if (typeof uri !== 'string' || !uri) throw new Error('resource uri required');
    this.#token = options.token ?? randomBytes(8).toString('hex');
    if (!/^[0-9a-f]{2,16}$/.test(this.#token)) throw new Error('invalid observe token');
    this.#pollMs = options.pollMs ?? 200;
    this.#preferredSzx = options.preferredSzx ?? 0;
    this.#onRecord = options.onRecord ?? (() => {});
  }

  get uri() {return this.#uri;}
  get token() {return this.#token;}

  /** 建立观察订阅并领取首份完整表示；服务器已注销时返回 cancelled 状态而不抛错。 */
  async start() {
    const confirm = await this.#client('/request', {method: 'POST', body: {uri: this.#uri, token: this.#token, observe: 0, block2: {num: 0, szx: this.#preferredSzx}}});
    this.#serverEpoch = confirm.serverEpoch;
    if (confirm.code === '4.04' || confirm.cancelled === true) {
      this.#markCancelled('服务器在订阅确认中返回 4.04');
      return this.status();
    }
    if (confirm.code && confirm.code !== '2.05') throw new Error(`subscription rejected: ${confirm.code}`);
    if (confirm.observe !== undefined) {
      this.#baselineObserve = confirm.observe;
      this.#latestObserve = confirm.observe; // 确认响应即第一版表示，作为环回比较基准
    }
    await this.#runAssembly(confirm);
    this.#schedule(0);
    return this.status();
  }

  /** 等待下一份完整表示发布；停止时以最后一份记录结算。 */
  waitForRecord(timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {this.#waiters.delete(entry);reject(new Error('waitForRecord timeout'));}, timeoutMs);
      const entry = {timer, resolve, reject};
      this.#waiters.add(entry);
    });
  }

  /**
   * 停止领取并显式注销本资源的订阅。注销只带本资源自己的令牌，
   * 不触碰其他资源的订阅；注销失败也不阻塞关闭。
   */
  async stop() {
    this.#stopped = true;
    if (this.#timer) {clearTimeout(this.#timer);this.#timer = null;}
    if (this.#pollWake) {clearTimeout(this.#pollWake);this.#pollWake = null;}
    let deregistered = false;
    if (!this.#cancelled) {
      try {
        const reply = await this.#client('/request', {method: 'POST', body: {uri: this.#uri, token: this.#token, observe: 1}});
        deregistered = reply?.cancelled === true || reply?.code === '2.05';
      } catch (error) {deregistered = false;}
    }
    for (const waiter of this.#waiters) {clearTimeout(waiter.timer);waiter.resolve(this.#published);}
    this.#waiters.clear();
    return {uri: this.#uri, token: this.#token, deregistered, cancelled: this.#cancelled};
  }

  /** 采集端可据此向业务解释当前资料为何可用、陈旧或未收齐。 */
  status() {
    const now = Date.now();
    let phase;
    if (this.#cancelled) phase = 'cancelled';
    else if (this.#assembly) phase = 'collecting';
    else if (!this.#published) phase = 'empty';
    else if (this.#isExpired(this.#published, now)) phase = 'stale';
    else phase = 'fresh';
    return {
      uri: this.#uri,
      token: this.#token,
      serverEpoch: this.#serverEpoch,
      phase,
      reason: phase === 'cancelled' && this.#cancellationReason
        ? `${REASONS.cancelled}：${this.#cancellationReason}`
        : REASONS[phase],
      latestObserve: this.#latestObserve,
      baselineObserve: this.#baselineObserve,
      etag: this.#published?.etag ?? this.#assembly?.etag ?? null,
      maxAge: this.#published?.maxAge ?? this.#assembly?.maxAge ?? null,
      receivedAt: this.#published?.sourceReceivedAt ?? this.#assembly?.sourceReceivedAt ?? null,
      expiresAt: this.#published ? this.#published.expiresAt : null,
      assembly: this.#assembly ? {
        observe: this.#assembly.observe, etag: this.#assembly.etag,
        bytes: this.#assembly.offset, complete: false, gapRetries: this.#gapRetries
      } : null,
      lastRecord: this.#published ? {observe: this.#published.observe, etag: this.#published.etag, bytes: this.#assembly ? undefined : this.#published.bytes} : null,
      counters: {duplicates: this.#duplicates, late: this.#late, gapRetries: this.#gapRetries},
      lastError: this.#lastError,
      stopped: this.#stopped
    };
  }

  #isExpired(representation, now) {
    if (representation.maxAge === undefined || representation.maxAge === null) return false;
    return now >= representation.receivedAtMs + representation.maxAge * 1000;
  }

  #schedule(delay = this.#pollMs) {
    if (this.#stopped || this.#timer) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#poll().catch(error => {this.#lastError = {message:error.message, at:Date.now()};}).finally(() => this.#schedule());
    }, delay);
    this.#timer.unref?.();
  }

  /** 新通知到达或 ETag 前置条件失败时立刻再轮询，不必等下一个节拍。 */
  #wakePoll() {
    if (this.#stopped || this.#timer || this.#pollWake) return;
    this.#pollWake = setTimeout(() => {
      this.#pollWake = null;
      this.#poll().catch(error => {this.#lastError = {message:error.message, at:Date.now()};}).finally(() => this.#schedule(0));
    }, 0);
    this.#pollWake.unref?.();
  }

  async #poll() {
    if (this.#stopped || this.#cancelled || this.#inFlight || this.#resubscribing) return;
    this.#inFlight = true;
    try {
      const after = this.#latestObserve ?? 0;
      let envelope;
      try {
        envelope = await this.#client(`/notifications?after=${after}&token=${encodeURIComponent(this.#token)}`);
      } catch (error) {
        // 服务端会话丢失（409）：以同一令牌重建订阅，而不是把它当成资料陈旧或取消
        if (error instanceof SourceError && error.status === 409) {
          this.#lastError = {message:error.message, at:Date.now(), recovered:'resubscribe'};
          await this.#resubscribe();
          return;
        }
        throw error;
      }
      for (const note of envelope.items ?? []) {
        if (this.#stopped || this.#cancelled) break;
        if (note.token !== this.#token) continue; // 只认领本订阅身份的通知
        if (note.code === '4.04') {this.#markCancelled('观察通知返回 4.04，服务器注销订阅');break;}
        if (this.#serverEpoch !== undefined && note.serverEpoch !== undefined && note.serverEpoch !== this.#serverEpoch) {
          await this.#resubscribe(note.serverEpoch);
          break;
        }
        await this.#acceptNotification(note);
      }
    } finally {this.#inFlight = false;}
  }

  #markCancelled(detail) {
    this.#cancelled = true;
    if (this.#timer) {clearTimeout(this.#timer);this.#timer = null;}
    if (this.#pollWake) {clearTimeout(this.#pollWake);this.#pollWake = null;}
    this.#assembly = null;
    this.#cancellationReason = detail;
  }

  /** 服务器纪元变化或会话丢失时重新订阅；观察序号回绕本身绝不视为重启。 */
  async #resubscribe(newEpoch) {
    if (this.#resubscribing || this.#stopped) return;
    this.#resubscribing = true;
    this.#assembly = null;
    try {
      const confirm = await this.#client('/request', {method: 'POST', body: {uri: this.#uri, token: this.#token, observe: 0, block2: {num: 0, szx: this.#preferredSzx}}});
      if (confirm.code === '4.04' || confirm.cancelled === true) {this.#markCancelled('重新订阅时服务器返回 4.04');return;}
      this.#serverEpoch = confirm.serverEpoch ?? newEpoch ?? this.#serverEpoch;
      this.#baselineObserve = confirm.observe;
      this.#latestObserve = confirm.observe;
      await this.#runAssembly(confirm);
    } finally {this.#resubscribing = false;}
  }

  /** 依据环回序号区分重传、晚到与新表示。 */
  async #acceptNotification(note) {
    if (note.observe === undefined || note.etag === undefined) return;
    const currentEtag = this.#published?.etag ?? this.#assembly?.etag;
    // 同序号同 ETag：订阅确认或已发布表示的重传，不重复领取
    if (this.#latestObserve !== null && note.observe === this.#latestObserve && note.etag === currentEtag) {
      this.#duplicates += 1;return;
    }
    if (!isNewerObserve(note.observe, this.#latestObserve)) {this.#late += 1;return;} // 含回绕后的旧序号
    // 更新比领取更快：显式作废旧表示的半截分块，整体切到新观察
    if (this.#assembly) this.#assembly.superseded = true;
    this.#latestObserve = note.observe;
    try {
      await this.#runAssembly(note);
    } catch (error) {
      this.#lastError = {message:error.message, at:Date.now(), observe:note.observe, etag:note.etag};
      if (this.#assembly?.observe === note.observe) this.#assembly = null;
    }
  }

  /**
   * 领取同一表示（同一 observe+ETag）的全部分块。
   * 分块位置按字节偏移管理：续块请求回显服务器最近响应的 szx，
   * 块号取响应块号 + 1（块号按响应尺寸解释）；每块的字节偏移必须与已收齐长度
   * 严格连续，服务器缩小块尺寸时不会按旧块号跳过字节。
   */
  async #runAssembly(firstMessage) {
    let message = firstMessage;
    if (!message.block2) {
      message = await this.#fetchBlock(0, this.#preferredSzx, message.etag, message.observe);
      if (this.#abortIfStale(message)) return;
    }
    const assembly = {
      observe: message.observe,
      etag: message.etag,
      maxAge: message.maxAge,
      sourceReceivedAt: message.receivedAt,
      receivedAtMs: Date.now(),
      szx: message.block2.szx,                 // 最近一次响应的实际尺寸
      size: blockSize(message.block2.szx),
      chunks: [],
      offset: 0,
      superseded: false
    };
    this.#assembly = assembly;

    let guard = 0;
    while (true) {
      try {
        this.#safeIngest(assembly, message);
      } catch (error) {
        if (!(error instanceof BlockGapError)) throw error;
        message = await this.#restartAssembly(assembly, error, ++guard);
        if (message === null) return;
        continue;
      }
      if (assembly.superseded) return;
      if (!message.block2.more) break;
      if (this.#stopped || this.#cancelled) return;
      // 回显响应 szx 与块号 +1；字节连续性由 #safeIngest 的偏移断言保证
      const nextNum = message.block2.num + 1;
      try {
        message = await this.#fetchBlock(nextNum, assembly.szx, assembly.etag, assembly.observe);
      } catch (error) {
        if (!(error instanceof BlockGapError)) throw error;
        message = await this.#restartAssembly(assembly, error, ++guard);
        if (message === null) return;
        continue;
      }
      if (this.#abortIfStale(message)) return;
      this.#adoptResponseSize(assembly, message);
    }
    if (assembly.superseded || this.#assembly !== assembly) return;
    this.#publish(assembly);
  }

  #adoptResponseSize(assembly, message) {
    if (message.block2.szx === assembly.szx) return;
    if (message.block2.szx > this.#preferredSzx) {
      throw new BlockGapError(`server grew block size to szx ${message.block2.szx} mid-transfer`);
    }
    assembly.szx = message.block2.szx;
    assembly.size = blockSize(assembly.szx);
  }

  /** 分块语义变化出现缺口：从偏移 0 重领整份表示，绝不按旧块号跳字节。 */
  async #restartAssembly(assembly, error, guard) {
    if (guard > 8 || this.#gapRetries > 32) throw error;
    this.#gapRetries += 1;
    const restart = await this.#fetchBlock(0, assembly.szx, assembly.etag, assembly.observe);
    if (this.#abortIfStale(restart)) return null;
    this.#adoptResponseSize(assembly, restart);
    assembly.chunks = [];assembly.offset = 0;
    return restart;
  }

  #abortIfStale(message) {
    if (message?.code === '4.12') {
      // 表示已在领取中途变更：旧 ETag 的剩余字节不再有效，等新观察通知重领
      if (this.#assembly) this.#assembly.superseded = true;
      this.#assembly = null;
      this.#wakePoll();
      return true;
    }
    return false;
  }

  async #fetchBlock(num, szx, etag, observe) {
    const body = {uri: this.#uri, token: this.#token, block2: {num, szx}};
    if (etag !== undefined && etag !== null) body.ifMatch = etag; // 只领取同一 ETag 的表示
    const reply = await this.#client('/request', {method: 'POST', body});
    if (reply?.code === '4.12') return reply;
    if (reply?.code && reply.code !== '2.05') throw new Error(`block request rejected: ${reply.code}`);
    if (reply.token !== this.#token) throw new Error('block response token does not match subscription');
    if (etag !== undefined && etag !== null && reply.etag !== etag) {
      throw new BlockGapError(`etag changed mid-transfer: expected ${etag}, got ${reply.etag}`);
    }
    if (observe !== undefined && reply.observe !== undefined && reply.observe !== observe) {
      throw new BlockGapError(`observe number changed mid-transfer: expected ${observe}, got ${reply.observe}`);
    }
    if (!reply.block2) throw new Error('block response missing Block2');
    return reply;
  }

  /** 把一块载荷放到它声明的字节偏移上；身份不符、缺口或重叠都拒绝拼入。 */
  #safeIngest(assembly, message) {
    const {num, szx, more} = message.block2;
    if (szx !== assembly.szx) throw new BlockGapError(`unexpected szx ${szx}, assembling with ${assembly.szx}`);
    const payload = Buffer.from(message.payload ?? '', 'base64');
    // 响应块号按响应实际尺寸解释为字节偏移（契约：块号按响应尺寸解释）
    const offset = num * blockSize(szx);
    if (offset !== assembly.offset) throw new BlockGapError(`gap at byte ${assembly.offset}, response offered ${offset}`);
    if (!more && payload.length > assembly.size) throw new BlockGapError('final block exceeds negotiated size');
    if (more && payload.length !== assembly.size) throw new BlockGapError('non-final block is short');
    if (assembly.chunks.some(chunk => chunk.offset === offset)) return; // 重传块幂等
    assembly.chunks.push({offset, payload});
    assembly.offset = offset + payload.length;
  }

  #publish(assembly) {
    const ordered = assembly.chunks.slice().sort((a, b) => a.offset - b.offset);
    const bytes = Buffer.concat(ordered.map(chunk => chunk.payload));
    let value;
    try {value = JSON.parse(bytes.toString('utf8'));}
    catch (error) {throw new BlockGapError(`assembled representation is not valid JSON: ${error.message}`);}
    const record = {
      id: this.#uri,
      kind: 'observation',
      value,
      observe: assembly.observe,
      etag: assembly.etag,
      maxAge: assembly.maxAge,
      bytes: bytes.length,
      sourceReceivedAt: assembly.sourceReceivedAt,
      receivedAtMs: assembly.receivedAtMs,
      expiresAt: assembly.maxAge === undefined || assembly.maxAge === null ? null : assembly.receivedAtMs + assembly.maxAge * 1000
    };
    this.#published = record;
    this.#assembly = null;
    try {this.#onRecord(record, this.status());} catch { /* 发布回调失败不影响采集状态机 */ }
    for (const waiter of this.#waiters) {clearTimeout(waiter.timer);waiter.resolve(record);}
    this.#waiters.clear();
  }
}

/** 为目录中的每个资源建立独立令牌的观察订阅。 */
export async function createObservers(client, options = {}) {
  const directory = await client('/resources');
  const observers = (directory.items ?? []).map(item => new ResourceObserver(client, item.uri, options));
  const initialStatuses = [];
  for (const observer of observers) initialStatuses.push(await observer.start());
  return {
    observers,
    initialStatuses,
    waitForRecord(timeoutMs) {
      return Promise.race(observers.map(observer => observer.waitForRecord(timeoutMs)));
    },
    statuses() {return observers.map(observer => observer.status());},
    async stop() {return Promise.all(observers.map(observer => observer.stop()));}
  };
}

export {SourceError};
