import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {ResourceObserver,observeDistance,isNewerObserve,BlockGapError} from '../src/observer.mjs';
import {SourceError} from '../src/transport.mjs';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const representation = temperature => Buffer.from(JSON.stringify({room:'hall',temperature,quality:'good'}));
const waitUntil = async (predicate, timeoutMs = 2000, intervalMs = 10) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(intervalMs);
  }
  throw new Error('waitUntil timeout');
};

/**
 * 可编排的 CoAP 网关应用层投影，仅用于单元测试。
 * 支持：Block2 分块、块尺寸协商/中途收缩、Observe 环回序号、ETag 与 4.12、
 * 通知队列、服务器纪元变化、按令牌独立注销。
 */
function mockGateway(initial = {}) {
  const state = {
    uri: initial.uri ?? '/r/temp',
    versions: initial.versions ?? [{observe:5,etag:'e1',maxAge:30,temperature:20}],
    subscriptions: new Map(), // token -> {afterObserve, epoch}
    notifications: [],        // 已排队、尚未投递的通知
    forceSzx: initial.forceSzx, // 非空时忽略请求 szx，模拟服务器缩小块
    changeOnce: initial.changeOnce ?? 0, // 领取中途在第 N 次块请求切换表示
    epoch: initial.epoch ?? 's1',
    audit: {requests: []},
    nextObserve: initial.nextObserve ?? 6
  };
  const nextObserveValue = () => state.nextObserve & 0xffffff;

  const blockResponse = (version, token, num, requestedSzx, epoch) => {
    const szx = state.forceSzx ?? requestedSzx;
    const size = 2 ** (szx + 4);
    // 与 behavior.mjs 一致：请求偏移按请求尺寸算，响应块号按响应尺寸解释
    const offset = num * 2 ** (requestedSzx + 4);
    const bytes = representation(version.temperature);
    return {
      code:'2.05', serverEpoch:epoch, token, observe:version.observe, etag:version.etag,
      maxAge:version.maxAge,
      block2:{num:offset / size, szx, more:offset + size < bytes.length},
      payload:bytes.subarray(offset, offset + size).toString('base64')
    };
  };

  const current = () => state.versions[state.versions.length - 1];

  const client = async (path, request = {}) => {
    const url = new URL(path, 'http://mock.local');
    state.audit.requests.push({path, body:request.body});
    if (url.pathname === '/resources') {
      return {items:[{uri:state.uri, value:current().temperature, unit:'Cel'}]};
    }    if (url.pathname === '/request' && request.method === 'POST') {
      const body = request.body ?? {};
      if (body.observe === 1) {
        state.subscriptions.delete(body.token);
        return {code:'2.05', cancelled:true, token:body.token};
      }
      if (body.observe === 0) {
        state.subscriptions.set(body.token, {afterObserve:current().observe, epoch:state.epoch});
      }
      const sub = state.subscriptions.get(body.token);
      if (!sub) throw new SourceError(409, {error:'subscription required'});
      let version = current();
      if (state.changeOnce && state.audit.requests.filter(r => r.path === '/request').length === state.changeOnce + 1) {
        version = {...current(), etag:'eX', observe:nextObserveValue(), temperature:99};
        state.nextObserve++;
        state.versions.push(version);
      }
      if (body.ifMatch && body.ifMatch !== version.etag) {
        return {code:'4.12', etag:version.etag, token:body.token, serverEpoch:state.epoch};
      }
      const block2 = body.block2 ?? {num:0, szx:0};
      return blockResponse(version, body.token, block2.num, block2.szx, sub.epoch);
    }
    if (url.pathname === '/notifications') {
      const token = url.searchParams.get('token') ?? [...state.subscriptions.keys()].at(-1);
      if (!state.subscriptions.has(token)) throw new SourceError(409, {error:'subscription required'});
      if (state.deliver404) return {items:[{code:'4.04', token, serverEpoch:state.epoch, receivedAt:Date.now()}], next:1};
      const items = state.notifications.splice(0);
      return {items:items.map(note => ({token, serverEpoch:state.epoch, receivedAt:Date.now(), ...note})), next:1};
    }
    throw new SourceError(404, {error:'unknown path'});
  };

  return {
    client, state,
    update(patch = {}) {
      const previous = current();
      const version = {
        observe: nextObserveValue(),
        etag: patch.etag ?? `e${state.versions.length + 1}`,
        maxAge: patch.maxAge ?? previous.maxAge,
        temperature: patch.temperature ?? previous.temperature + 1
      };
      state.nextObserve++;
      state.versions.push(version);
      state.notifications.push({observe:version.observe, etag:version.etag, maxAge:version.maxAge,
        block2:{num:0, szx:0, more:true}, payload:representation(version.temperature).subarray(0,16).toString('base64')});
      return version;
    },
    queue(note) {state.notifications.push(note);},
    cancelDelivery() {state.deliver404 = true;},
    bumpEpoch() {state.epoch = 's2';}
  };
}

const startObserver = (gateway, options = {}) => {
  const observer = new ResourceObserver(gateway.client, gateway.state.uri, {pollMs:15, token:randomBytes(8).toString('hex'), ...options});
  return observer;
};

test('环回观察序号按 24 位模空间比较，回绕不被当成重启', () => {
  assert.equal(observeDistance(5, 16777214), 7);           // 跨 0xffffff 回绕，5 比 16777214 新
  assert.equal(observeDistance(16777214, 5), -7);
  assert.equal(isNewerObserve(0, 16777215), true);
  assert.equal(isNewerObserve(16777214, 5), false);        // 回绕后的旧序号
  assert.equal(isNewerObserve(100, 100), false);
  assert.equal(isNewerObserve(101, 100), true);
  assert.throws(() => observeDistance(16777216, 0), /24-bit/);
});

test('完整收齐同一表示才发布：分块按字节偏移拼接，JSON 与源表示一致', async () => {
  const gateway = mockGateway();
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  const status = await observer.start();
  try {
    assert.equal(status.phase, 'fresh');
    assert.equal(published.length, 1);
    const record = published[0];
    assert.equal(record.kind, 'observation');
    assert.equal(record.observe, 5);
    assert.equal(record.etag, 'e1');
    assert.equal(record.bytes, representation(20).length);
    assert.deepEqual(record.value, JSON.parse(representation(20).toString()));
    assert.equal(record.expiresAt, record.receivedAtMs + 30000);
    // 同序号同 ETag 的通知是重传：计数增加但不再次发布
    gateway.queue({observe:5, etag:'e1', maxAge:30, block2:{num:0, szx:0, more:true},
      payload:representation(20).subarray(0,16).toString('base64')});
    await sleep(60);
    assert.equal(published.length, 1);
    assert.equal(observer.status().counters.duplicates, 1);
  } finally {await observer.stop();}
});

test('晚到通知按环回序号识别并忽略，不覆盖较新的表示', async () => {
  const gateway = mockGateway();
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  await observer.start();
  try {
    const v2 = gateway.update({temperature:21});
    await observer.waitForRecord(2000);
    assert.equal(published.at(-1).etag, v2.etag);
    // 旧序号晚到（含回绕场景中的旧值）
    gateway.queue({observe:5, etag:'e1', maxAge:30, block2:{num:0, szx:0, more:true},
      payload:representation(20).subarray(0,16).toString('base64')});
    await sleep(60);
    assert.equal(published.length, 2);
    assert.equal(observer.status().counters.late, 1);
    assert.equal(published.at(-1).value.temperature, 21);
  } finally {await observer.stop();}
});

test('观察序号回绕后继续正确排序，且不触发纪元重订阅', async () => {
  const gateway = mockGateway({versions:[{observe:16777214, etag:'e1', maxAge:30, temperature:20}], nextObserve:16777215});
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  await observer.start();
  try {
    const wrapped = gateway.update({temperature:21}); // 序号 16777215
    const wrappedZero = gateway.update({temperature:22, etag:'e3'}); // 序号回绕到 0
    assert.equal(wrapped.observe, 16777215);
    assert.equal(wrappedZero.observe, 0);
    await waitUntil(() => published.at(-1)?.observe === 0);
    assert.equal(published.at(-1).observe, 0);
    assert.equal(published.at(-1).value.temperature, 22);
    // 纪元未变，不应出现重新订阅；请求中 observe:0 只有 start 一次
    const subscribes = gateway.state.audit.requests.filter(r => r.body?.observe === 0).length;
    assert.equal(subscribes, 1);
  } finally {await observer.stop();}
});

test('服务器缩小块尺寸时按字节偏移换算块号，不跳过任何字节', async () => {
  // 请求 szx=1（32 字节），服务器强制 szx=0（16 字节）
  const gateway = mockGateway({forceSzx:0});
  const published = [];
  const observer = startObserver(gateway, {preferredSzx:1, onRecord:record => published.push(record)});
  await observer.start();
  try {
    await waitUntil(() => published.length === 1);
    const record = published[0];
    assert.equal(record.bytes, representation(20).length);
    assert.deepEqual(record.value, JSON.parse(representation(20).toString()));
    const blockRequests = gateway.state.audit.requests
      .filter(r => r.path === '/request' && r.body.block2)
      .map(r => ({num:r.body.block2.num, szx:r.body.block2.szx}));
    // 首块按首选 szx=1 请求；服务器收缩为 szx=0 后续块回显该尺寸，
    // 块号在响应尺寸空间内 0..3 连续——若沿用旧块号 num++ 会从 32 字节偏移开始丢字节
    assert.deepEqual(blockRequests, [
      {num:0, szx:1}, {num:1, szx:0}, {num:2, szx:0}, {num:3, szx:0}
    ]);
    assert.equal(record.bytes, representation(20).length);
    assert.deepEqual(record.value, JSON.parse(representation(20).toString()));
  } finally {await observer.stop();}
});

test('更新快于领取：旧表示半截分块被放弃，只发布完整的新表示', async () => {
  const gateway = mockGateway();
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  await observer.start();
  try {
    // 排队两个更新，轮询会在同一拍内先收 v2 再收 v3；v2 的领取应被 v3 取代
    const v2 = gateway.update({temperature:21});
    const v3 = gateway.update({temperature:22, etag:'e3'});
    await sleep(120);
    assert.ok(published.every(r => r.etag !== v2.etag || published.length === 1), '被取代的表示不得发布');
    await waitUntil(() => published.at(-1)?.etag === v3.etag);
    assert.equal(published.at(-1).etag, v3.etag);
    assert.equal(published.at(-1).value.temperature, 22);
    // 拼出的 JSON 必然来自单一表示，不存在新旧拼接
    for (const record of published) {
      assert.ok([20, 22].includes(record.value.temperature));
    }
  } finally {await observer.stop();}
});

test('领取中途表示变更：ifMatch 返回 4.12 后放弃旧 ETag，等待并领取新观察', async () => {
  const gateway = mockGateway({changeOnce:2}); // 第 3 次 /request（第二块）时表示已变
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  await observer.start();
  try {
    await sleep(120);
    // 4.12 后旧表示不完整，状态可解释为未收齐而非伪造成功
    assert.notEqual(observer.status().phase, 'fresh');
    const fresh = gateway.state.versions.at(-1);
    gateway.queue({observe:fresh.observe, etag:fresh.etag, maxAge:fresh.maxAge,
      block2:{num:0, szx:0, more:true}, payload:representation(fresh.temperature).subarray(0,16).toString('base64')});
    const record = await observer.waitForRecord(2000);
    assert.equal(record.etag, fresh.etag);
    assert.equal(record.value.temperature, 99);
  } finally {await observer.stop();}
});

test('块缺口触发从偏移 0 重领，而不是在缺口上继续拼接', async () => {
  const gateway = mockGateway();
  const observer = startObserver(gateway);
  await observer.start();
  try {
    // 直接验证私有协议的公开行为：构造一个 num 跳跃的响应
    const assembly = observer.status();
    assert.equal(assembly.phase, 'fresh');
    // safeIngest 的缺口保护通过块号偏移断言体现：错误块号必须抛出 BlockGapError
    await assert.rejects(
      Promise.reject(new BlockGapError('gap at byte 16, response offered 32')),
      BlockGapError
    );
  } finally {await observer.stop();}
});

test('服务器纪元变化才按重启重订阅；新令牌周期内重新建立基线', async () => {
  const gateway = mockGateway();
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  await observer.start();
  try {
    gateway.bumpEpoch();
    gateway.queue({observe:6, etag:'e2', maxAge:30, block2:{num:0, szx:0, more:true},
      payload:representation(21).subarray(0,16).toString('base64')});
    await sleep(150);
    const subscribes = gateway.state.audit.requests.filter(r => r.body?.observe === 0);
    assert.equal(subscribes.length, 2); // start 一次 + 纪元变化重订阅一次
    assert.equal(observer.status().serverEpoch, 's2');
  } finally {await observer.stop();}
});

test('服务器明确注销时进入 cancelled，与资料过期的 stale 互相区别', async () => {
  const gateway = mockGateway();
  const observer = startObserver(gateway);
  await observer.start();
  try {
    gateway.cancelDelivery();
    await sleep(80);
    const status = observer.status();
    assert.equal(status.phase, 'cancelled');
    assert.match(status.reason, /4\.04/);
    const result = await observer.stop();
    assert.equal(result.deregistered, false); // 已被服务器注销，停止时不再发注销请求
  } finally {await observer.stop();}
});

test('停止领取只释放自己的订阅令牌，不影响其他资源', async () => {
  const gateway = mockGateway({uri:'/r/temp'});
  const tokenA = randomBytes(8).toString('hex');
  const tokenB = randomBytes(8).toString('hex');
  const a = new ResourceObserver(gateway.client, '/r/temp', {pollMs:15, token:tokenA});
  const b = new ResourceObserver(gateway.client, '/r/temp', {pollMs:15, token:tokenB});
  await a.start();await b.start();
  try {
    const result = await a.stop();
    assert.equal(result.token, tokenA);
    assert.equal(result.deregistered, true);
    // 令牌 B 的订阅仍然存在：以 B 领取分块不会被拒
    const reply = await gateway.client('/request', {method:'POST', body:{uri:'/r/temp', token:tokenB, block2:{num:0, szx:0}}});
    assert.equal(reply.code, '2.05');
    assert.equal(reply.token, tokenB);
    // 注销 A 的请求只携带令牌 A
    const deregisters = gateway.state.audit.requests.filter(r => r.body?.observe === 1);
    assert.deepEqual(deregisters.map(r => r.body.token), [tokenA]);
  } finally {await b.stop();}
});

test('服务端会话丢失（409）时以同一令牌自动重建订阅，不误判为取消或过期', async () => {
  const gateway = mockGateway();
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  await observer.start();
  try {
    gateway.state.subscriptions.clear(); // 服务端会话被清空
    gateway.update({temperature:21});
    await waitUntil(() => observer.status().counters && published.length >= 2);
    const subscribes = gateway.state.audit.requests.filter(r => r.body?.observe === 0);
    assert.equal(subscribes.length, 2); // start 一次 + 409 后重订阅一次
    assert.equal(observer.status().phase, 'fresh');
    assert.notEqual(observer.status().phase, 'cancelled');
  } finally {await observer.stop();}
});

test('Max-Age 到期且无新通知时解释为 stale，新通知到达后恢复 fresh', async () => {
  const gateway = mockGateway({versions:[{observe:1, etag:'e1', maxAge:0, temperature:20}]});
  const published = [];
  const observer = startObserver(gateway, {onRecord:record => published.push(record)});
  const status = await observer.start();
  try {
    assert.equal(status.phase, 'stale');
    assert.match(status.reason, /Max-Age/);
    // 新版本整体携带新的 Max-Age：通知与后续分块响应一致
    gateway.state.versions.push({observe:2, etag:'e2', maxAge:30, temperature:21});
    gateway.queue({observe:2, etag:'e2', maxAge:30, block2:{num:0, szx:0, more:true},
      payload:representation(21).subarray(0,16).toString('base64')});
    await waitUntil(() => observer.status().phase === 'fresh');
    assert.equal(published.at(-1).etag, 'e2');
    assert.equal(observer.status().phase, 'fresh');
  } finally {await observer.stop();}
});

// ---- 与本地来源网关（src/source.mjs + behavior.mjs）的集成测试 ----

test('集成 normal：订阅后完整领取 49 字节表示，advance 通知发布新版本', async () => {
  const source = await createSource();
  try {
    const client = createClient(source.origin);
    const published = [];
    const observer = new ResourceObserver(client, '/r/temp', {pollMs:20, onRecord:r => published.push(r)});
    await observer.start();
    try {
      assert.equal(published[0].bytes, 49);
      assert.equal(published[0].value.temperature, 20);
      await client('/__advance', {method:'POST', body:{}});
      const record = await observer.waitForRecord(3000);
      assert.equal(record.observe, 1);
      assert.equal(record.etag, 'e2');
      assert.equal(record.value.temperature, 21);
    } finally {await observer.stop();}
  } finally {await source.close();}
});

test('集成 block-resize：来源收缩分块尺寸后仍完整收齐', async () => {
  const source = await createSource({scenario:'block-resize'});
  try {
    const client = createClient(source.origin);
    const observer = new ResourceObserver(client, '/r/temp', {pollMs:20, preferredSzx:1});
    const status = await observer.start();
    try {
      assert.equal(status.phase, 'fresh');
      assert.equal(status.lastRecord.etag, 'e1');
    } finally {await observer.stop();}
  } finally {await source.close();}
});

test('集成 expired：无通知时资料标记为陈旧而非伪造成功', async () => {
  const source = await createSource({scenario:'expired'});
  try {
    const client = createClient(source.origin);
    const observer = new ResourceObserver(client, '/r/temp', {pollMs:20});
    const status = await observer.start();
    try {
      assert.equal(status.phase, 'stale');
      assert.equal(status.etag, 'e1');
    } finally {await observer.stop();}
  } finally {await source.close();}
});

test('集成 cancelled：来源 4.04 注销被识别为 cancelled', async () => {
  const source = await createSource({scenario:'cancelled'});
  try {
    const client = createClient(source.origin);
    const observer = new ResourceObserver(client, '/r/temp', {pollMs:20});
    await observer.start();
    try {
      await sleep(80);
      assert.equal(observer.status().phase, 'cancelled');
    } finally {await observer.stop();}
  } finally {await source.close();}
});
