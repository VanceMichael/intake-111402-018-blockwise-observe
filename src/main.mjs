import {resolve} from 'node:path';
import {createSource} from './source.mjs';
import {createClient} from './transport.mjs';
import {ResourceObserver} from './observer.mjs';
import {Archive} from './archive.mjs';
import {createApi} from './api.mjs';

const source = await createSource({port:Number(process.env.SOURCE_PORT ?? 4010),scenario:process.env.SCENARIO ?? 'normal'});
const archive = new Archive(resolve(process.env.DATA_DIR ?? '.state'));
const api = await createApi(archive,Number(process.env.PORT ?? 4020));
console.log(JSON.stringify({source:source.origin,api:api.origin}));

const client = createClient(source.origin);
const directory = await client('/resources');
// 每个资源持有独立订阅令牌；只有完整收齐同一表示后才归档发布
const observers = [];
for (const item of directory.items ?? []) {
  const observer = new ResourceObserver(client, item.uri, {
    pollMs:Number(process.env.POLL_MS ?? 200),
    onRecord: async (record, status) => {
      const row = {id:record.id, kind:record.kind, value:record.value, observe:record.observe,
        etag:record.etag, maxAge:record.maxAge, bytes:record.bytes,
        sourceReceivedAt:record.sourceReceivedAt, expiresAt:record.expiresAt};
      const result = await archive.put([row], {source:source.origin, adapter:'blockwise-observe'});
      console.log(JSON.stringify({published:row.id, etag:row.etag, observe:row.observe, archived:result, phase:status.phase}));
    }
  });
  const status = await observer.start();
  console.log(JSON.stringify({subscribed:item.uri, token:observer.token, phase:status.phase, reason:status.reason}));
  observers.push(observer);
}

const dumpStatus = () => {
  for (const observer of observers) console.log(JSON.stringify(observer.status()));
};
const timer = setInterval(dumpStatus, Number(process.env.STATUS_MS ?? 30000));
timer.unref?.();

let stopping = false;
const stop = async () => {
  if (stopping) return;
  stopping = true;
  clearInterval(timer);
  // 停止领取时按各自令牌注销订阅，不影响其他资源
  const releases = await Promise.all(observers.map(observer => observer.stop()));
  console.log(JSON.stringify({releases}));
  await Promise.all([source.close(), api.close()]);
};
process.on('SIGINT', stop);process.on('SIGTERM', stop);
