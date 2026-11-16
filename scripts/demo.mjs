import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';
import {ResourceObserver} from '../src/observer.mjs';
import {Archive} from '../src/archive.mjs';
import {createApi} from '../src/api.mjs';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const directory = await mkdtemp(join(tmpdir(),'intake-'));
const source = await createSource({scenario:process.env.SCENARIO ?? 'normal'});
const archive = new Archive(directory);
const api = await createApi(archive);
try {
  console.log('本地来源',source.origin);
  const client = createClient(source.origin);
  const published = [];
  const observer = new ResourceObserver(client, '/r/temp', {
    pollMs:50,
    onRecord: async (record, status) => {
      const row = {id:record.id, kind:record.kind, value:record.value, observe:record.observe,
        etag:record.etag, maxAge:record.maxAge, bytes:record.bytes, expiresAt:record.expiresAt};
      await archive.put([row], {source:source.origin, adapter:'blockwise-observe'});
      published.push(row);
      console.log('发布完整表示', JSON.stringify({etag:row.etag, observe:row.observe, phase:status.phase}));
    }
  });

  const initial = await observer.start();
  console.log('订阅后状态', JSON.stringify({phase:initial.phase, reason:initial.reason, token:initial.token}));

  if (initial.phase !== 'cancelled') {
    // 推进来源一次：观察通知应带来新版本并再次发布，旧表示不会被半截拼接
    await client('/__advance', {method:'POST', body:{}});
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const status = observer.status();
      if (status.phase === 'cancelled' || published.some(row => row.etag === 'e2')) break;
      await sleep(50);
    }
    await sleep(100);
  }

  console.log('采集端解释', JSON.stringify(observer.status(), null, 2));
  const response = await fetch(`${api.origin}/records`);
  console.log('查询归档结果', await response.text());
  const release = await observer.stop();
  console.log('停止领取并释放订阅', JSON.stringify(release));
  const audit = await client('/__audit');
  console.log('来源侧订阅已注销', JSON.stringify({token:audit.state.token ?? null}));
} finally {
  await Promise.all([source.close(), api.close()]);
  await rm(directory, {recursive:true, force:true});
}
