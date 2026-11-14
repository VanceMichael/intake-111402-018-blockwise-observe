import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';
import {collect} from '../src/reader.mjs';
let source,client;
before(async () => {source = await createSource();client = createClient(source.origin);});
after(async () => {await source.close();});

test('目录摘要没有宣称存在观察订阅', async () => {
  const row = (await collect(client))[0];
  assert.equal(row.id,'/r/temp');
  assert.equal(row.kind,'directory');
  await assert.rejects(client('/notifications?after=0'),error => error.status === 409);
});
