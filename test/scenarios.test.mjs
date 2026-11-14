import test from 'node:test';
import assert from 'node:assert/strict';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';
test('来源变更与异常边界具有可观察的区别',async () => {
  const source = await createSource();
  try {
    const client = createClient(source.origin);
    const choose = name => client('/__scenario',{method:'POST',body:{name}});
await choose('block-resize');
const block = await client('/request',{method:'POST',body:{uri:'/r/temp',token:'ab',observe:0,block2:{num:1,szx:1}}});
assert.equal(block.block2.szx,0);
assert.equal(block.block2.num,2);
assert.equal(Buffer.from(block.payload,'base64').length,16);
await choose('new-observation');
const changed = await client('/request',{method:'POST',body:{uri:'/r/temp',token:'ab',ifMatch:'e1'}});
assert.equal(changed.code,'4.12');
await choose('normal');
await client('/request',{method:'POST',body:{uri:'/r/temp',token:'ab',observe:0}});
await client('/__advance',{method:'POST',body:{}});
const notification = (await client('/notifications?after=0')).items[0];
assert.equal(notification.token,'ab');
assert.equal(notification.observe,1);
assert.equal(notification.etag,'e2');
} finally {await source.close();}
});
