export function behavior(entry,state,fixture,scenario) {
if (entry.path === '/__advance' && entry.method === 'POST') {state.changed = true;return {body:{advanced:true}};}
const changed = state.changed || scenario === 'new-observation';
if (entry.path === '/notifications') {
  if (!state.token) return {status:409,body:{error:'subscription required'}};
  if (scenario === 'cancelled') return {body:{items:[{code:'4.04',token:state.token,serverEpoch:'s1',receivedAt:100}],next:1}};
  const payload = Buffer.from(JSON.stringify({room:'hall',temperature:changed ? 21 : 20,quality:'good'}));
  return {body:{items:scenario === 'expired' ? [] : [{token:state.token,serverEpoch:'s1',observe:changed ? 1 : 16777214,etag:changed ? 'e2' : 'e1',maxAge:30,receivedAt:100,block2:{num:0,szx:0,more:true},payload:payload.subarray(0,16).toString('base64')}],next:1}};
}
if (entry.path !== '/request' || entry.method !== 'POST') return null;
const request = entry.body;
if (request?.uri !== '/r/temp' || typeof request.token !== 'string' || !/^[0-9a-f]{2,16}$/.test(request.token)) return {status:400,body:{error:'resource and token required'}};
if (request.observe === 1) {delete state.token;return {body:{code:'2.05',cancelled:true,token:request.token}};}
if (request.observe === 0) state.token = request.token;
const etag = changed ? 'e2' : 'e1';
if (request.ifMatch && request.ifMatch !== etag) return {body:{code:'4.12',etag,token:request.token}};
const num = request.block2?.num ?? 0, requestedSzx = request.block2?.szx ?? 0;
if (!Number.isInteger(num) || num < 0 || !Number.isInteger(requestedSzx) || requestedSzx < 0 || requestedSzx > 6) return {status:400,body:{error:'invalid Block2'}};
const szx = scenario === 'block-resize' ? 0 : requestedSzx;
const size = 2 ** (szx+4), offset = num * 2 ** (requestedSzx+4);
const bytes = Buffer.from(JSON.stringify({room:'hall',temperature:changed ? 21 : 20,quality:'good'}));
return {body:{code:'2.05',serverEpoch:'s1',token:request.token,observe:changed ? 1 : 16777214,etag,maxAge:scenario === 'expired' ? 0 : 30,block2:{num:offset/size,szx,more:offset+size < bytes.length},payload:bytes.subarray(offset,offset+size).toString('base64')}};
}
