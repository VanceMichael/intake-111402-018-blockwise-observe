export async function collect(client) {
  const resources = await client('/resources');
  return resources.items.map(resource => ({id:resource.uri,value:resource.value,unit:resource.unit,kind:'directory'}));
}
