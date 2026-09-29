const $ = id => document.getElementById(id);
let root = null, parentOffset = 0, childOffset = 0, eventOffset = 0;
const pageSize = 20;
const text = (tag, value, className) => { const el = document.createElement(tag); el.textContent = value ?? '—'; if (className) el.className = className; return el; };
async function get(path) {
  const response = await fetch('/api' + path, {cache:'no-store'});
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
  return body;
}
async function action(fn) { $('error').textContent = ''; try { await fn(); } catch (e) { $('error').textContent = e.message; } }
function paging(kind, offset, count) {
  $(`prev${kind}`).disabled = offset === 0;
  $(`next${kind}`).disabled = count < pageSize || offset + pageSize > 10000;
  $(kind === 'Parents' ? 'parentPage' : kind === 'Children' ? 'childPage' : 'eventPage').textContent = `Page ${offset / pageSize + 1}`;
}
async function parents() {
  const query = new URLSearchParams({limit:pageSize, offset:parentOffset});
  if ($('rootFilter').value.trim()) query.set('rootOrderId', $('rootFilter').value.trim());
  if ($('statusFilter').value) query.set('status', $('statusFilter').value);
  const {items} = await get('/parents?' + query);
  $('parents').replaceChildren();
  for (const p of items) {
    const row = document.createElement('tr'), cell = document.createElement('td');
    const link = text('button', p.root_order_id);
    link.onclick = () => action(async () => { root = p.root_order_id; childOffset = eventOffset = 0; $('childFilter').value = ''; await detail(); });
    cell.append(link); row.append(cell);
    for (const value of [p.status,p.quantity,p.filled_quantity,p.child_count,p.business_updated_at]) row.append(text('td',value));
    $('parents').append(row);
  }
  if (!items.length) { const row = document.createElement('tr'), cell = text('td','No orders found. Publish the synthetic demo events to begin.'); cell.colSpan = 6; row.append(cell); $('parents').append(row); }
  paging('Parents',parentOffset,items.length);
  $('queried').textContent = 'Queried ' + new Date().toLocaleTimeString();
}
async function children() {
  const {items} = await get(`/parents/${encodeURIComponent(root)}/children?limit=${pageSize}&offset=${childOffset}`);
  $('children').replaceChildren();
  for (const c of items) {
    const row = document.createElement('tr');
    const notes = [...c.warnings, ...(c.pending_cancel?['CANCEL PENDING']:[]), ...(c.pending_replace?['REPLACE PENDING']:[])].join(', ') || 'Complete evidence';
    for (const value of [c.order_id,c.status,c.quantity,c.filled_quantity,c.price,notes]) row.append(text('td',value));
    $('children').append(row);
  }
  paging('Children',childOffset,items.length);
}
async function events() {
  const query = new URLSearchParams({limit:pageSize,offset:eventOffset});
  if ($('childFilter').value.trim()) query.set('childOrderId',$('childFilter').value.trim());
  const {items} = await get(`/parents/${encodeURIComponent(root)}/events?${query}`);
  $('events').replaceChildren();
  for (const e of items) {
    const node = document.createElement('details'); node.className = 'event';
    node.append(text('summary',`${e.occurred_at} · ${e.source} / ${e.event_type} · ${e.order_id} · seq ${e.source_sequence}`),text('pre',JSON.stringify(e.payload,null,2)));
    $('events').append(node);
  }
  if (!items.length) $('events').append(text('p','No matching events.'));
  paging('Events',eventOffset,items.length);
}
async function detail() {
  const p = await get('/parents/' + encodeURIComponent(root));
  $('detail').hidden = false; $('selectedRoot').textContent = root;
  $('summary').replaceChildren();
  for (const [label,value] of [['Scepter status',p.status],['Order quantity',p.quantity],['Filled quantity',p.filled_quantity],['Children',p.child_count]]) {
    const card = text('div','','card'); card.append(text('small',label),text('strong',value)); $('summary').append(card);
  }
  $('warnings').textContent = `${p.warnings.length ? p.warnings.join(' · ') : 'Complete observed evidence'} · Last event: ${p.business_updated_at} · Persisted: ${p.persisted_at}`;
  await Promise.all([children(),events()]);
}
async function refresh() {
  try { const h = await get('/health'); $('health').textContent = `PostgreSQL ${h.database} · Kafka Streams ${h.pipeline.streams} · Writer ${h.pipeline.writer}`; }
  catch (e) { $('health').textContent = e.message; }
  await parents(); if(root) await detail();
}
$('filters').onsubmit = e => {e.preventDefault();parentOffset=0;action(parents);};
$('refresh').onclick = () => action(refresh);
$('applyChild').onclick = () => {eventOffset=0;action(events);};
for (const [kind,fn] of [['Parents',parents],['Children',children],['Events',events]]) for (const [direction,delta] of [['prev',-pageSize],['next',pageSize]]) {
  $(direction+kind).onclick = () => action(async () => {if(kind==='Parents')parentOffset+=delta;else if(kind==='Children')childOffset+=delta;else eventOffset+=delta;await fn();});
}
action(refresh);
