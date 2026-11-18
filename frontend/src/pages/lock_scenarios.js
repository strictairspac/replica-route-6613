import {table} from '../components/common/table.js';import {esc} from '../utils/html.js';import {session} from '../hooks/session.js';
const sample=[
 {at_ms:0,kind:'begin',tx:'tx-A'},{at_ms:0,kind:'begin',tx:'tx-B'},
 {at_ms:10,kind:'acquire',tx:'tx-A',resource:'orders:42'},
 {at_ms:30,kind:'acquire',tx:'tx-B',resource:'stock:42'},
 {at_ms:40,kind:'acquire',tx:'tx-A',resource:'stock:42'},
 {at_ms:50,kind:'acquire',tx:'tx-B',resource:'orders:42'},
 {at_ms:60,kind:'check'},{at_ms:70,kind:'decide'},
 {at_ms:80,kind:'retry_rollback',rollback_id:1},
 {at_ms:90,kind:'commit',tx:'tx-A'}];
export function lockScenariosPage(items){return `<div class="split"><section><div class="section-top"><h2>写事务锁场景</h2><span>${items.length} SCENARIOS / LOCK TIMELINE</span></div>${table(['场景 / 说明','事件数','操作'],items.map(s=>[`<strong>${esc(s.code)}</strong><small>${esc(s.title)}</small><small>${esc(s.note)}</small>`,`${s.events.length} 个`,`<button class="small edit-lock-scenario" data-id="${s.id}" ${session.user.role==='viewer'?'disabled':''}>编辑</button>`]))}<div class="protocol"><h3>事件字段速查</h3><p>begin / commit / rollback：at_ms、kind、tx。<br>acquire：另需 resource（锁行名）。<br>check / decide：无额外字段。<br>retry_rollback：另需 rollback_id（重放已裁决的回滚回执）。<br>replicate / node_down / node_up：另需 node，replicate 还要 position、delay_ms。<br>read：使用 session 字段，按提交后的日志位点选路。</p><p>acquire 记为该行写入，commit 才推进复制位点；未提交写入随 rollback 丢弃。事件按 at_ms 非递减，同一时刻按输入先后，最多120条。</p></div></section><section class="form-panel"><span class="eyebrow">LOCK CONTENTION SCENARIO</span><h2 id="lock-scenario-heading">编排锁等待时序</h2><form id="lock-scenario-form"><input type="hidden" name="id"><label>场景编号<input name="code" placeholder="QY-LOCK-A" required></label><label>场景名称<input name="title" value="订单与库存互相等锁" required></label><label>时序事件JSON<textarea name="events" rows="14" required>${esc(JSON.stringify(sample,null,2))}</textarea></label><label>备注<input name="note" value="交接班写事务等锁复核"></label><button class="primary" ${session.user.role==='viewer'?'disabled':''}>保存锁场景 +</button></form><p class="help">拿锁、检查、裁决、提交的先后由事件顺序决定；检查只记录等待环，裁决时还要复核环是否仍在。</p></section></div>`;}
