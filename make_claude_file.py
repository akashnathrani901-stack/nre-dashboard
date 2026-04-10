"""
Generates nre_dashboard_server.py — a single self-contained Python file that:
  1. Embeds all Snowflake data as JSON
  2. Starts an HTTP server on port 8502
  3. Opens the browser automatically to the full interactive dashboard

Usage:
  python make_claude_file.py          # builds nre_dashboard_server.py
  python nre_dashboard_server.py      # run it (or share with anyone)
"""
import base64
import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
OUT_FILE  = Path(__file__).parent / "nre_dashboard_server.py"

# ── Load parquet files ────────────────────────────────────────────────────────
def load(name):
    df = pd.read_parquet(DATA_DIR / f"{name}_cache.parquet")
    df.columns = df.columns.str.lower()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str).replace("NaT", "")
    return df

print("Loading parquet files…")
df_opps = load("opps")
df_inv  = load("invoices")
df_li   = load("lineitems")
generated_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

opps_json = df_opps.to_json(orient="records")
inv_json  = df_inv.to_json(orient="records")
li_json   = df_li.to_json(orient="records")

print(f"  Opportunities  : {len(df_opps):,} rows")
print(f"  Invoice Headers: {len(df_inv):,} rows")
print(f"  Line Items     : {len(df_li):,} rows")

# ── Full HTML dashboard (%%PLACEHOLDERS%% replaced below) ─────────────────────
# Note: JS uses { } normally — no Python f-string conflicts since we use replace()
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NRE Sales & Invoice Dashboard</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body      { font-family: "Segoe UI", sans-serif; background:#f0f2f6; margin:0; }
  .sidebar  { width:270px; min-width:270px; background:#fff; border-right:1px solid #dee2e6;
               padding:14px; height:100vh; overflow-y:auto; position:fixed; top:0; left:0; z-index:10; }
  .main     { margin-left:270px; padding:18px; }
  .kpi-card { background:#fff; border-radius:8px; padding:14px 10px; box-shadow:0 1px 4px rgba(0,0,0,.08); text-align:center; }
  .kpi-val  { font-size:1.3rem; font-weight:700; color:#1a1a2e; }
  .kpi-lbl  { font-size:0.72rem; color:#666; margin-top:2px; }
  .card     { background:#fff; border-radius:8px; padding:14px; box-shadow:0 1px 4px rgba(0,0,0,.08); margin-bottom:14px; }
  .sec-ttl  { font-size:.95rem; font-weight:600; color:#333; margin-bottom:8px; }
  .sort-row { display:flex; gap:6px; margin-bottom:6px; }
  .sort-row select { font-size:.78rem; padding:2px 6px; border:1px solid #ccc; border-radius:4px; }
  .flbl     { font-size:.78rem; font-weight:600; color:#444; margin-bottom:3px; margin-top:8px; }
  select[multiple] { width:100%; font-size:.78rem; border:1px solid #ccc; border-radius:4px; }
  .info-box { font-size:.72rem; color:#666; background:#f8f9fa; border-radius:6px; padding:7px; margin-top:6px; }
  .cond-row { display:flex; gap:8px; justify-content:center; margin:4px 0; flex-wrap:wrap; }
  .cond-row label { font-size:.76rem; cursor:pointer; }
  .cond-desc { font-size:.7rem; color:#888; text-align:center; margin-top:2px; }
  table { font-size:.78rem; }
  .divider { border-top:1px solid #eee; margin:10px 0; }
  #countdown { font-size:.7rem; color:#4C78A8; font-weight:600; }
</style>
</head>
<body>

<!-- SIDEBAR -->
<div class="sidebar">
  <div style="font-size:1rem;font-weight:700;color:#4C78A8;">NRE Dashboard</div>
  <div style="font-size:.7rem;color:#888;margin-bottom:6px;">Data as of %%GENERATED_AT%%</div>
  <div class="info-box">
    ⟳ Auto-refresh every 15 min<br>
    <span id="countdown"></span>
  </div>

  <div class="divider"></div>

  <div class="flbl">Sales Group</div>
  <select multiple id="f-group" style="height:74px;" onchange="renderAll()"></select>

  <div class="flbl">Account</div>
  <select multiple id="f-account" style="height:84px;" onchange="renderAll()"></select>

  <div class="divider"></div>
  <div style="font-size:.82rem;font-weight:600;color:#333;margin-bottom:4px;">Invoice Year Filters</div>

  <div class="flbl">Invoice Billed Year</div>
  <select multiple id="f-billed" style="height:66px;" onchange="renderAll()"></select>

  <div class="flbl" style="text-align:center;color:#999;font-weight:400;">── Year Condition ──</div>
  <div class="cond-row">
    <label><input type="radio" name="ycond" value="AND" checked onchange="onCondChange()"> AND</label>
    <label><input type="radio" name="ycond" value="OR"  onchange="onCondChange()"> OR</label>
    <label><input type="radio" name="ycond" value="SAME" onchange="onCondChange()"> Same Year</label>
  </div>
  <div class="cond-desc" id="cond-desc"></div>

  <div id="paid-group">
    <div class="flbl">Invoice Paid / Closed Year</div>
    <select multiple id="f-paid" style="height:66px;" onchange="renderAll()"></select>
  </div>

  <div class="divider"></div>
  <div class="flbl">Invoice Status</div>
  <select multiple id="f-status" style="height:74px;" onchange="renderAll()"></select>
  <div style="font-size:.68rem;color:#999;margin-top:4px;">Tip: Billed 2025 + Open → unpaid 2025 invoices.</div>
</div>

<!-- MAIN -->
<div class="main">
  <h5 style="color:#1a1a2e;margin-bottom:2px;">NRE Sales &amp; Invoice Dashboard</h5>
  <div style="font-size:.78rem;color:#888;margin-bottom:14px;">Closed Won · NRE &gt; 0 · Since Jan 2025</div>

  <!-- KPIs -->
  <div class="row g-2 mb-3">
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">Deals</div><div class="kpi-val" id="k-deals">—</div></div></div>
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">Invoices</div><div class="kpi-val" id="k-inv">—</div></div></div>
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">NRE Contract Value</div><div class="kpi-val" id="k-nre">—</div></div></div>
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">Total Invoiced</div><div class="kpi-val" id="k-invoiced">—</div></div></div>
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">Total Paid</div><div class="kpi-val" id="k-paid">—</div></div></div>
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">Outstanding</div><div class="kpi-val" id="k-out">—</div></div></div>
    <div class="col"><div class="kpi-card"><div class="kpi-lbl">Collection Rate</div><div class="kpi-val" id="k-rate">—</div></div></div>
  </div>

  <!-- Row 1: Account bar + Status donut -->
  <div class="row g-3">
    <div class="col-8">
      <div class="card">
        <div class="sec-ttl">Invoiced vs Paid vs Remaining by Account</div>
        <div class="sort-row">
          <select id="s-acct-f" onchange="renderAll()"><option>Invoiced</option><option>Paid</option><option>Remaining</option><option>Count</option></select>
          <select id="s-acct-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
        </div>
        <div id="c-account"></div>
      </div>
    </div>
    <div class="col-4">
      <div class="card">
        <div class="sec-ttl">Invoice Status (by Amount)</div>
        <div id="c-status"></div>
      </div>
    </div>
  </div>

  <!-- Row 2: NRE by Group + Monthly Trend -->
  <div class="row g-3">
    <div class="col-6">
      <div class="card">
        <div class="sec-ttl">NRE Contract Value by Sales Group</div>
        <div class="sort-row">
          <select id="s-grp-f" onchange="renderAll()"><option>NRE</option><option>Count</option></select>
          <select id="s-grp-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
        </div>
        <div id="c-group"></div>
      </div>
    </div>
    <div class="col-6">
      <div class="card">
        <div class="sec-ttl">Monthly Invoice Trend</div>
        <div id="c-trend"></div>
      </div>
    </div>
  </div>

  <!-- Row 3: Heatmap -->
  <div class="card">
    <div class="sec-ttl">Billed Year × Status — Heatmap</div>
    <div style="font-size:.74rem;color:#888;margin-bottom:6px;">Spot open invoices from prior years at a glance.</div>
    <div style="display:flex;gap:12px;margin-bottom:6px;">
      <label style="font-size:.78rem;"><input type="radio" name="hm" value="Amount" checked onchange="renderAll()"> Amount</label>
      <label style="font-size:.78rem;"><input type="radio" name="hm" value="Count" onchange="renderAll()"> Count</label>
    </div>
    <div id="c-heatmap"></div>
  </div>

  <!-- Row 4: Line items -->
  <div class="card">
    <div class="sec-ttl">Top 20 Line Item Descriptions</div>
    <div class="sort-row">
      <select id="s-li-f" onchange="renderAll()"><option>Amount</option><option>Paid</option><option>Remaining</option><option>Count</option></select>
      <select id="s-li-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
    </div>
    <div id="c-lineitems"></div>
  </div>

  <!-- Row 5: Aging -->
  <div class="card">
    <div class="sec-ttl">Invoice Aging (Outstanding Only)</div>
    <div class="sort-row">
      <select id="s-ag-f" onchange="renderAll()"><option>Outstanding</option><option>Count</option></select>
      <select id="s-ag-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
    </div>
    <div id="c-aging"></div>
  </div>

  <!-- Tables -->
  <div class="card">
    <ul class="nav nav-tabs mb-2" id="tabs">
      <li class="nav-item"><a class="nav-link active" data-bs-toggle="tab" href="#t-opps">Opportunities</a></li>
      <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t-inv">Invoice Headers</a></li>
      <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t-li">Line Items</a></li>
    </ul>
    <div class="tab-content">
      <div class="tab-pane fade show active" id="t-opps"><div id="tbl-opps"></div></div>
      <div class="tab-pane fade" id="t-inv"><div id="tbl-inv"></div></div>
      <div class="tab-pane fade" id="t-li"><div id="tbl-li"></div></div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
<script>
// ── Embedded data ─────────────────────────────────────────────────────────────
const OPPS = %%OPPS_JSON%%;
const INV  = %%INV_JSON%%;
const LI   = %%LI_JSON%%;

// Parse numbers
const NUM_FIELDS = ['amount','amount_paid','amount_remaining','adjusted_unpaid_amount'];
INV.forEach(r => NUM_FIELDS.forEach(f => r[f] = parseFloat(r[f]) || 0));
LI.forEach(r  => NUM_FIELDS.forEach(f => r[f] = parseFloat(r[f]) || 0));
OPPS.forEach(r => r.nre_contract_value = parseFloat(r.nre_contract_value) || 0);

// ── Helpers ───────────────────────────────────────────────────────────────────
const $m = n => '$' + (n||0).toLocaleString('en-US', {maximumFractionDigits:0});
const today = new Date(); today.setHours(0,0,0,0);

function yr(s) { if (!s) return null; const d = new Date(s); return isNaN(d) ? null : d.getFullYear(); }
function daysAgo(s) { if (!s) return null; const d = new Date(s); return isNaN(d) ? null : Math.floor((today - d)/86400000); }

function vals(id) { return [...document.getElementById(id).selectedOptions].map(o => o.value); }
function numVals(id) { return vals(id).map(Number); }

function populate(id, items, selectAll=true) {
  const el = document.getElementById(id); el.innerHTML = '';
  items.forEach(v => { const o = document.createElement('option'); o.value=v; o.text=v; o.selected=selectAll; el.appendChild(o); });
}

function groupBy(arr, key, aggs) {
  const m = {};
  arr.forEach(r => {
    const k = r[key] ?? 'Unknown';
    if (!m[k]) { m[k] = {[key]:k}; Object.keys(aggs).forEach(a => m[k][a]=0); }
    Object.entries(aggs).forEach(([a,fn]) => m[k][a] = fn(m[k][a], r));
  });
  return Object.values(m);
}

function srt(arr, f, asc) { return [...arr].sort((a,b) => asc ? a[f]-b[f] : b[f]-a[f]); }

// ── Init sidebar options ──────────────────────────────────────────────────────
populate('f-group',   [...new Set(OPPS.map(o=>o.sales_group).filter(Boolean))].sort());
populate('f-account', [...new Set(OPPS.map(o=>o.account_name).filter(Boolean))].sort());
populate('f-billed',  [...new Set(INV.map(r=>yr(r.invoice_date)).filter(Boolean))].sort((a,b)=>b-a));
populate('f-paid',    [...new Set(INV.map(r=>yr(r.invoice_closed_date)).filter(Boolean))].sort((a,b)=>b-a), false);
populate('f-status',  [...new Set(INV.map(r=>r.status).filter(Boolean))].sort());

// ── Year condition ────────────────────────────────────────────────────────────
const COND_DESC = {
  AND:  'Both year filters must match',
  OR:   'Either year filter can match',
  SAME: 'Billed year = Paid year (Paid Year picker ignored)'
};
function onCondChange() {
  const c = document.querySelector('input[name="ycond"]:checked').value;
  document.getElementById('cond-desc').textContent = COND_DESC[c];
  const pg = document.getElementById('paid-group');
  const fp = document.getElementById('f-paid');
  pg.style.opacity = c === 'SAME' ? '0.35' : '1';
  fp.disabled = c === 'SAME';
  renderAll();
}
document.querySelectorAll('input[name="ycond"]').forEach(r => r.addEventListener('change', onCondChange));
onCondChange();

// ── Apply all filters ─────────────────────────────────────────────────────────
function applyFilters() {
  const sg  = vals('f-group');
  const acc = vals('f-account');
  const by  = numVals('f-billed');
  const py  = numVals('f-paid');
  const st  = vals('f-status');
  const yc  = document.querySelector('input[name="ycond"]:checked').value;

  // 1. Opps by sales group + account
  let fOpps = OPPS.filter(o => sg.includes(o.sales_group) && acc.includes(o.account_name));
  const sfdc = new Set(fOpps.map(o => o.sfdc_18_id));

  // 2. Invoices by sfdc
  let fInv = INV.filter(i => sfdc.has(i.salesforce_id));

  // 3. Year condition
  if (yc === 'AND') {
    const bm = by.length ? i => by.includes(yr(i.invoice_date))         : () => true;
    const pm = py.length ? i => py.includes(yr(i.invoice_closed_date))  : () => true;
    fInv = fInv.filter(i => bm(i) && pm(i));
  } else if (yc === 'OR') {
    if (by.length || py.length) {
      fInv = fInv.filter(i =>
        (by.length && by.includes(yr(i.invoice_date))) ||
        (py.length && py.includes(yr(i.invoice_closed_date)))
      );
    }
  } else { // SAME
    const bm = by.length ? i => by.includes(yr(i.invoice_date)) : () => true;
    fInv = fInv.filter(i => bm(i) && yr(i.invoice_date) === yr(i.invoice_closed_date));
  }

  // 4. Status
  if (st.length) fInv = fInv.filter(i => st.includes(i.status));

  // 5. Re-filter opps by remaining sfdc_ids → fixes NRE Contract Value reactivity
  const fSfdc = new Set(fInv.map(i => i.salesforce_id));
  fOpps = fOpps.filter(o => fSfdc.has(o.sfdc_18_id));

  // 6. Line items (billed year only)
  const fLi = LI.filter(i => sfdc.has(i.salesforce_id) && (by.length ? by.includes(yr(i.invoice_date)) : true));

  return { fOpps, fInv, fLi };
}

// ── KPIs ──────────────────────────────────────────────────────────────────────
function updateKPIs(opps, inv) {
  const nre  = opps.reduce((s,r) => s + r.nre_contract_value, 0);
  const inv$ = inv.reduce((s,r)  => s + r.amount, 0);
  const paid = inv.reduce((s,r)  => s + r.amount_paid, 0);
  const out  = inv.reduce((s,r)  => s + r.adjusted_unpaid_amount, 0);
  const rate = inv$ ? (paid/inv$*100) : 0;
  document.getElementById('k-deals').textContent   = opps.length;
  document.getElementById('k-inv').textContent     = new Set(inv.map(r=>r.invoice_number)).size;
  document.getElementById('k-nre').textContent     = $m(nre);
  document.getElementById('k-invoiced').textContent= $m(inv$);
  document.getElementById('k-paid').textContent    = $m(paid);
  document.getElementById('k-out').textContent     = $m(out);
  document.getElementById('k-rate').textContent    = rate.toFixed(1) + '%';
}

// ── Charts ────────────────────────────────────────────────────────────────────
const PLY = { responsive:true };

function cAccount(opps, inv) {
  const aMap = {}; opps.forEach(o => aMap[o.sfdc_18_id] = o.account_name);
  const rows = groupBy(inv.map(i => ({...i, account_name: aMap[i.salesforce_id]||'Unknown'})),
    'account_name', { Invoiced:(s,r)=>s+r.amount, Paid:(s,r)=>s+r.amount_paid, Remaining:(s,r)=>s+r.adjusted_unpaid_amount, Count:(s,r)=>s+1 });
  const f = document.getElementById('s-acct-f').value;
  const d = srt(rows, f, document.getElementById('s-acct-o').value==='Ascending').slice(0,15);
  Plotly.react('c-account', [
    {name:'Invoiced',  x:d.map(r=>r.account_name), y:d.map(r=>r.Invoiced),  type:'bar', marker:{color:'#4C78A8'}},
    {name:'Paid',      x:d.map(r=>r.account_name), y:d.map(r=>r.Paid),      type:'bar', marker:{color:'#54A24B'}},
    {name:'Remaining', x:d.map(r=>r.account_name), y:d.map(r=>r.Remaining), type:'bar', marker:{color:'#E45756'}},
  ], {barmode:'group',height:360,xaxis:{tickangle:-40},yaxis:{title:'Amount ($)'},legend:{orientation:'h',y:1.1},margin:{t:10,b:10}}, PLY);
}

function cStatus(inv) {
  const rows = groupBy(inv,'status',{Amount:(s,r)=>s+r.amount});
  Plotly.react('c-status', [{labels:rows.map(r=>r.status),values:rows.map(r=>r.Amount),type:'pie',hole:.45,
    textinfo:'percent+label',textposition:'outside'}],
    {height:360,margin:{t:10,b:10}}, PLY);
}

function cGroup(opps) {
  const rows = groupBy(opps,'sales_group',{NRE:(s,r)=>s+r.nre_contract_value,Count:(s,r)=>s+1});
  const f = document.getElementById('s-grp-f').value;
  const d = srt(rows, f, document.getElementById('s-grp-o').value==='Ascending');
  Plotly.react('c-group', [{x:d.map(r=>r.sales_group),y:d.map(r=>r.NRE),text:d.map(r=>r.Count+' deals'),
    textposition:'outside',type:'bar',
    marker:{color:d.map((_,i)=>['#4C78A8','#54A24B','#E45756','#F5C518','#B07AA1'][i%5])}}],
    {height:340,yaxis:{title:'NRE Contract Value ($)'},showlegend:false,margin:{t:20,b:10}}, PLY);
}

function cTrend(inv) {
  const m = {};
  inv.forEach(r => { if(!r.invoice_date) return; const k=r.invoice_date.slice(0,7);
    if(!m[k]) m[k]={I:0,P:0,R:0};
    m[k].I+=r.amount; m[k].P+=r.amount_paid; m[k].R+=r.adjusted_unpaid_amount; });
  const ks = Object.keys(m).sort();
  Plotly.react('c-trend', [
    {x:ks,y:ks.map(k=>m[k].I),mode:'lines+markers',name:'Invoiced', line:{color:'#4C78A8',width:2}},
    {x:ks,y:ks.map(k=>m[k].P),mode:'lines+markers',name:'Paid',     line:{color:'#54A24B',width:2}},
    {x:ks,y:ks.map(k=>m[k].R),mode:'lines+markers',name:'Remaining',line:{color:'#E45756',width:2}},
  ], {height:340,xaxis:{tickangle:-40},yaxis:{title:'Amount ($)'},legend:{orientation:'h',y:1.1},margin:{t:10,b:10}}, PLY);
}

function cHeatmap(inv) {
  const metric = document.querySelector('input[name="hm"]:checked').value;
  const map = {};
  inv.forEach(r => {
    const y = yr(r.invoice_date); if(!y) return;
    const s = r.status||'Unknown';
    if(!map[y]) map[y] = {}; if(!map[y][s]) map[y][s]={Amount:0,Count:0};
    map[y][s].Amount += r.amount; map[y][s].Count++;
  });
  const years = Object.keys(map).sort();
  const stats = [...new Set(inv.map(r=>r.status).filter(Boolean))].sort();
  const z = years.map(y => stats.map(s => map[y]?.[s]?.[metric] ?? 0));
  Plotly.react('c-heatmap', [{z,x:stats,y:years,type:'heatmap',colorscale:'Blues',
    text:z, texttemplate: metric==='Amount'? '$%{text:.2s}' : '%{text}'}],
    {height:240,margin:{t:10,b:40,l:60}}, PLY);
}

function cLineItems(li) {
  const rows = groupBy(li,'description',{Amount:(s,r)=>s+r.amount,Paid:(s,r)=>s+r.amount_paid,Remaining:(s,r)=>s+r.adjusted_unpaid_amount,Count:(s,r)=>s+1});
  const f = document.getElementById('s-li-f').value;
  const d = srt(rows, f, document.getElementById('s-li-o').value==='Ascending').slice(-20);
  Plotly.react('c-lineitems', [
    {name:'Amount',    y:d.map(r=>r.description),x:d.map(r=>r.Amount),    type:'bar',orientation:'h',marker:{color:'#4C78A8'}},
    {name:'Paid',      y:d.map(r=>r.description),x:d.map(r=>r.Paid),      type:'bar',orientation:'h',marker:{color:'#54A24B'}},
    {name:'Remaining', y:d.map(r=>r.description),x:d.map(r=>r.Remaining), type:'bar',orientation:'h',marker:{color:'#E45756'}},
  ], {barmode:'group',height:540,xaxis:{title:'Amount ($)'},legend:{orientation:'h',y:1.02},margin:{t:10,b:10,l:320}}, PLY);
}

function cAging(inv) {
  const COLORS = {Current:'#54A24B','1-30 days':'#F5C518','31-60 days':'#FFA500','61-90 days':'#E45756','90+ days':'#8B0000','No due date':'#AAAAAA'};
  const ORDER  = ['Current','1-30 days','31-60 days','61-90 days','90+ days','No due date'];
  function bkt(s) { const d=daysAgo(s); if(d===null) return 'No due date'; if(d<=0) return 'Current'; if(d<=30) return '1-30 days'; if(d<=60) return '31-60 days'; if(d<=90) return '61-90 days'; return '90+ days'; }
  const map = {};
  inv.filter(r=>r.adjusted_unpaid_amount>0).forEach(r => { const b=bkt(r.invoice_due_date); if(!map[b]) map[b]={Outstanding:0,Count:0}; map[b].Outstanding+=r.adjusted_unpaid_amount; map[b].Count++; });
  const f = document.getElementById('s-ag-f').value;
  const asc = document.getElementById('s-ag-o').value==='Ascending';
  let rows = ORDER.map(b=>({bucket:b,...(map[b]||{Outstanding:0,Count:0})})).filter(r=>r.Outstanding>0||r.Count>0);
  rows.sort((a,b)=>asc? a[f]-b[f] : b[f]-a[f]);
  Plotly.react('c-aging', [{x:rows.map(r=>r.bucket),y:rows.map(r=>r.Outstanding),text:rows.map(r=>r.Count+' inv'),
    textposition:'outside',type:'bar',marker:{color:rows.map(r=>COLORS[r.bucket])}}],
    {height:340,yaxis:{title:'Outstanding ($)'},showlegend:false,margin:{t:30,b:10}}, PLY);
}

// ── Tables ────────────────────────────────────────────────────────────────────
function mkTable(id, rows, cols) {
  if(!rows.length){document.getElementById(id).innerHTML='<p style="color:#888;padding:10px">No data for current filters.</p>';return;}
  let h='<div style="overflow-x:auto"><table class="table table-sm table-striped table-hover"><thead><tr>';
  cols.forEach(c=>h+=`<th>${c.l}</th>`); h+='</tr></thead><tbody>';
  rows.slice(0,300).forEach(r=>{
    h+='<tr>';
    cols.forEach(c=>{let v=r[c.k]??''; if(c.m&&v!=='')v=$m(parseFloat(v)||0); h+=`<td>${v}</td>`;});
    h+='</tr>';
  });
  h+='</tbody></table></div>';
  document.getElementById(id).innerHTML=h;
}

// ── Main render ───────────────────────────────────────────────────────────────
function renderAll() {
  const {fOpps, fInv, fLi} = applyFilters();
  updateKPIs(fOpps, fInv);
  cAccount(fOpps, fInv);
  cStatus(fInv);
  cGroup(fOpps);
  cTrend(fInv);
  cHeatmap(fInv);
  cLineItems(fLi);
  cAging(fInv);

  mkTable('tbl-opps', fOpps, [
    {k:'opportunity_name',   l:'Opportunity'},
    {k:'account_name',       l:'Account'},
    {k:'global_client_name', l:'Global Client'},
    {k:'sales_group',        l:'Sales Group'},
    {k:'product',            l:'Product'},
    {k:'close_date',         l:'Close Date'},
    {k:'nre_contract_value', l:'NRE Value', m:true},
  ]);
  mkTable('tbl-inv', fInv.map(r=>({...r, billed_year:yr(r.invoice_date), paid_year:yr(r.invoice_closed_date)})), [
    {k:'invoice_date',           l:'Invoice Date'},
    {k:'invoice_number',         l:'Invoice #'},
    {k:'name',                   l:'Customer'},
    {k:'status',                 l:'Status'},
    {k:'billed_year',            l:'Billed Year'},
    {k:'paid_year',              l:'Paid Year'},
    {k:'amount',                 l:'Amount', m:true},
    {k:'amount_paid',            l:'Paid', m:true},
    {k:'adjusted_unpaid_amount', l:'Outstanding', m:true},
    {k:'invoice_due_date',       l:'Due Date'},
    {k:'invoice_closed_date',    l:'Closed Date'},
  ]);
  mkTable('tbl-li', fLi, [
    {k:'invoice_date',           l:'Invoice Date'},
    {k:'invoice_number',         l:'Invoice #'},
    {k:'name',                   l:'Customer'},
    {k:'item',                   l:'Item'},
    {k:'description',            l:'Description'},
    {k:'bitgo_lineitem',         l:'BitGo Line Item'},
    {k:'amount',                 l:'Amount', m:true},
    {k:'amount_paid',            l:'Paid', m:true},
    {k:'adjusted_unpaid_amount', l:'Outstanding', m:true},
    {k:'status',                 l:'Status'},
  ]);
}

// ── Auto-refresh countdown + page reload ──────────────────────────────────────
const REFRESH_MS = 15 * 60 * 1000;
const startedAt  = Date.now();
function tick() {
  const left = REFRESH_MS - ((Date.now() - startedAt) % REFRESH_MS);
  const m = Math.floor(left/60000), s = Math.floor((left%60000)/1000);
  document.getElementById('countdown').textContent = `Next refresh: ${m}m ${s < 10 ? '0'+s : s}s`;
}
setInterval(tick, 1000); tick();
setTimeout(() => location.reload(), REFRESH_MS);

// ── Boot ──────────────────────────────────────────────────────────────────────
renderAll();
</script>
</body>
</html>"""

# ── Inject data into HTML ─────────────────────────────────────────────────────
html = (HTML_TEMPLATE
        .replace("%%OPPS_JSON%%",     opps_json)
        .replace("%%INV_JSON%%",      inv_json)
        .replace("%%LI_JSON%%",       li_json)
        .replace("%%GENERATED_AT%%",  generated_at))

# Base64-encode to safely embed in the Python output file (avoids triple-quote issues)
html_b64 = base64.b64encode(html.encode("utf-8")).decode("ascii")

# ── Write the self-contained server file ──────────────────────────────────────
server_code = f'''"""
NRE Sales & Invoice Dashboard — Self-Contained Server
======================================================
Generated : {generated_at}
Data      : {len(df_opps)} opportunities | {len(df_inv)} invoice headers | {len(df_li)} line items

HOW TO USE
----------
Run this file:
    python nre_dashboard_server.py

A browser window opens automatically at http://localhost:8502
with the full interactive dashboard — all filters, charts, and tables.

No Snowflake, no GitHub, no pip installs required beyond the standard library.
(Plotly.js and Bootstrap are loaded from CDN — internet required for first load.)
"""
import base64
import http.server
import socketserver
import threading
import webbrowser
import sys

PORT = 8502

_HTML_B64 = "{html_b64}"
HTML = base64.b64decode(_HTML_B64).decode("utf-8")

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(HTML.encode("utf-8"))))
        self.end_headers()
        self.wfile.write(HTML.encode("utf-8"))
    def log_message(self, fmt, *args):
        pass  # suppress server logs

def open_browser():
    webbrowser.open(f"http://localhost:{{PORT}}")

print("=" * 55)
print("  NRE Sales & Invoice Dashboard")
print(f"  Data as of : {generated_at}")
print("=" * 55)
print(f"  URL  →  http://localhost:{{PORT}}")
print("  Press Ctrl+C to stop.")
print("=" * 55)

threading.Timer(1.2, open_browser).start()

try:
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        httpd.serve_forever()
except OSError:
    PORT = 8503
    print(f"  Port 8502 busy, trying http://localhost:{{PORT}}")
    threading.Timer(0.5, lambda: webbrowser.open(f"http://localhost:{{PORT}}")).start()
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        httpd.serve_forever()
except KeyboardInterrupt:
    print("\\nServer stopped.")
    sys.exit(0)
'''

OUT_FILE.write_text(server_code, encoding="utf-8")
size_kb = OUT_FILE.stat().st_size / 1024
print(f"\nGenerated : {OUT_FILE.name}  ({size_kb:.0f} KB)")
print("\nTo run:")
print(f"  python {OUT_FILE.name}")
print("\nTo share: send nre_dashboard_server.py to anyone.")
print('They run it — browser opens automatically with the full dashboard.')
