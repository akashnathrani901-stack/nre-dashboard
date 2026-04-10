"""
Generates nre_dashboard_server.py — run it to open the full dashboard in browser.
"""
import base64, json, pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
OUT_FILE  = Path(__file__).parent / "nre_dashboard_server.py"

def load(name):
    df = pd.read_parquet(DATA_DIR / f"{name}_cache.parquet")
    df.columns = df.columns.str.lower()
    for col in df.select_dtypes(include=["datetime64[ns]","datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str).replace("NaT","")
    return df

print("Loading data…")
df_opps = load("opps"); df_inv = load("invoices"); df_li = load("lineitems")
gen = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NRE Sales Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f0f2f6;display:flex;height:100vh;overflow:hidden}

/* ── Sidebar ─────────────────────────────────── */
#sidebar{width:260px;min-width:260px;background:#1a1f36;color:#c5cae9;height:100vh;overflow-y:auto;display:flex;flex-direction:column;padding:0}
.sb-header{padding:18px 16px 12px;border-bottom:1px solid #2e3557}
.sb-logo{font-size:1rem;font-weight:700;color:#fff;letter-spacing:.3px}
.sb-sub{font-size:.68rem;color:#7986cb;margin-top:2px}
.sb-refresh{background:#1f2647;border-radius:6px;padding:7px 10px;margin:10px 14px;font-size:.68rem;color:#90caf9}
#countdown{color:#64b5f6;font-weight:600}
.sb-section{padding:10px 14px 4px;font-size:.68rem;font-weight:700;color:#7986cb;text-transform:uppercase;letter-spacing:.8px}
.sb-divider{border:none;border-top:1px solid #2e3557;margin:8px 0}
.flbl{font-size:.72rem;color:#b0bec5;margin:8px 14px 3px;display:block}
#sidebar select[multiple]{width:calc(100% - 28px);margin:0 14px;background:#1f2647;color:#e8eaf6;border:1px solid #3949ab;border-radius:5px;font-size:.73rem;padding:3px}
#sidebar select[multiple] option:checked{background:#3949ab;color:#fff}
.cond-box{margin:6px 14px;background:#1f2647;border:1px solid #3949ab;border-radius:6px;padding:7px 8px}
.cond-lbl{font-size:.7rem;color:#9fa8da;text-align:center;margin-bottom:5px}
.cond-opts{display:flex;justify-content:space-around}
.cond-opts label{font-size:.72rem;color:#c5cae9;cursor:pointer;display:flex;align-items:center;gap:3px}
.cond-opts input[type=radio]{accent-color:#7986cb}
.cond-hint{font-size:.66rem;color:#7986cb;text-align:center;margin-top:4px}
.tip-box{margin:8px 14px;font-size:.66rem;color:#78909c;background:#1a1f36;border-left:2px solid #3949ab;padding:5px 7px}
.sb-footer{margin-top:auto;padding:10px 14px;font-size:.65rem;color:#546e7a;border-top:1px solid #2e3557}

/* ── Main ────────────────────────────────────── */
#main{flex:1;overflow-y:auto;padding:16px}
.page-title{font-size:1.2rem;font-weight:700;color:#1a237e;margin-bottom:2px}
.page-sub{font-size:.75rem;color:#78909c;margin-bottom:14px}

/* ── KPI Cards ───────────────────────────────── */
.kpi-row{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.kpi{flex:1;min-width:110px;background:#fff;border-radius:10px;padding:12px 14px;box-shadow:0 1px 6px rgba(0,0,0,.08);border-top:3px solid #e8eaf6}
.kpi.blue{border-top-color:#1565c0}.kpi.teal{border-top-color:#00695c}.kpi.green{border-top-color:#2e7d32}
.kpi.red{border-top-color:#c62828}.kpi.orange{border-top-color:#e65100}.kpi.purple{border-top-color:#4527a0}
.kpi.indigo{border-top-color:#283593}
.kpi-val{font-size:1.25rem;font-weight:700;color:#1a237e;line-height:1.2}
.kpi-lbl{font-size:.68rem;color:#78909c;margin-top:3px;text-transform:uppercase;letter-spacing:.4px}

/* ── Chart Cards ─────────────────────────────── */
.chart-card{background:#fff;border-radius:10px;box-shadow:0 1px 6px rgba(0,0,0,.08);padding:14px 16px;margin-bottom:14px}
.chart-title{font-size:.88rem;font-weight:600;color:#1a237e;margin-bottom:4px}
.chart-sub{font-size:.7rem;color:#90a4ae;margin-bottom:8px}
.row2{display:grid;grid-template-columns:1.8fr 1fr;gap:14px;margin-bottom:14px}
.row2-eq{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}

/* ── Sort bar ────────────────────────────────── */
.sort-bar{display:flex;align-items:center;gap:6px;margin-bottom:6px}
.sort-bar span{font-size:.7rem;color:#90a4ae}
.sort-bar select{font-size:.72rem;padding:3px 8px;border:1px solid #e0e0e0;border-radius:5px;background:#fafafa;color:#37474f;cursor:pointer}

/* ── Heatmap radio ───────────────────────────── */
.radio-bar{display:flex;gap:12px;margin-bottom:6px}
.radio-bar label{font-size:.75rem;color:#546e7a;cursor:pointer;display:flex;align-items:center;gap:4px}

/* ── Tables ──────────────────────────────────── */
.tab-nav{display:flex;border-bottom:2px solid #e8eaf6;margin-bottom:10px}
.tab-btn{padding:7px 14px;font-size:.78rem;font-weight:600;color:#78909c;cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;margin-bottom:-2px}
.tab-btn.active{color:#1565c0;border-bottom-color:#1565c0}
.tab-pane{display:none}.tab-pane.active{display:block}
.tbl-wrap{overflow-x:auto;max-height:340px;overflow-y:auto}
table{width:100%;border-collapse:collapse;font-size:.74rem}
thead th{background:#e8eaf6;color:#283593;font-weight:600;padding:7px 10px;position:sticky;top:0;white-space:nowrap;border-bottom:2px solid #c5cae9}
tbody tr:hover{background:#f3f4ff}
tbody td{padding:6px 10px;border-bottom:1px solid #f0f0f0;color:#37474f;white-space:nowrap}
tbody tr:nth-child(even) td{background:#fafbff}

/* ── Loading overlay ─────────────────────────── */
#loading{position:fixed;inset:0;background:rgba(26,31,54,.85);display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:9999;color:#fff}
.spinner{width:44px;height:44px;border:4px solid rgba(255,255,255,.2);border-top-color:#7986cb;border-radius:50%;animation:spin .8s linear infinite;margin-bottom:12px}
@keyframes spin{to{transform:rotate(360deg)}}
.ld-text{font-size:.9rem;color:#c5cae9}
</style>
</head>
<body>

<div id="loading"><div class="spinner"></div><div class="ld-text">Loading dashboard…</div></div>

<!-- ═══ SIDEBAR ═══════════════════════════════════════════════════════════ -->
<div id="sidebar">
  <div class="sb-header">
    <div class="sb-logo">⬡ NRE Dashboard</div>
    <div class="sb-sub">BitGo · Closed Won · Since Jan 2025</div>
  </div>

  <div class="sb-refresh">
    ⟳ &nbsp;Auto-refresh every 15 min<br>
    <span id="countdown">Calculating…</span>
  </div>

  <div class="sb-section">Filters</div>

  <span class="flbl">Sales Group</span>
  <select multiple id="f-group" style="height:72px;" onchange="renderAll()"></select>

  <span class="flbl">Account</span>
  <select multiple id="f-account" style="height:88px;" onchange="renderAll()"></select>

  <hr class="sb-divider">
  <div class="sb-section">Invoice Year</div>

  <span class="flbl">Billed Year</span>
  <select multiple id="f-billed" style="height:64px;" onchange="renderAll()"></select>

  <div class="cond-box">
    <div class="cond-lbl">── Year Condition ──</div>
    <div class="cond-opts">
      <label><input type="radio" name="yc" value="AND" checked onchange="onCond()"> AND</label>
      <label><input type="radio" name="yc" value="OR"  onchange="onCond()"> OR</label>
      <label><input type="radio" name="yc" value="SAME" onchange="onCond()"> Same Year</label>
    </div>
    <div class="cond-hint" id="cond-hint"></div>
  </div>

  <span class="flbl" id="paid-lbl">Paid / Closed Year</span>
  <select multiple id="f-paid" style="height:64px;" onchange="renderAll()"></select>

  <hr class="sb-divider">
  <div class="sb-section">Status</div>
  <select multiple id="f-status" style="height:72px;" onchange="renderAll()"></select>

  <div class="tip-box">Combine Billed Year + Open status to find unpaid invoices from prior years.</div>
  <div class="sb-footer">Data as of %%GEN%%<br>github.com/akashnathrani901-stack</div>
</div>

<!-- ═══ MAIN ══════════════════════════════════════════════════════════════ -->
<div id="main">
  <div class="page-title">NRE Sales &amp; Invoice Dashboard</div>
  <div class="page-sub">Closed Won opportunities with NRE &gt; 0 · Since Jan 2025</div>

  <!-- KPIs -->
  <div class="kpi-row">
    <div class="kpi blue"> <div class="kpi-val" id="k-deals">—</div><div class="kpi-lbl">Deals</div></div>
    <div class="kpi indigo"><div class="kpi-val" id="k-inv">—</div><div class="kpi-lbl">Invoices</div></div>
    <div class="kpi purple"><div class="kpi-val" id="k-nre">—</div><div class="kpi-lbl">NRE Contract Value</div></div>
    <div class="kpi teal"> <div class="kpi-val" id="k-invoiced">—</div><div class="kpi-lbl">Total Invoiced</div></div>
    <div class="kpi green"><div class="kpi-val" id="k-paid">—</div><div class="kpi-lbl">Total Paid</div></div>
    <div class="kpi red">  <div class="kpi-val" id="k-out">—</div><div class="kpi-lbl">Outstanding</div></div>
    <div class="kpi orange"><div class="kpi-val" id="k-rate">—</div><div class="kpi-lbl">Collection Rate</div></div>
  </div>

  <!-- Row 1: Account bar + Status donut -->
  <div class="row2">
    <div class="chart-card">
      <div class="chart-title">Invoiced vs Paid vs Remaining by Account</div>
      <div class="sort-bar">
        <span>Sort by</span>
        <select id="s-ac-f" onchange="renderAll()"><option>Invoiced</option><option>Paid</option><option>Remaining</option><option>Count</option></select>
        <select id="s-ac-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
      </div>
      <div id="c-acct" style="height:340px"></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Invoice Status</div>
      <div class="chart-sub">Distribution by invoiced amount</div>
      <div id="c-status" style="height:340px"></div>
    </div>
  </div>

  <!-- Row 2: NRE by group + Monthly trend -->
  <div class="row2-eq">
    <div class="chart-card">
      <div class="chart-title">NRE Contract Value by Sales Group</div>
      <div class="sort-bar">
        <span>Sort by</span>
        <select id="s-gr-f" onchange="renderAll()"><option>NRE</option><option>Count</option></select>
        <select id="s-gr-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
      </div>
      <div id="c-grp" style="height:300px"></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Monthly Invoice Trend</div>
      <div class="chart-sub">Invoiced · Paid · Remaining over time</div>
      <div id="c-trend" style="height:300px"></div>
    </div>
  </div>

  <!-- Heatmap -->
  <div class="chart-card">
    <div class="chart-title">Billed Year × Invoice Status — Heatmap</div>
    <div class="chart-sub">Quickly spot open invoices from prior years</div>
    <div class="radio-bar">
      <label><input type="radio" name="hm" value="Amount" checked onchange="renderAll()"> Amount ($)</label>
      <label><input type="radio" name="hm" value="Count"  onchange="renderAll()"> Count</label>
    </div>
    <div id="c-heat" style="height:220px"></div>
  </div>

  <!-- Line items -->
  <div class="chart-card">
    <div class="chart-title">Top 20 Line Item Descriptions</div>
    <div class="sort-bar">
      <span>Sort by</span>
      <select id="s-li-f" onchange="renderAll()"><option>Amount</option><option>Paid</option><option>Remaining</option><option>Count</option></select>
      <select id="s-li-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
    </div>
    <div id="c-li" style="height:500px"></div>
  </div>

  <!-- Aging -->
  <div class="chart-card">
    <div class="chart-title">Invoice Aging — Outstanding Only</div>
    <div class="sort-bar">
      <span>Sort by</span>
      <select id="s-ag-f" onchange="renderAll()"><option>Outstanding</option><option>Count</option></select>
      <select id="s-ag-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
    </div>
    <div id="c-aging" style="height:300px"></div>
  </div>

  <!-- Tables -->
  <div class="chart-card">
    <div class="tab-nav">
      <button class="tab-btn active" onclick="switchTab('t-opps',this)">Opportunities</button>
      <button class="tab-btn" onclick="switchTab('t-inv',this)">Invoice Headers</button>
      <button class="tab-btn" onclick="switchTab('t-li',this)">Line Items</button>
    </div>
    <div class="tab-pane active" id="t-opps"><div class="tbl-wrap" id="tbl-opps"></div></div>
    <div class="tab-pane" id="t-inv"><div class="tbl-wrap" id="tbl-inv"></div></div>
    <div class="tab-pane" id="t-li"><div class="tbl-wrap" id="tbl-li"></div></div>
  </div>

  <div style="height:30px"></div>
</div><!-- /main -->

<script>
// ── DATA ─────────────────────────────────────────────────────────────────────
const OPPS = %%OPPS%%;
const INV  = %%INV%%;
const LI   = %%LI%%;

const NF = ['amount','amount_paid','amount_remaining','adjusted_unpaid_amount'];
INV.forEach(r => NF.forEach(f => r[f] = +r[f]||0));
LI.forEach(r  => NF.forEach(f => r[f] = +r[f]||0));
OPPS.forEach(r => r.nre_contract_value = +r.nre_contract_value||0);

// ── UTILS ────────────────────────────────────────────────────────────────────
const $m = n => '$'+(+n||0).toLocaleString('en-US',{maximumFractionDigits:0});
const today = new Date(); today.setHours(0,0,0,0);
const yr = s => { if(!s) return null; const d=new Date(s); return isNaN(d)?null:d.getFullYear(); };
const daysAgo = s => { if(!s) return null; const d=new Date(s); return isNaN(d)?null:Math.floor((today-d)/864e5); };
const sel = id => [...document.getElementById(id).selectedOptions].map(o=>o.value);
const nSel = id => sel(id).map(Number);

function populate(id, vals, all=true) {
  const el=document.getElementById(id); el.innerHTML='';
  vals.forEach(v=>{ const o=document.createElement('option'); o.value=v; o.text=v; o.selected=all; el.appendChild(o); });
}

function groupBy(arr, key, aggs) {
  const m={};
  arr.forEach(r=>{ const k=r[key]??'Unknown';
    if(!m[k]){ m[k]={[key]:k}; for(const a in aggs) m[k][a]=0; }
    for(const [a,fn] of Object.entries(aggs)) m[k][a]=fn(m[k][a],r);
  });
  return Object.values(m);
}
const sort = (arr,f,asc) => [...arr].sort((a,b)=>asc?a[f]-b[f]:b[f]-a[f]);

const PALETTE = ['#3949ab','#00897b','#e53935','#fb8c00','#8e24aa','#039be5','#43a047','#f4511e'];
const PLY_CFG = {responsive:true, displayModeBar:false};
const PLY_MARGIN = {t:10,b:10,l:10,r:10,pad:4};

// ── INIT FILTERS ─────────────────────────────────────────────────────────────
populate('f-group',   [...new Set(OPPS.map(o=>o.sales_group).filter(Boolean))].sort());
populate('f-account', [...new Set(OPPS.map(o=>o.account_name).filter(Boolean))].sort());
populate('f-billed',  [...new Set(INV.map(r=>yr(r.invoice_date)).filter(Boolean))].sort((a,b)=>b-a));
populate('f-paid',    [...new Set(INV.map(r=>yr(r.invoice_closed_date)).filter(Boolean))].sort((a,b)=>b-a), false);
populate('f-status',  [...new Set(INV.map(r=>r.status).filter(Boolean))].sort());

const COND_HINTS = {
  AND: 'Both billed AND paid year must match',
  OR:  'Either billed OR paid year can match',
  SAME:'Billed year = Paid year (no separate paid filter)'
};
function onCond() {
  const c = document.querySelector('input[name="yc"]:checked').value;
  document.getElementById('cond-hint').textContent = COND_HINTS[c];
  const dis = c==='SAME';
  document.getElementById('f-paid').disabled = dis;
  document.getElementById('paid-lbl').style.opacity = dis ? '.4' : '1';
  renderAll();
}
document.querySelectorAll('input[name="yc"]').forEach(r=>r.addEventListener('change',onCond));
onCond();

// ── FILTERS ──────────────────────────────────────────────────────────────────
function applyFilters() {
  const sg=sel('f-group'), ac=sel('f-account'), by=nSel('f-billed'), py=nSel('f-paid'),
        st=sel('f-status'), yc=document.querySelector('input[name="yc"]:checked').value;

  let fO = OPPS.filter(o=>sg.includes(o.sales_group)&&ac.includes(o.account_name));
  const sIDs = new Set(fO.map(o=>o.sfdc_18_id));
  let fI = INV.filter(i=>sIDs.has(i.salesforce_id));

  if(yc==='AND'){
    const bm=by.length?i=>by.includes(yr(i.invoice_date)):()=>true;
    const pm=py.length?i=>py.includes(yr(i.invoice_closed_date)):()=>true;
    fI=fI.filter(i=>bm(i)&&pm(i));
  } else if(yc==='OR'){
    if(by.length||py.length)
      fI=fI.filter(i=>(by.length&&by.includes(yr(i.invoice_date)))||(py.length&&py.includes(yr(i.invoice_closed_date))));
  } else {
    const bm=by.length?i=>by.includes(yr(i.invoice_date)):()=>true;
    fI=fI.filter(i=>bm(i)&&yr(i.invoice_date)===yr(i.invoice_closed_date));
  }
  if(st.length) fI=fI.filter(i=>st.includes(i.status));

  const fSIDs=new Set(fI.map(i=>i.salesforce_id));
  fO=fO.filter(o=>fSIDs.has(o.sfdc_18_id));
  const fL=LI.filter(i=>sIDs.has(i.salesforce_id)&&(by.length?by.includes(yr(i.invoice_date)):true));
  return {fO,fI,fL};
}

// ── KPIs ─────────────────────────────────────────────────────────────────────
function updateKPIs(O,I) {
  const nre=O.reduce((s,r)=>s+r.nre_contract_value,0);
  const inv=I.reduce((s,r)=>s+r.amount,0);
  const paid=I.reduce((s,r)=>s+r.amount_paid,0);
  const out=I.reduce((s,r)=>s+r.adjusted_unpaid_amount,0);
  document.getElementById('k-deals').textContent    = O.length;
  document.getElementById('k-inv').textContent      = new Set(I.map(r=>r.invoice_number)).size;
  document.getElementById('k-nre').textContent      = $m(nre);
  document.getElementById('k-invoiced').textContent = $m(inv);
  document.getElementById('k-paid').textContent     = $m(paid);
  document.getElementById('k-out').textContent      = $m(out);
  document.getElementById('k-rate').textContent     = (inv?paid/inv*100:0).toFixed(1)+'%';
}

// ── CHART HELPERS ─────────────────────────────────────────────────────────────
const FONT = {family:'-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif', size:11, color:'#546e7a'};
function layout(extra) {
  return {margin:PLY_MARGIN, font:FONT, paper_bgcolor:'#fff', plot_bgcolor:'#fff',
    legend:{font:{size:10},orientation:'h',y:1.08,x:0}, ...extra};
}

// ── CHARTS ────────────────────────────────────────────────────────────────────
function cAcct(O,I) {
  const aMap={}; O.forEach(o=>aMap[o.sfdc_18_id]=o.account_name);
  const rows=groupBy(I.map(i=>({...i,_ac:aMap[i.salesforce_id]||'Unknown'})),'_ac',
    {Invoiced:(s,r)=>s+r.amount,Paid:(s,r)=>s+r.amount_paid,Remaining:(s,r)=>s+r.adjusted_unpaid_amount,Count:(s,r)=>s+1});
  const f=document.getElementById('s-ac-f').value, asc=document.getElementById('s-ac-o').value==='Ascending';
  const d=sort(rows,f,asc).slice(0,15);
  Plotly.react('c-acct',[
    {name:'Invoiced', x:d.map(r=>r._ac),y:d.map(r=>r.Invoiced), type:'bar',marker:{color:'#3949ab'},hovertemplate:'%{x}<br>Invoiced: $%{y:,.0f}<extra></extra>'},
    {name:'Paid',     x:d.map(r=>r._ac),y:d.map(r=>r.Paid),     type:'bar',marker:{color:'#00897b'},hovertemplate:'%{x}<br>Paid: $%{y:,.0f}<extra></extra>'},
    {name:'Remaining',x:d.map(r=>r._ac),y:d.map(r=>r.Remaining),type:'bar',marker:{color:'#e53935'},hovertemplate:'%{x}<br>Remaining: $%{y:,.0f}<extra></extra>'},
  ],layout({barmode:'group',xaxis:{tickangle:-35,tickfont:{size:10}},yaxis:{title:'Amount ($)',gridcolor:'#f0f0f0'}}),PLY_CFG);
}

function cStatus(I) {
  const rows=groupBy(I,'status',{amt:(s,r)=>s+r.amount});
  Plotly.react('c-status',[{
    labels:rows.map(r=>r.status), values:rows.map(r=>r.amt), type:'pie', hole:.42,
    marker:{colors:PALETTE},
    textinfo:'percent+label', textposition:'outside', textfont:{size:10},
    hovertemplate:'%{label}<br>$%{value:,.0f} (%{percent})<extra></extra>',
  }],layout({showlegend:false,margin:{t:20,b:20,l:20,r:20}}),PLY_CFG);
}

function cGrp(O) {
  const rows=groupBy(O,'sales_group',{NRE:(s,r)=>s+r.nre_contract_value,Count:(s,r)=>s+1});
  const f=document.getElementById('s-gr-f').value, asc=document.getElementById('s-gr-o').value==='Ascending';
  const d=sort(rows,f,asc);
  Plotly.react('c-grp',[{
    x:d.map(r=>r.sales_group), y:d.map(r=>r.NRE),
    text:d.map(r=>r.Count+' deals'), textposition:'outside', textfont:{size:10},
    type:'bar', marker:{color:d.map((_,i)=>PALETTE[i%PALETTE.length])},
    hovertemplate:'%{x}<br>NRE: $%{y:,.0f}<extra></extra>',
  }],layout({showlegend:false,yaxis:{title:'NRE ($)',gridcolor:'#f0f0f0'},xaxis:{tickfont:{size:10}}}),PLY_CFG);
}

function cTrend(I) {
  const m={};
  I.forEach(r=>{ if(!r.invoice_date) return; const k=r.invoice_date.slice(0,7);
    if(!m[k]) m[k]={I:0,P:0,R:0};
    m[k].I+=r.amount; m[k].P+=r.amount_paid; m[k].R+=r.adjusted_unpaid_amount;});
  const ks=Object.keys(m).sort();
  Plotly.react('c-trend',[
    {x:ks,y:ks.map(k=>m[k].I),mode:'lines+markers',name:'Invoiced', line:{color:'#3949ab',width:2.5},marker:{size:5},hovertemplate:'%{x}<br>$%{y:,.0f}<extra>Invoiced</extra>'},
    {x:ks,y:ks.map(k=>m[k].P),mode:'lines+markers',name:'Paid',     line:{color:'#00897b',width:2.5},marker:{size:5},hovertemplate:'%{x}<br>$%{y:,.0f}<extra>Paid</extra>'},
    {x:ks,y:ks.map(k=>m[k].R),mode:'lines+markers',name:'Remaining',line:{color:'#e53935',width:2},  marker:{size:5},hovertemplate:'%{x}<br>$%{y:,.0f}<extra>Remaining</extra>'},
  ],layout({xaxis:{tickangle:-30,tickfont:{size:10}},yaxis:{title:'Amount ($)',gridcolor:'#f0f0f0'}}),PLY_CFG);
}

function cHeat(I) {
  const met=document.querySelector('input[name="hm"]:checked').value;
  const map={};
  I.forEach(r=>{ const y=yr(r.invoice_date); if(!y) return; const s=r.status||'Unknown';
    if(!map[y]) map[y]={}; if(!map[y][s]) map[y][s]={Amount:0,Count:0};
    map[y][s].Amount+=r.amount; map[y][s].Count++;});
  const years=Object.keys(map).sort(), stats=[...new Set(I.map(r=>r.status).filter(Boolean))].sort();
  const z=years.map(y=>stats.map(s=>map[y]?.[s]?.[met]??0));
  Plotly.react('c-heat',[{z,x:stats,y:years,type:'heatmap',colorscale:'Blues',
    text:z, texttemplate:met==='Amount'?'$%{text:.2s}':'%{text}',
    hovertemplate:'Year: %{y}<br>Status: %{x}<br>'+met+': %{z:,.0f}<extra></extra>',
  }],{...layout({margin:{t:10,b:50,l:60,r:10}}), xaxis:{tickfont:{size:10}}, yaxis:{tickfont:{size:11}}},PLY_CFG);
}

function cLI(L) {
  const rows=groupBy(L,'description',{Amount:(s,r)=>s+r.amount,Paid:(s,r)=>s+r.amount_paid,Remaining:(s,r)=>s+r.adjusted_unpaid_amount,Count:(s,r)=>s+1});
  const f=document.getElementById('s-li-f').value, asc=document.getElementById('s-li-o').value==='Ascending';
  const d=sort(rows,f,asc).slice(-20);
  const lbls=d.map(r=>r.description.length>45?r.description.slice(0,45)+'…':r.description);
  Plotly.react('c-li',[
    {name:'Amount',    y:lbls,x:d.map(r=>r.Amount),    type:'bar',orientation:'h',marker:{color:'#3949ab'},hovertemplate:'%{y}<br>Amount: $%{x:,.0f}<extra></extra>'},
    {name:'Paid',      y:lbls,x:d.map(r=>r.Paid),      type:'bar',orientation:'h',marker:{color:'#00897b'},hovertemplate:'%{y}<br>Paid: $%{x:,.0f}<extra></extra>'},
    {name:'Remaining', y:lbls,x:d.map(r=>r.Remaining), type:'bar',orientation:'h',marker:{color:'#e53935'},hovertemplate:'%{y}<br>Remaining: $%{x:,.0f}<extra></extra>'},
  ],layout({barmode:'group',xaxis:{title:'Amount ($)',gridcolor:'#f0f0f0'},yaxis:{tickfont:{size:9}},margin:{...PLY_MARGIN,l:280}}),PLY_CFG);
}

function cAging(I) {
  const C={'Current':'#43a047','1–30d':'#fdd835','31–60d':'#fb8c00','61–90d':'#e53935','90+d':'#b71c1c','No due date':'#90a4ae'};
  const ORD=['Current','1–30d','31–60d','61–90d','90+d','No due date'];
  const bkt=s=>{const d=daysAgo(s);if(d===null)return'No due date';if(d<=0)return'Current';if(d<=30)return'1–30d';if(d<=60)return'31–60d';if(d<=90)return'61–90d';return'90+d';};
  const m={};
  I.filter(r=>r.adjusted_unpaid_amount>0).forEach(r=>{const b=bkt(r.invoice_due_date);if(!m[b])m[b]={Outstanding:0,Count:0};m[b].Outstanding+=r.adjusted_unpaid_amount;m[b].Count++;});
  const f=document.getElementById('s-ag-f').value, asc=document.getElementById('s-ag-o').value==='Ascending';
  let rows=ORD.map(b=>({b,...(m[b]||{Outstanding:0,Count:0})})).filter(r=>r.Outstanding>0);
  rows.sort((a,z)=>asc?a[f]-z[f]:z[f]-a[f]);
  Plotly.react('c-aging',[{
    x:rows.map(r=>r.b), y:rows.map(r=>r.Outstanding),
    text:rows.map(r=>r.Count+' inv'), textposition:'outside', textfont:{size:10},
    type:'bar', marker:{color:rows.map(r=>C[r.b])},
    hovertemplate:'%{x}<br>Outstanding: $%{y:,.0f}<extra></extra>',
  }],layout({showlegend:false,yaxis:{title:'Outstanding ($)',gridcolor:'#f0f0f0'},xaxis:{tickfont:{size:10}}}),PLY_CFG);
}

// ── TABLES ────────────────────────────────────────────────────────────────────
function switchTab(id, btn) {
  document.querySelectorAll('.tab-pane').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById(id).classList.add('active'); btn.classList.add('active');
}

function mkTbl(id, rows, cols) {
  if(!rows.length){document.getElementById(id).innerHTML='<p style="color:#90a4ae;padding:12px;font-size:.8rem">No data for current filters.</p>';return;}
  let h='<table><thead><tr>'+cols.map(c=>`<th>${c.l}</th>`).join('')+'</tr></thead><tbody>';
  rows.slice(0,300).forEach(r=>{
    h+='<tr>'+cols.map(c=>{let v=r[c.k]??'';if(c.m&&v!=='')v=$m(+v||0);return`<td>${v}</td>`;}).join('')+'</tr>';
  });
  h+='</tbody></table>';
  document.getElementById(id).innerHTML=h;
}

// ── RENDER ALL ────────────────────────────────────────────────────────────────
function renderAll() {
  const {fO,fI,fL}=applyFilters();
  updateKPIs(fO,fI);
  cAcct(fO,fI); cStatus(fI); cGrp(fO); cTrend(fI); cHeat(fI); cLI(fL); cAging(fI);
  mkTbl('tbl-opps',fO,[
    {k:'opportunity_name',   l:'Opportunity'},{k:'account_name',l:'Account'},
    {k:'global_client_name', l:'Global Client'},{k:'sales_group',l:'Sales Group'},
    {k:'product',l:'Product'},{k:'close_date',l:'Close Date'},
    {k:'nre_contract_value', l:'NRE Value',m:true},
  ]);
  mkTbl('tbl-inv',fI.map(r=>({...r,by:yr(r.invoice_date),py:yr(r.invoice_closed_date)})),[
    {k:'invoice_date',l:'Invoice Date'},{k:'invoice_number',l:'Invoice #'},
    {k:'name',l:'Customer'},{k:'status',l:'Status'},{k:'by',l:'Billed Yr'},{k:'py',l:'Paid Yr'},
    {k:'amount',l:'Amount',m:true},{k:'amount_paid',l:'Paid',m:true},
    {k:'adjusted_unpaid_amount',l:'Outstanding',m:true},{k:'invoice_due_date',l:'Due Date'},
  ]);
  mkTbl('tbl-li',fL,[
    {k:'invoice_date',l:'Invoice Date'},{k:'invoice_number',l:'Invoice #'},
    {k:'name',l:'Customer'},{k:'item',l:'Item'},{k:'description',l:'Description'},
    {k:'bitgo_lineitem',l:'BitGo Line Item'},
    {k:'amount',l:'Amount',m:true},{k:'amount_paid',l:'Paid',m:true},
    {k:'adjusted_unpaid_amount',l:'Outstanding',m:true},{k:'status',l:'Status'},
  ]);
}

// ── COUNTDOWN + AUTO-RELOAD ───────────────────────────────────────────────────
const T0=Date.now(), RM=15*60*1000;
setInterval(()=>{
  const left=RM-((Date.now()-T0)%RM);
  const m=Math.floor(left/60000),s=Math.floor((left%60000)/1000);
  document.getElementById('countdown').textContent=`Next refresh: ${m}m ${s<10?'0'+s:s}s`;
},1000);
setTimeout(()=>location.reload(),RM);

// ── BOOT ──────────────────────────────────────────────────────────────────────
window.addEventListener('load',()=>{
  renderAll();
  document.getElementById('loading').style.display='none';
});
</script>
</body></html>"""

# Inject data
html = (HTML
  .replace("%%OPPS%%", df_opps.to_json(orient="records"))
  .replace("%%INV%%",  df_inv.to_json(orient="records"))
  .replace("%%LI%%",   df_li.to_json(orient="records"))
  .replace("%%GEN%%",  gen))

html_b64 = base64.b64encode(html.encode("utf-8")).decode("ascii")

server_py = f'''"""
NRE Sales & Invoice Dashboard — Self-Contained Server
Generated: {gen} | {len(df_opps)} deals · {len(df_inv)} invoices · {len(df_li)} line items

Run:  python nre_dashboard_server.py
Then open:  http://localhost:8502
"""
import base64, http.server, socketserver, threading, webbrowser, sys

PORT = 8502
_B64 = "{html_b64}"
HTML = base64.b64decode(_B64).decode("utf-8")

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        b = HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-type","text/html; charset=utf-8")
        self.send_header("Content-Length",str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass

def run(port):
    with socketserver.TCPServer(("",port),H) as s:
        print(f"  Dashboard → http://localhost:{{port}}")
        print("  Press Ctrl+C to stop.\\n")
        threading.Timer(1.2, lambda: webbrowser.open(f"http://localhost:{{port}}")).start()
        s.serve_forever()

print("\\n{'='*50}")
print("  NRE Sales & Invoice Dashboard")
print(f"  Data as of: {gen}")
print(f"{'='*50}")
try:    run(PORT)
except OSError: run(PORT+1)
except KeyboardInterrupt: print("\\nStopped."); sys.exit(0)
'''

OUT_FILE.write_text(server_py, encoding="utf-8")
kb = OUT_FILE.stat().st_size // 1024
print(f"\nGenerated: {OUT_FILE.name}  ({kb} KB)")
print(f"Run with:  python {OUT_FILE.name}")
