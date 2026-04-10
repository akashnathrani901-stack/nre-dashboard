"""
Generates nre_dashboard.html — a completely self-contained HTML file.
Plotly.js is inlined. No server, no CDN, no network requests.
Just open the file in any browser.

Run:  python make_claude_file.py
Then: the browser opens automatically.
"""
import json, pandas as pd, webbrowser
from pathlib import Path

DATA_DIR  = Path(__file__).parent / "data"
OUT_HTML  = Path(__file__).parent / "nre_dashboard.html"

# -- Load parquet files --------------------------------------------------------
def load(name):
    df = pd.read_parquet(DATA_DIR / f"{name}_cache.parquet")
    df.columns = df.columns.str.lower()
    for col in df.select_dtypes(include=["datetime64[ns]","datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str).replace("NaT","")
    return df

print("Loading data...")
df_opps = load("opps"); df_inv = load("invoices"); df_li = load("lineitems")
gen = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
print(f"  {len(df_opps)} opportunities | {len(df_inv)} invoices | {len(df_li)} line items")

# -- Load local Plotly.js ------------------------------------------------------
import plotly as _plt, os
PLOTLY_PATH = Path(os.path.dirname(_plt.__file__)) / "package_data" / "plotly.min.js"
print(f"Loading Plotly.js from {PLOTLY_PATH} ...")
plotly_js = PLOTLY_PATH.read_text(encoding="utf-8")
print(f"  Plotly.js size: {len(plotly_js)//1024} KB")

# -- Serialize data ------------------------------------------------------------
opps_json = df_opps.to_json(orient="records")
inv_json  = df_inv.to_json(orient="records")
li_json   = df_li.to_json(orient="records")

# -- Build HTML ----------------------------------------------------------------
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NRE Sales Dashboard</title>
<script>
{plotly_js}
</script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f6;display:flex;height:100vh;overflow:hidden}}
#sidebar{{width:260px;min-width:260px;background:#1a1f36;color:#c5cae9;height:100vh;overflow-y:auto;padding:0;flex-shrink:0}}
.sb-head{{padding:16px;border-bottom:1px solid #2e3557}}
.sb-logo{{font-size:1rem;font-weight:700;color:#fff}}
.sb-sub{{font-size:.68rem;color:#7986cb;margin-top:2px}}
.sb-pill{{background:#1f2647;border-radius:6px;padding:7px 10px;margin:10px 14px;font-size:.68rem;color:#90caf9}}
#countdown{{color:#64b5f6;font-weight:600}}
.sb-sec{{padding:10px 14px 3px;font-size:.66rem;font-weight:700;color:#7986cb;text-transform:uppercase;letter-spacing:.8px}}
hr.sb{{border:none;border-top:1px solid #2e3557;margin:6px 0}}
.flbl{{font-size:.72rem;color:#b0bec5;margin:7px 14px 2px;display:block}}
#sidebar select[multiple]{{width:calc(100% - 28px);margin:0 14px;background:#1f2647;color:#e8eaf6;border:1px solid #3949ab;border-radius:5px;font-size:.73rem;padding:3px}}
#sidebar select[multiple] option:checked{{background:#3949ab;color:#fff}}
.cond-box{{margin:6px 14px;background:#1f2647;border:1px solid #3949ab;border-radius:6px;padding:7px 8px}}
.cond-title{{font-size:.68rem;color:#9fa8da;text-align:center;margin-bottom:4px}}
.cond-opts{{display:flex;justify-content:space-around}}
.cond-opts label{{font-size:.72rem;color:#c5cae9;cursor:pointer;display:flex;align-items:center;gap:3px}}
.cond-hint{{font-size:.65rem;color:#7986cb;text-align:center;margin-top:3px}}
.tip{{margin:8px 14px;font-size:.66rem;color:#78909c;border-left:2px solid #3949ab;padding:4px 7px}}
.sb-foot{{padding:10px 14px;font-size:.64rem;color:#546e7a;border-top:1px solid #2e3557;margin-top:auto}}
#main{{flex:1;overflow-y:auto;padding:16px}}
.pg-title{{font-size:1.15rem;font-weight:700;color:#1a237e;margin-bottom:2px}}
.pg-sub{{font-size:.74rem;color:#78909c;margin-bottom:14px}}
.kpi-row{{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap}}
.kpi{{flex:1;min-width:110px;background:#fff;border-radius:10px;padding:12px 14px;box-shadow:0 1px 6px rgba(0,0,0,.08);border-top:3px solid #e8eaf6}}
.kpi.c1{{border-top-color:#1565c0}}.kpi.c2{{border-top-color:#283593}}.kpi.c3{{border-top-color:#4527a0}}
.kpi.c4{{border-top-color:#00695c}}.kpi.c5{{border-top-color:#2e7d32}}.kpi.c6{{border-top-color:#b71c1c}}.kpi.c7{{border-top-color:#e65100}}
.kpi-val{{font-size:1.2rem;font-weight:700;color:#1a237e;line-height:1.2}}
.kpi-lbl{{font-size:.66rem;color:#78909c;margin-top:2px;text-transform:uppercase;letter-spacing:.4px}}
.card{{background:#fff;border-radius:10px;box-shadow:0 1px 6px rgba(0,0,0,.08);padding:14px 16px;margin-bottom:14px}}
.c-title{{font-size:.88rem;font-weight:600;color:#1a237e;margin-bottom:3px}}
.c-sub{{font-size:.7rem;color:#90a4ae;margin-bottom:7px}}
.r2{{display:grid;grid-template-columns:1.8fr 1fr;gap:14px;margin-bottom:14px}}
.r2eq{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
.sort-bar{{display:flex;align-items:center;gap:6px;margin-bottom:6px}}
.sort-bar span{{font-size:.7rem;color:#90a4ae}}
.sort-bar select,.hm-bar select{{font-size:.72rem;padding:3px 8px;border:1px solid #e0e0e0;border-radius:5px;background:#fafafa;color:#37474f;cursor:pointer}}
.hm-bar{{display:flex;gap:12px;margin-bottom:6px}}
.hm-bar label{{font-size:.76rem;color:#546e7a;cursor:pointer;display:flex;align-items:center;gap:4px}}
.tab-nav{{display:flex;border-bottom:2px solid #e8eaf6;margin-bottom:10px}}
.tab-btn{{padding:7px 14px;font-size:.78rem;font-weight:600;color:#78909c;cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;margin-bottom:-2px;transition:color .15s}}
.tab-btn.active{{color:#1565c0;border-bottom-color:#1565c0}}
.tab-pane{{display:none}}.tab-pane.active{{display:block}}
.tbl-wrap{{overflow-x:auto;max-height:320px;overflow-y:auto}}
table{{width:100%;border-collapse:collapse;font-size:.74rem}}
thead th{{background:#e8eaf6;color:#283593;font-weight:600;padding:7px 10px;position:sticky;top:0;white-space:nowrap;border-bottom:2px solid #c5cae9}}
tbody tr:hover td{{background:#f0f4ff!important}}
tbody td{{padding:6px 10px;border-bottom:1px solid #f5f5f5;color:#37474f;white-space:nowrap}}
tbody tr:nth-child(even) td{{background:#fafbff}}
</style>
</head>
<body>

<div id="sidebar">
  <div class="sb-head">
    <div class="sb-logo">NRE Dashboard</div>
    <div class="sb-sub">BitGo · Closed Won · Since Jan 2025</div>
  </div>
  <div class="sb-pill">Auto-refreshes every 15 min<br><span id="countdown"></span></div>

  <div class="sb-sec">Filters</div>
  <span class="flbl">Sales Group</span>
  <select multiple id="f-group" style="height:70px;" onchange="renderAll()"></select>
  <span class="flbl">Account</span>
  <select multiple id="f-account" style="height:84px;" onchange="renderAll()"></select>

  <hr class="sb">
  <div class="sb-sec">Invoice Year</div>
  <span class="flbl">Billed Year</span>
  <select multiple id="f-billed" style="height:62px;" onchange="renderAll()"></select>

  <div class="cond-box">
    <div class="cond-title">Year Condition</div>
    <div class="cond-opts">
      <label><input type="radio" name="yc" value="AND" checked onchange="onCond()"> AND</label>
      <label><input type="radio" name="yc" value="OR" onchange="onCond()"> OR</label>
      <label><input type="radio" name="yc" value="SAME" onchange="onCond()"> Same Year</label>
    </div>
    <div class="cond-hint" id="cond-hint"></div>
  </div>

  <span class="flbl" id="paid-lbl">Paid / Closed Year</span>
  <select multiple id="f-paid" style="height:62px;" onchange="renderAll()"></select>

  <hr class="sb">
  <div class="sb-sec">Status</div>
  <select multiple id="f-status" style="height:70px;" onchange="renderAll()"></select>
  <div class="tip">Billed 2025 + Open = unpaid 2025 invoices</div>
  <div class="sb-foot">Data as of {gen}</div>
</div>

<div id="main">
  <div class="pg-title">NRE Sales &amp; Invoice Dashboard</div>
  <div class="pg-sub">Closed Won · NRE &gt; 0 · Since Jan 2025</div>

  <div class="kpi-row">
    <div class="kpi c1"><div class="kpi-val" id="k-deals">-</div><div class="kpi-lbl">Deals</div></div>
    <div class="kpi c2"><div class="kpi-val" id="k-inv">-</div><div class="kpi-lbl">Invoices</div></div>
    <div class="kpi c3"><div class="kpi-val" id="k-nre">-</div><div class="kpi-lbl">NRE Contract Value</div></div>
    <div class="kpi c4"><div class="kpi-val" id="k-invoiced">-</div><div class="kpi-lbl">Total Invoiced</div></div>
    <div class="kpi c5"><div class="kpi-val" id="k-paid">-</div><div class="kpi-lbl">Total Paid</div></div>
    <div class="kpi c6"><div class="kpi-val" id="k-out">-</div><div class="kpi-lbl">Outstanding</div></div>
    <div class="kpi c7"><div class="kpi-val" id="k-rate">-</div><div class="kpi-lbl">Collection Rate</div></div>
  </div>

  <div class="r2">
    <div class="card">
      <div class="c-title">Invoiced vs Paid vs Remaining by Account</div>
      <div class="sort-bar"><span>Sort by</span>
        <select id="s-ac-f" onchange="renderAll()"><option>Invoiced</option><option>Paid</option><option>Remaining</option><option>Count</option></select>
        <select id="s-ac-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
      </div>
      <div id="c-acct" style="height:320px"></div>
    </div>
    <div class="card">
      <div class="c-title">Invoice Status</div>
      <div class="c-sub">Distribution by invoiced amount</div>
      <div id="c-status" style="height:320px"></div>
    </div>
  </div>

  <div class="r2eq">
    <div class="card">
      <div class="c-title">NRE Contract Value by Sales Group</div>
      <div class="sort-bar"><span>Sort by</span>
        <select id="s-gr-f" onchange="renderAll()"><option>NRE</option><option>Count</option></select>
        <select id="s-gr-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
      </div>
      <div id="c-grp" style="height:280px"></div>
    </div>
    <div class="card">
      <div class="c-title">Monthly Invoice Trend</div>
      <div class="c-sub">Invoiced · Paid · Remaining over time</div>
      <div id="c-trend" style="height:280px"></div>
    </div>
  </div>

  <div class="card">
    <div class="c-title">Billed Year x Invoice Status Heatmap</div>
    <div class="c-sub">Spot open invoices from prior years</div>
    <div class="hm-bar">
      <label><input type="radio" name="hm" value="Amount" checked onchange="renderAll()"> Amount</label>
      <label><input type="radio" name="hm" value="Count" onchange="renderAll()"> Count</label>
    </div>
    <div id="c-heat" style="height:200px"></div>
  </div>

  <div class="card">
    <div class="c-title">Top 20 Line Item Descriptions</div>
    <div class="sort-bar"><span>Sort by</span>
      <select id="s-li-f" onchange="renderAll()"><option>Amount</option><option>Paid</option><option>Remaining</option><option>Count</option></select>
      <select id="s-li-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
    </div>
    <div id="c-li" style="height:480px"></div>
  </div>

  <div class="card">
    <div class="c-title">Invoice Aging - Outstanding Only</div>
    <div class="sort-bar"><span>Sort by</span>
      <select id="s-ag-f" onchange="renderAll()"><option>Outstanding</option><option>Count</option></select>
      <select id="s-ag-o" onchange="renderAll()"><option>Descending</option><option>Ascending</option></select>
    </div>
    <div id="c-aging" style="height:280px"></div>
  </div>

  <div class="card">
    <div class="tab-nav">
      <button class="tab-btn active" onclick="switchTab('t-opps',this)">Opportunities</button>
      <button class="tab-btn" onclick="switchTab('t-inv',this)">Invoice Headers</button>
      <button class="tab-btn" onclick="switchTab('t-li',this)">Line Items</button>
    </div>
    <div class="tab-pane active" id="t-opps"><div class="tbl-wrap" id="tbl-opps"></div></div>
    <div class="tab-pane" id="t-inv"><div class="tbl-wrap" id="tbl-inv"></div></div>
    <div class="tab-pane" id="t-li"><div class="tbl-wrap" id="tbl-li"></div></div>
  </div>
  <div style="height:24px"></div>
</div>

<script>
// DATA
var OPPS = {opps_json};
var INV  = {inv_json};
var LI   = {li_json};

// Parse numbers
var NF = ['amount','amount_paid','amount_remaining','adjusted_unpaid_amount'];
INV.forEach(function(r){{ NF.forEach(function(f){{ r[f]=+r[f]||0; }}); }});
LI.forEach(function(r){{  NF.forEach(function(f){{ r[f]=+r[f]||0; }}); }});
OPPS.forEach(function(r){{ r.nre_contract_value=+r.nre_contract_value||0; }});

// Utils
var $m = function(n){{ return '$'+(+n||0).toLocaleString('en-US',{{maximumFractionDigits:0}}); }};
var today = new Date(); today.setHours(0,0,0,0);
var yr = function(s){{ if(!s) return null; var d=new Date(s); return isNaN(d)?null:d.getFullYear(); }};
var dAgo = function(s){{ if(!s) return null; var d=new Date(s); return isNaN(d)?null:Math.floor((today-d)/864e5); }};
var sel = function(id){{ return Array.from(document.getElementById(id).selectedOptions).map(function(o){{return o.value;}}); }};
var nsel = function(id){{ return sel(id).map(Number); }};

function populate(id, vals, all) {{
  var el=document.getElementById(id); el.innerHTML='';
  vals.forEach(function(v){{ var o=document.createElement('option'); o.value=v; o.text=v; o.selected=(all!==false); el.appendChild(o); }});
}}

function groupBy(arr, key, aggs) {{
  var m={{}};
  arr.forEach(function(r){{
    var k=r[key]!=null?r[key]:'Unknown';
    if(!m[k]){{ m[k]={{}}; m[k][key]=k; Object.keys(aggs).forEach(function(a){{m[k][a]=0;}}); }}
    Object.keys(aggs).forEach(function(a){{ m[k][a]=aggs[a](m[k][a],r); }});
  }});
  return Object.values(m);
}}

function srt(arr,f,asc){{ return arr.slice().sort(function(a,b){{return asc?a[f]-b[f]:b[f]-a[f];}}); }}

var PAL=['#3949ab','#00897b','#e53935','#fb8c00','#8e24aa','#039be5','#43a047','#f4511e'];
var PLY={{responsive:true,displayModeBar:false}};
var MAR={{t:10,b:10,l:10,r:10}};
var FONT={{family:'-apple-system,"Segoe UI",sans-serif',size:11,color:'#546e7a'}};
function lay(extra){{
  var base={{margin:MAR,font:FONT,paper_bgcolor:'#fff',plot_bgcolor:'#fff',
    legend:{{font:{{size:10}},orientation:'h',y:1.08,x:0}}}};
  if(extra) Object.assign(base,extra);
  return base;
}}

// Init filters
populate('f-group',   Array.from(new Set(OPPS.map(function(o){{return o.sales_group;}})).values()).filter(Boolean).sort());
populate('f-account', Array.from(new Set(OPPS.map(function(o){{return o.account_name;}})).values()).filter(Boolean).sort());
populate('f-billed',  Array.from(new Set(INV.map(function(r){{return yr(r.invoice_date);}})).values()).filter(Boolean).sort(function(a,b){{return b-a;}}));
populate('f-paid',    Array.from(new Set(INV.map(function(r){{return yr(r.invoice_closed_date);}})).values()).filter(Boolean).sort(function(a,b){{return b-a;}}), false);
populate('f-status',  Array.from(new Set(INV.map(function(r){{return r.status;}})).values()).filter(Boolean).sort());

var HINTS={{AND:'Both billed AND paid year must match',OR:'Either billed OR paid year matches',SAME:'Billed year equals paid year'}};
function onCond(){{
  var c=document.querySelector('input[name="yc"]:checked').value;
  document.getElementById('cond-hint').textContent=HINTS[c];
  var dis=c==='SAME';
  document.getElementById('f-paid').disabled=dis;
  document.getElementById('paid-lbl').style.opacity=dis?'.35':'1';
  renderAll();
}}
document.querySelectorAll('input[name="yc"]').forEach(function(r){{r.addEventListener('change',onCond);}}); onCond();

function applyFilters(){{
  var sg=sel('f-group'),ac=sel('f-account'),by=nsel('f-billed'),py=nsel('f-paid'),
      st=sel('f-status'),yc=document.querySelector('input[name="yc"]:checked').value;
  var fO=OPPS.filter(function(o){{return sg.includes(o.sales_group)&&ac.includes(o.account_name);}});
  var sIDs=new Set(fO.map(function(o){{return o.sfdc_18_id;}}));
  var fI=INV.filter(function(i){{return sIDs.has(i.salesforce_id);}});
  if(yc==='AND'){{
    var bm=by.length?function(i){{return by.includes(yr(i.invoice_date));}}:function(){{return true;}};
    var pm=py.length?function(i){{return py.includes(yr(i.invoice_closed_date));}}:function(){{return true;}};
    fI=fI.filter(function(i){{return bm(i)&&pm(i);}});
  }}else if(yc==='OR'){{
    if(by.length||py.length) fI=fI.filter(function(i){{return (by.length&&by.includes(yr(i.invoice_date)))||(py.length&&py.includes(yr(i.invoice_closed_date)));}});
  }}else{{
    var bm2=by.length?function(i){{return by.includes(yr(i.invoice_date));}}:function(){{return true;}};
    fI=fI.filter(function(i){{return bm2(i)&&yr(i.invoice_date)===yr(i.invoice_closed_date);}});
  }}
  if(st.length) fI=fI.filter(function(i){{return st.includes(i.status);}});
  var fSIDs=new Set(fI.map(function(i){{return i.salesforce_id;}}));
  fO=fO.filter(function(o){{return fSIDs.has(o.sfdc_18_id);}});
  var fL=LI.filter(function(i){{return sIDs.has(i.salesforce_id)&&(by.length?by.includes(yr(i.invoice_date)):true);}});
  return {{fO:fO,fI:fI,fL:fL}};
}}

function updateKPIs(O,I){{
  var nre=O.reduce(function(s,r){{return s+r.nre_contract_value;}},0);
  var inv=I.reduce(function(s,r){{return s+r.amount;}},0);
  var paid=I.reduce(function(s,r){{return s+r.amount_paid;}},0);
  var out=I.reduce(function(s,r){{return s+r.adjusted_unpaid_amount;}},0);
  document.getElementById('k-deals').textContent   =O.length;
  document.getElementById('k-inv').textContent     =new Set(I.map(function(r){{return r.invoice_number;}})).size;
  document.getElementById('k-nre').textContent     =$m(nre);
  document.getElementById('k-invoiced').textContent=$m(inv);
  document.getElementById('k-paid').textContent    =$m(paid);
  document.getElementById('k-out').textContent     =$m(out);
  document.getElementById('k-rate').textContent    =(inv?paid/inv*100:0).toFixed(1)+'%';
}}

function cAcct(O,I){{
  var aMap={{}};O.forEach(function(o){{aMap[o.sfdc_18_id]=o.account_name;}});
  var rows=groupBy(I.map(function(i){{var r=Object.assign({{}},i);r._ac=aMap[i.salesforce_id]||'Unknown';return r;}}),'_ac',{{
    Invoiced:function(s,r){{return s+r.amount;}},Paid:function(s,r){{return s+r.amount_paid;}},
    Remaining:function(s,r){{return s+r.adjusted_unpaid_amount;}},Count:function(s,r){{return s+1;}}
  }});
  var f=document.getElementById('s-ac-f').value,asc=document.getElementById('s-ac-o').value==='Ascending';
  var d=srt(rows,f,asc).slice(0,15);
  Plotly.newPlot('c-acct',[
    {{name:'Invoiced', x:d.map(function(r){{return r._ac;}}),y:d.map(function(r){{return r.Invoiced;}}), type:'bar',marker:{{color:'#3949ab'}},hovertemplate:'%{{x}}<br>Invoiced: $%{{y:,.0f}}<extra></extra>'}},
    {{name:'Paid',     x:d.map(function(r){{return r._ac;}}),y:d.map(function(r){{return r.Paid;}}),     type:'bar',marker:{{color:'#00897b'}},hovertemplate:'%{{x}}<br>Paid: $%{{y:,.0f}}<extra></extra>'}},
    {{name:'Remaining',x:d.map(function(r){{return r._ac;}}),y:d.map(function(r){{return r.Remaining;}}),type:'bar',marker:{{color:'#e53935'}},hovertemplate:'%{{x}}<br>Remaining: $%{{y:,.0f}}<extra></extra>'}},
  ],lay({{barmode:'group',xaxis:{{tickangle:-35,tickfont:{{size:10}}}},yaxis:{{title:'Amount ($)',gridcolor:'#f0f0f0'}}}}),PLY);
}}

function cStatus(I){{
  var rows=groupBy(I,'status',{{amt:function(s,r){{return s+r.amount;}}}});
  Plotly.newPlot('c-status',[{{labels:rows.map(function(r){{return r.status;}}),values:rows.map(function(r){{return r.amt;}}),
    type:'pie',hole:.42,marker:{{colors:PAL}},textinfo:'percent+label',textposition:'outside',textfont:{{size:10}},
    hovertemplate:'%{{label}}<br>$%{{value:,.0f}}<extra></extra>'
  }}],lay({{showlegend:false,margin:{{t:20,b:20,l:20,r:20}}}}),PLY);
}}

function cGrp(O){{
  var rows=groupBy(O,'sales_group',{{NRE:function(s,r){{return s+r.nre_contract_value;}},Count:function(s,r){{return s+1;}}}});
  var f=document.getElementById('s-gr-f').value,asc=document.getElementById('s-gr-o').value==='Ascending';
  var d=srt(rows,f,asc);
  Plotly.newPlot('c-grp',[{{x:d.map(function(r){{return r.sales_group;}}),y:d.map(function(r){{return r.NRE;}}),
    text:d.map(function(r){{return r.Count+' deals';}}),textposition:'outside',textfont:{{size:10}},type:'bar',
    marker:{{color:d.map(function(_,i){{return PAL[i%PAL.length];}})}},
    hovertemplate:'%{{x}}<br>NRE: $%{{y:,.0f}}<extra></extra>'
  }}],lay({{showlegend:false,yaxis:{{title:'NRE ($)',gridcolor:'#f0f0f0'}}}}),PLY);
}}

function cTrend(I){{
  var m={{}};
  I.forEach(function(r){{if(!r.invoice_date)return;var k=r.invoice_date.slice(0,7);
    if(!m[k])m[k]={{I:0,P:0,R:0}};m[k].I+=r.amount;m[k].P+=r.amount_paid;m[k].R+=r.adjusted_unpaid_amount;}});
  var ks=Object.keys(m).sort();
  Plotly.newPlot('c-trend',[
    {{x:ks,y:ks.map(function(k){{return m[k].I;}}),mode:'lines+markers',name:'Invoiced', line:{{color:'#3949ab',width:2.5}},marker:{{size:5}}}},
    {{x:ks,y:ks.map(function(k){{return m[k].P;}}),mode:'lines+markers',name:'Paid',     line:{{color:'#00897b',width:2.5}},marker:{{size:5}}}},
    {{x:ks,y:ks.map(function(k){{return m[k].R;}}),mode:'lines+markers',name:'Remaining',line:{{color:'#e53935',width:2}},  marker:{{size:5}}}},
  ],lay({{xaxis:{{tickangle:-30,tickfont:{{size:10}}}},yaxis:{{title:'Amount ($)',gridcolor:'#f0f0f0'}}}}),PLY);
}}

function cHeat(I){{
  var met=document.querySelector('input[name="hm"]:checked').value;
  var map={{}};
  I.forEach(function(r){{var y=yr(r.invoice_date);if(!y)return;var s=r.status||'Unknown';
    if(!map[y])map[y]={{}};if(!map[y][s])map[y][s]={{Amount:0,Count:0}};
    map[y][s].Amount+=r.amount;map[y][s].Count++;
  }});
  var years=Object.keys(map).sort();
  var stats=Array.from(new Set(I.map(function(r){{return r.status;}}).filter(Boolean))).sort();
  var z=years.map(function(y){{return stats.map(function(s){{return map[y]&&map[y][s]?map[y][s][met]:0;}});}});
  Plotly.newPlot('c-heat',[{{z:z,x:stats,y:years,type:'heatmap',colorscale:'Blues',
    text:z,texttemplate:met==='Amount'?'$%{{text:.2s}}':'%{{text}}',
    hovertemplate:'Year: %{{y}}<br>Status: %{{x}}<br>'+met+': %{{z:,.0f}}<extra></extra>'
  }}],{{margin:{{t:10,b:50,l:60,r:10}},font:FONT,paper_bgcolor:'#fff',plot_bgcolor:'#fff'}},PLY);
}}

function cLI(L){{
  var rows=groupBy(L,'description',{{Amount:function(s,r){{return s+r.amount;}},Paid:function(s,r){{return s+r.amount_paid;}},
    Remaining:function(s,r){{return s+r.adjusted_unpaid_amount;}},Count:function(s,r){{return s+1;}}}});
  var f=document.getElementById('s-li-f').value,asc=document.getElementById('s-li-o').value==='Ascending';
  var d=srt(rows,f,asc).slice(-20);
  var lbls=d.map(function(r){{return r.description.length>50?r.description.slice(0,50)+'...':r.description;}});
  Plotly.newPlot('c-li',[
    {{name:'Amount',    y:lbls,x:d.map(function(r){{return r.Amount;}}),    type:'bar',orientation:'h',marker:{{color:'#3949ab'}}}},
    {{name:'Paid',      y:lbls,x:d.map(function(r){{return r.Paid;}}),      type:'bar',orientation:'h',marker:{{color:'#00897b'}}}},
    {{name:'Remaining', y:lbls,x:d.map(function(r){{return r.Remaining;}}), type:'bar',orientation:'h',marker:{{color:'#e53935'}}}},
  ],lay({{barmode:'group',xaxis:{{title:'Amount ($)',gridcolor:'#f0f0f0'}},yaxis:{{tickfont:{{size:9}}}},margin:{{t:10,b:10,l:300,r:10}}}}),PLY);
}}

function cAging(I){{
  var C={{'Current':'#43a047','1-30d':'#fdd835','31-60d':'#fb8c00','61-90d':'#e53935','90+d':'#b71c1c','No due date':'#90a4ae'}};
  var ORD=['Current','1-30d','31-60d','61-90d','90+d','No due date'];
  var bkt=function(s){{var d=dAgo(s);if(d===null)return'No due date';if(d<=0)return'Current';if(d<=30)return'1-30d';if(d<=60)return'31-60d';if(d<=90)return'61-90d';return'90+d';}};
  var m={{}};
  I.filter(function(r){{return r.adjusted_unpaid_amount>0;}}).forEach(function(r){{
    var b=bkt(r.invoice_due_date);if(!m[b])m[b]={{Outstanding:0,Count:0}};m[b].Outstanding+=r.adjusted_unpaid_amount;m[b].Count++;
  }});
  var f=document.getElementById('s-ag-f').value,asc=document.getElementById('s-ag-o').value==='Ascending';
  var rows=ORD.map(function(b){{return Object.assign({{b:b}},m[b]||{{Outstanding:0,Count:0}});}}).filter(function(r){{return r.Outstanding>0;}});
  rows.sort(function(a,z){{return asc?a[f]-z[f]:z[f]-a[f];}});
  Plotly.newPlot('c-aging',[{{x:rows.map(function(r){{return r.b;}}),y:rows.map(function(r){{return r.Outstanding;}}),
    text:rows.map(function(r){{return r.Count+' inv';}}),textposition:'outside',type:'bar',
    marker:{{color:rows.map(function(r){{return C[r.b];}})}}
  }}],lay({{showlegend:false,yaxis:{{title:'Outstanding ($)',gridcolor:'#f0f0f0'}}}}),PLY);
}}

function switchTab(id,btn){{
  document.querySelectorAll('.tab-pane').forEach(function(p){{p.classList.remove('active');}});
  document.querySelectorAll('.tab-btn').forEach(function(b){{b.classList.remove('active');}});
  document.getElementById(id).classList.add('active');btn.classList.add('active');
}}

function mkTbl(id,rows,cols){{
  if(!rows.length){{document.getElementById(id).innerHTML='<p style="color:#90a4ae;padding:12px;font-size:.8rem">No data.</p>';return;}}
  var h='<table><thead><tr>'+cols.map(function(c){{return '<th>'+c.l+'</th>';}}).join('')+'</tr></thead><tbody>';
  rows.slice(0,300).forEach(function(r){{
    h+='<tr>'+cols.map(function(c){{var v=r[c.k]!=null?r[c.k]:'';if(c.m&&v!=='')v=$m(+v||0);return'<td>'+v+'</td>';}}).join('')+'</tr>';
  }});
  document.getElementById(id).innerHTML=h+'</tbody></table>';
}}

function renderAll(){{
  var res=applyFilters(),fO=res.fO,fI=res.fI,fL=res.fL;
  updateKPIs(fO,fI); cAcct(fO,fI); cStatus(fI); cGrp(fO); cTrend(fI); cHeat(fI); cLI(fL); cAging(fI);
  mkTbl('tbl-opps',fO,[{{k:'opportunity_name',l:'Opportunity'}},{{k:'account_name',l:'Account'}},
    {{k:'global_client_name',l:'Global Client'}},{{k:'sales_group',l:'Sales Group'}},{{k:'product',l:'Product'}},
    {{k:'close_date',l:'Close Date'}},{{k:'nre_contract_value',l:'NRE Value',m:true}}]);
  mkTbl('tbl-inv',fI.map(function(r){{return Object.assign({{}},r,{{by:yr(r.invoice_date),py:yr(r.invoice_closed_date)}});}}),[
    {{k:'invoice_date',l:'Invoice Date'}},{{k:'invoice_number',l:'Invoice #'}},{{k:'name',l:'Customer'}},
    {{k:'status',l:'Status'}},{{k:'by',l:'Billed Yr'}},{{k:'py',l:'Paid Yr'}},
    {{k:'amount',l:'Amount',m:true}},{{k:'amount_paid',l:'Paid',m:true}},{{k:'adjusted_unpaid_amount',l:'Outstanding',m:true}},
    {{k:'invoice_due_date',l:'Due Date'}}]);
  mkTbl('tbl-li',fL,[{{k:'invoice_date',l:'Invoice Date'}},{{k:'invoice_number',l:'Invoice #'}},
    {{k:'name',l:'Customer'}},{{k:'item',l:'Item'}},{{k:'description',l:'Description'}},
    {{k:'bitgo_lineitem',l:'BitGo Line Item'}},{{k:'amount',l:'Amount',m:true}},
    {{k:'amount_paid',l:'Paid',m:true}},{{k:'adjusted_unpaid_amount',l:'Outstanding',m:true}},{{k:'status',l:'Status'}}]);
}}

var T0=Date.now(),RM=15*60*1000;
setInterval(function(){{
  var left=RM-((Date.now()-T0)%RM),m=Math.floor(left/60000),s=Math.floor((left%60000)/1000);
  document.getElementById('countdown').textContent='Next refresh: '+m+'m '+(s<10?'0':'')+s+'s';
}},1000);
setTimeout(function(){{location.reload();}},RM);

renderAll();
</script>
</body>
</html>"""

# Write and open
print(f"\nWriting {OUT_HTML.name} ...")
OUT_HTML.write_text(html, encoding="utf-8")
size_mb = OUT_HTML.stat().st_size / 1024 / 1024
print(f"Done: {OUT_HTML.name}  ({size_mb:.1f} MB)")
print("Opening in browser...")
webbrowser.open(OUT_HTML.as_uri())
