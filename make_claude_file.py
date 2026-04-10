"""
Generates nre_dashboard_claude.py — a single self-contained Python file
with all dashboard data embedded as JSON.

How to use:
  1. Run this script:  python make_claude_file.py
  2. Share the output  nre_dashboard_claude.py  with Claude.ai
  3. Tell Claude:      "Run this file and show me the dashboard"
  4. Claude executes it and displays all charts as artifacts — no Snowflake,
     no GitHub, no installs needed.

To refresh data before generating, run export_data.py first.
"""
import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
OUT_FILE = Path(__file__).parent / "nre_dashboard_claude.py"


# ── Load parquet files ────────────────────────────────────────────────────────
def load_parquet(name):
    df = pd.read_parquet(DATA_DIR / f"{name}_cache.parquet")
    df.columns = df.columns.str.lower()
    # Convert all date/timestamp columns to strings so JSON serialises them
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]",
                                          "datetimetz", "object"]).columns:
        df[col] = df[col].astype(str).replace("NaT", "")
    return df

print("Loading parquet files…")
df_opps = load_parquet("opps")
df_inv  = load_parquet("invoices")
df_li   = load_parquet("lineitems")

generated_at = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

opps_json = df_opps.to_json(orient="records")
inv_json  = df_inv.to_json(orient="records")
li_json   = df_li.to_json(orient="records")

print(f"  Opportunities : {len(df_opps):,} rows")
print(f"  Invoice Headers: {len(df_inv):,} rows")
print(f"  Line Items    : {len(df_li):,} rows")

# ── Write the self-contained dashboard script ─────────────────────────────────
code = f'''"""
NRE Sales & Invoice Dashboard — Claude Artifact
================================================
Generated : {generated_at}
Data      : {len(df_opps)} opportunities | {len(df_inv)} invoice headers | {len(df_li)} line items

HOW TO USE
----------
Share this file with Claude.ai and say:
  "Run this file and show me the NRE dashboard charts."

Claude will execute the code and display all charts as interactive artifacts.
No Snowflake, no GitHub, no pip installs required.
"""

import json
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ── Embedded data (Snowflake snapshot as of {generated_at}) ──────────────────
_OPPS_JSON = {repr(opps_json)}
_INV_JSON  = {repr(inv_json)}
_LI_JSON   = {repr(li_json)}

df_opps = pd.DataFrame(json.loads(_OPPS_JSON))
df_inv  = pd.DataFrame(json.loads(_INV_JSON))
df_li   = pd.DataFrame(json.loads(_LI_JSON))

# Parse dates
for col in ["invoice_date", "invoice_due_date", "invoice_closed_date"]:
    df_inv[col] = pd.to_datetime(df_inv[col], errors="coerce")
    df_li[col]  = pd.to_datetime(df_li[col],  errors="coerce")

for col in ["nre_contract_value", "amount", "amount_paid",
            "amount_remaining", "adjusted_unpaid_amount"]:
    if col in df_inv.columns:
        df_inv[col] = pd.to_numeric(df_inv[col], errors="coerce").fillna(0)
    if col in df_li.columns:
        df_li[col]  = pd.to_numeric(df_li[col],  errors="coerce").fillna(0)
if "nre_contract_value" in df_opps.columns:
    df_opps["nre_contract_value"] = pd.to_numeric(df_opps["nre_contract_value"], errors="coerce").fillna(0)

# ── KPIs ─────────────────────────────────────────────────────────────────────
total_nre       = df_opps["nre_contract_value"].sum()
total_invoiced  = df_inv["amount"].sum()
total_paid      = df_inv["amount_paid"].sum()
total_remaining = df_inv["adjusted_unpaid_amount"].sum()
collection_rate = (total_paid / total_invoiced * 100) if total_invoiced else 0
num_deals       = len(df_opps)
num_invoices    = df_inv["invoice_number"].nunique()

print("=" * 60)
print("  NRE SALES & INVOICE DASHBOARD")
print(f"  Data as of : {generated_at}")
print("=" * 60)
print(f"  Deals            : {{num_deals}}")
print(f"  Invoices         : {{num_invoices}}")
print(f"  NRE Contract Val : ${{total_nre:,.0f}}")
print(f"  Total Invoiced   : ${{total_invoiced:,.0f}}")
print(f"  Total Paid       : ${{total_paid:,.0f}}")
print(f"  Outstanding      : ${{total_remaining:,.0f}}")
print(f"  Collection Rate  : {{collection_rate:.1f}}%")
print("=" * 60)

# ── Chart 1: Invoiced vs Paid vs Remaining by Account ────────────────────────
inv_acc = df_inv.merge(
    df_opps[["sfdc_18_id", "account_name"]].drop_duplicates(),
    left_on="salesforce_id", right_on="sfdc_18_id", how="left"
)
by_acct = (
    inv_acc.groupby("account_name", as_index=False)
    .agg(Invoiced=("amount","sum"), Paid=("amount_paid","sum"),
         Remaining=("adjusted_unpaid_amount","sum"))
    .sort_values("Invoiced", ascending=False).head(15)
)

fig1 = go.Figure([
    go.Bar(name="Invoiced",  x=by_acct["account_name"], y=by_acct["Invoiced"],  marker_color="#4C78A8"),
    go.Bar(name="Paid",      x=by_acct["account_name"], y=by_acct["Paid"],      marker_color="#54A24B"),
    go.Bar(name="Remaining", x=by_acct["account_name"], y=by_acct["Remaining"], marker_color="#E45756"),
])
fig1.update_layout(
    title="Invoiced vs Paid vs Remaining by Account (Top 15)",
    barmode="group", height=420,
    xaxis_tickangle=-40, yaxis_title="Amount ($)",
    legend=dict(orientation="h", y=1.1),
)
fig1.show()

# ── Chart 2: Invoice Status Distribution ─────────────────────────────────────
status_grp = df_inv.groupby("status", as_index=False).agg(amount=("amount","sum"))
fig2 = px.pie(
    status_grp, names="status", values="amount",
    hole=0.45, color_discrete_sequence=px.colors.qualitative.Set2,
    title="Invoice Status Distribution (by Amount)",
)
fig2.update_traces(textposition="outside", textinfo="percent+label")
fig2.update_layout(height=380)
fig2.show()

# ── Chart 3: NRE Contract Value by Sales Group ───────────────────────────────
by_grp = (
    df_opps.groupby("sales_group", as_index=False)
    .agg(NRE=("nre_contract_value","sum"), Deals=("opportunity_id","count"))
    .sort_values("NRE", ascending=False)
)
fig3 = px.bar(
    by_grp, x="sales_group", y="NRE",
    text=by_grp["Deals"].astype(str) + " deals",
    color="sales_group", color_discrete_sequence=px.colors.qualitative.Pastel,
    title="NRE Contract Value by Sales Group",
    labels={{"NRE": "NRE Contract Value ($)", "sales_group": "Sales Group"}},
)
fig3.update_traces(textposition="outside")
fig3.update_layout(height=360, showlegend=False)
fig3.show()

# ── Chart 4: Monthly Invoice Trend ───────────────────────────────────────────
inv_trend = df_inv.dropna(subset=["invoice_date"]).copy()
inv_trend["month"] = inv_trend["invoice_date"].dt.to_period("M").astype(str)
monthly = (
    inv_trend.groupby("month", as_index=False)
    .agg(Invoiced=("amount","sum"), Paid=("amount_paid","sum"),
         Remaining=("adjusted_unpaid_amount","sum"))
    .sort_values("month")
)
fig4 = go.Figure([
    go.Scatter(x=monthly["month"], y=monthly["Invoiced"],  mode="lines+markers",
               name="Invoiced",  line=dict(color="#4C78A8", width=2)),
    go.Scatter(x=monthly["month"], y=monthly["Paid"],      mode="lines+markers",
               name="Paid",      line=dict(color="#54A24B", width=2)),
    go.Scatter(x=monthly["month"], y=monthly["Remaining"], mode="lines+markers",
               name="Remaining", line=dict(color="#E45756", width=2)),
])
fig4.update_layout(
    title="Monthly Invoice Trend",
    height=360, xaxis_tickangle=-40, yaxis_title="Amount ($)",
    legend=dict(orientation="h", y=1.1),
)
fig4.show()

# ── Chart 5: Billed Year x Status Heatmap ────────────────────────────────────
pivot_df = df_inv.copy()
pivot_df["billed_year"] = pivot_df["invoice_date"].dt.year.astype("Int64").astype(str)
pivot = (
    pivot_df.groupby(["billed_year", "status"], as_index=False)
    .agg(Amount=("amount","sum"))
)
pivot_wide = pivot.pivot(index="billed_year", columns="status", values="Amount").fillna(0)
fig5 = px.imshow(
    pivot_wide, text_auto=".2s", aspect="auto",
    color_continuous_scale="Blues",
    title="Billed Year × Status Heatmap (Invoice Amount)",
    labels=dict(x="Status", y="Billed Year", color="Amount ($)"),
)
fig5.update_layout(height=280)
fig5.show()

# ── Chart 6: Top Line Item Descriptions ──────────────────────────────────────
li_desc = (
    df_li.groupby("description", as_index=False)
    .agg(Amount=("amount","sum"), Paid=("amount_paid","sum"),
         Remaining=("adjusted_unpaid_amount","sum"))
    .sort_values("Amount", ascending=True).tail(20)
)
fig6 = go.Figure([
    go.Bar(name="Amount",    y=li_desc["description"], x=li_desc["Amount"],
           orientation="h", marker_color="#4C78A8"),
    go.Bar(name="Paid",      y=li_desc["description"], x=li_desc["Paid"],
           orientation="h", marker_color="#54A24B"),
    go.Bar(name="Remaining", y=li_desc["description"], x=li_desc["Remaining"],
           orientation="h", marker_color="#E45756"),
])
fig6.update_layout(
    title="Top 20 Line Item Descriptions",
    barmode="group", height=560,
    xaxis_title="Amount ($)",
    legend=dict(orientation="h", y=1.02),
    margin=dict(l=320),
)
fig6.show()

# ── Chart 7: Invoice Aging ────────────────────────────────────────────────────
today = pd.Timestamp.today().normalize()
aging = df_inv[df_inv["adjusted_unpaid_amount"] > 0].copy()
aging["days_overdue"] = (today - aging["invoice_due_date"]).dt.days

BUCKET_ORDER = ["Current", "1-30 days", "31-60 days", "61-90 days", "90+ days", "No due date"]
def bucket(d):
    if pd.isna(d):  return "No due date"
    if d <= 0:      return "Current"
    if d <= 30:     return "1-30 days"
    if d <= 60:     return "31-60 days"
    if d <= 90:     return "61-90 days"
    return "90+ days"

aging["bucket"] = aging["days_overdue"].apply(bucket)
aging_grp = (
    aging.groupby("bucket", as_index=False)
    .agg(Outstanding=("adjusted_unpaid_amount","sum"), Count=("invoice_number","nunique"))
)
aging_grp["bucket"] = pd.Categorical(aging_grp["bucket"], categories=BUCKET_ORDER, ordered=True)
aging_grp = aging_grp.sort_values("bucket")

AGING_COLORS = {{
    "Current":      "#54A24B",
    "1-30 days":    "#F5C518",
    "31-60 days":   "#FFA500",
    "61-90 days":   "#E45756",
    "90+ days":     "#8B0000",
    "No due date":  "#AAAAAA",
}}
fig7 = px.bar(
    aging_grp, x="bucket", y="Outstanding",
    text=aging_grp["Count"].astype(str) + " inv",
    color="bucket", color_discrete_map=AGING_COLORS,
    title="Invoice Aging — Outstanding Amounts",
    labels={{"bucket": "Aging Bucket", "Outstanding": "Outstanding ($)"}},
)
fig7.update_traces(textposition="outside")
fig7.update_layout(height=360, showlegend=False)
fig7.show()

print("\\nAll charts displayed.")
print(f"Data snapshot: {generated_at}")
'''

OUT_FILE.write_text(code, encoding="utf-8")
size_kb = OUT_FILE.stat().st_size / 1024
print(f"\nGenerated: {OUT_FILE.name}  ({size_kb:.1f} KB)")
print("\nShare nre_dashboard_claude.py with Claude.ai and say:")
print('  "Run this file and show me the NRE dashboard charts."')
