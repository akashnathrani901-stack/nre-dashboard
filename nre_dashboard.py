import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NRE Sales & Invoice Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    div[data-testid="stMetricValue"] { font-size: 1.4rem; }
    div[data-testid="stMetricLabel"] { font-size: 0.85rem; color: #555; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
CACHE = {
    "opps":      DATA_DIR / "opps_cache.parquet",
    "invoices":  DATA_DIR / "invoices_cache.parquet",
    "lineitems": DATA_DIR / "lineitems_cache.parquet",
}

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
OPPS_SQL = """
SELECT
    o.opportunity_id, o.opportunity_name, o.SFDC_18_ID,
    o.ACCOUNT_NAME, o.GLOBAL_CLIENT_NAME, o.PRODUCT,
    o.OPPORTUNITY_DESCRIPTION AS description,
    CAST(o.CONTRACT_START_DATE AS VARCHAR) AS contract_start_date,
    CAST(o.CLOSE_DATE AS VARCHAR) AS close_date,
    o.NRE_CONTRACT_VALUE, a.SALES_GROUP
FROM analytics.looker.sf_opportunity_view_public o
LEFT JOIN analytics.looker.sf_account_view_public a ON o.SFDC_18_ID = a.SFDC_18_ID
WHERE o.NRE_CONTRACT_VALUE > 0
  AND o.STAGE = 'Closed Won'
  AND o.CLOSE_DATE >= '2025-01-01'
ORDER BY CASE WHEN a.sales_group ILIKE 'ecosystem' THEN 0 ELSE 1 END, o.CLOSE_DATE DESC
"""

INVOICES_SQL = """
SELECT
    invoice_date, invoice_number, netsuite_id, name, line_id, item,
    description, description_v2, bitgo_lineitem,
    amount, amount_paid, amount_remaining, adjusted_unpaid_amount,
    invoice_due_date, invoice_closed_date, status, salesforce_id
FROM analytics.looker.netsuite_invoice_items
WHERE salesforce_id IN (
    SELECT DISTINCT o.SFDC_18_ID
    FROM analytics.looker.SF_OPPORTUNITY_VIEW_PUBLIC o
    WHERE o.NRE_CONTRACT_VALUE > 0 AND o.STAGE = 'Closed Won' AND o.CLOSE_DATE >= '2025-01-01'
) AND line_id = 0
ORDER BY salesforce_id, invoice_date DESC
"""

LINE_ITEMS_SQL = """
SELECT
    invoice_date, invoice_number, netsuite_id, name, line_id, item,
    description, description_v2, bitgo_lineitem,
    amount, amount_paid, amount_remaining, adjusted_unpaid_amount,
    invoice_due_date, invoice_closed_date, status, salesforce_id
FROM analytics.looker.netsuite_invoice_items
WHERE salesforce_id IN (
    SELECT DISTINCT o.SFDC_18_ID
    FROM analytics.looker.sf_opportunity_view_public o
    WHERE o.NRE_CONTRACT_VALUE > 0 AND o.STAGE = 'Closed Won' AND o.CLOSE_DATE >= '2025-01-01'
) AND line_id <> 0
ORDER BY salesforce_id, invoice_date DESC
"""

# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------
def has_snowflake_secrets():
    try:
        _ = st.secrets["snowflake"]
        return True
    except Exception:
        return False


@st.cache_resource
def get_connection():
    import snowflake.connector
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=cfg["user"],
        account=cfg["account"],
        authenticator=cfg.get("authenticator", "snowflake"),
        warehouse=cfg["warehouse"],
        database=cfg.get("database", "MARTS"),
        schema=cfg.get("schema", "PUBLIC"),
        client_store_temporary_credential=True,
    )


def fetch_from_snowflake():
    conn = get_connection()
    df_opps = pd.read_sql(OPPS_SQL, conn)
    df_inv  = pd.read_sql(INVOICES_SQL, conn)
    df_li   = pd.read_sql(LINE_ITEMS_SQL, conn)
    for df in [df_opps, df_inv, df_li]:
        df.columns = df.columns.str.lower()
    return df_opps, df_inv, df_li


def save_cache(df_opps, df_inv, df_li):
    DATA_DIR.mkdir(exist_ok=True)
    df_opps.to_parquet(CACHE["opps"],      index=False)
    df_inv.to_parquet(CACHE["invoices"],   index=False)
    df_li.to_parquet(CACHE["lineitems"],   index=False)


def load_cache():
    return (
        pd.read_parquet(CACHE["opps"]),
        pd.read_parquet(CACHE["invoices"]),
        pd.read_parquet(CACHE["lineitems"]),
    )


def cache_exists():
    return all(f.exists() for f in CACHE.values())


def cache_age():
    ts = min(f.stat().st_mtime for f in CACHE.values() if f.exists())
    return pd.Timestamp.fromtimestamp(ts)

# ---------------------------------------------------------------------------
# Load data  (Live Snowflake  OR  cached parquet)
# ---------------------------------------------------------------------------
st.title("NRE Sales & Invoice Dashboard")
st.caption("Closed Won opportunities with NRE > 0 | Since Jan 2025")

can_live = has_snowflake_secrets()
has_cache = cache_exists()

# Data-source selector in sidebar
st.sidebar.header("Data Source")

if can_live and has_cache:
    data_mode = st.sidebar.radio(
        "Load data from",
        ["Cached (fast)", "Live Snowflake"],
        index=0,
    )
    use_live = data_mode == "Live Snowflake"
elif can_live:
    st.sidebar.info("No cache found — loading from Snowflake.")
    use_live = True
elif has_cache:
    age = cache_age()
    st.sidebar.success(f"Using cached data (as of {age:%Y-%m-%d %H:%M})")
    use_live = False
else:
    st.error("No Snowflake credentials and no cached data found. "
             "Add `.streamlit/secrets.toml` or place parquet files in `data/`.")
    st.stop()

if use_live:
    with st.spinner("Fetching live data from Snowflake…"):
        df_opps_raw, df_inv_raw, df_li_raw = fetch_from_snowflake()
    # Offer to save cache
    if st.sidebar.button("Save to cache (share with team)"):
        save_cache(df_opps_raw, df_inv_raw, df_li_raw)
        st.sidebar.success("Cache saved to `data/` folder.")
else:
    if has_cache:
        age = cache_age()
        st.sidebar.caption(f"Cache last updated: {age:%Y-%m-%d %H:%M}")
    df_opps_raw, df_inv_raw, df_li_raw = load_cache()

# Parse dates
for df in [df_inv_raw, df_li_raw]:
    df["invoice_date"]        = pd.to_datetime(df["invoice_date"],        errors="coerce")
    df["invoice_due_date"]    = pd.to_datetime(df["invoice_due_date"],    errors="coerce")
    df["invoice_closed_date"] = pd.to_datetime(df["invoice_closed_date"], errors="coerce")

# ---------------------------------------------------------------------------
# Sidebar — Filters
# ---------------------------------------------------------------------------
st.sidebar.header("Filters")

# --- Sales Group ---
all_groups = sorted(df_opps_raw["sales_group"].dropna().unique().tolist())
sel_groups = st.sidebar.multiselect("Sales Group", all_groups, default=all_groups)

# --- Account ---
all_accounts = sorted(df_opps_raw["account_name"].dropna().unique().tolist())
sel_accounts = st.sidebar.multiselect("Account", all_accounts, default=all_accounts)

st.sidebar.markdown("---")
st.sidebar.subheader("Invoice Year Filters")

# --- Invoice Billed Year ---
billed_years = sorted(
    df_inv_raw["invoice_date"].dropna().dt.year.unique().tolist(), reverse=True
)
sel_billed_years = st.sidebar.multiselect(
    "Invoice Billed Year", billed_years, default=billed_years
)

# --- Condition between the two year filters ---
st.sidebar.markdown(
    "<div style='text-align:center; color:#888; font-size:0.78rem; margin:4px 0 2px 0;'>"
    "── Year Filter Condition ──</div>",
    unsafe_allow_html=True,
)
year_condition = st.sidebar.radio(
    "",
    ["AND", "OR", "Same Year (Billed = Paid)"],
    index=0,
    horizontal=True,
    key="year_condition",
    help=(
        "AND — invoice must match both billed year AND paid year selections.\n\n"
        "OR  — invoice matches billed year OR paid year (either one).\n\n"
        "Same Year — billed year equals paid year "
        "(e.g. billed 2025 and also closed/paid in 2025)."
    ),
)

# Description under the radio so users know what each mode does
_cond_desc = {
    "AND":                    "Both year filters must match",
    "OR":                     "Either year filter can match",
    "Same Year (Billed = Paid)": "Billed year = Paid year (ignores Paid Year picker)",
}
st.sidebar.caption(_cond_desc[year_condition])

# --- Invoice Paid / Closed Year ---
paid_year_series = df_inv_raw.loc[
    df_inv_raw["invoice_closed_date"].notna(), "invoice_closed_date"
].dt.year
paid_years = sorted(paid_year_series.unique().tolist(), reverse=True)

# Disable the paid-year picker when "Same Year" is chosen (it's implicit)
_paid_disabled = year_condition == "Same Year (Billed = Paid)"
sel_paid_years = st.sidebar.multiselect(
    "Invoice Paid / Closed Year",
    paid_years,
    default=[],
    disabled=_paid_disabled,
    help="Disabled when 'Same Year' is selected — paid year is derived from billed year."
    if _paid_disabled else "",
)

st.sidebar.markdown("---")
st.sidebar.subheader("Status Filter")

# --- Status ---
all_statuses = sorted(df_inv_raw["status"].dropna().unique().tolist())
sel_statuses = st.sidebar.multiselect("Invoice Status", all_statuses, default=all_statuses)

# Cross-filter hint
st.sidebar.caption(
    "Tip: combine Billed Year + Status to find e.g. "
    "invoices billed in 2025 that are still Open."
)

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
opps_f = df_opps_raw[
    df_opps_raw["sales_group"].isin(sel_groups) &
    df_opps_raw["account_name"].isin(sel_accounts)
]
sfdc_ids = opps_f["sfdc_18_id"].unique()

inv_f = df_inv_raw[df_inv_raw["salesforce_id"].isin(sfdc_ids)].copy()

# --- Year filters with AND / OR / Same-Year logic ---
billed_mask = (
    inv_f["invoice_date"].dt.year.isin(sel_billed_years)
    if sel_billed_years
    else pd.Series(True, index=inv_f.index)
)
paid_mask = (
    inv_f["invoice_closed_date"].dt.year.isin(sel_paid_years)
    if sel_paid_years
    else pd.Series(True, index=inv_f.index)
)

if year_condition == "AND":
    inv_f = inv_f[billed_mask & paid_mask]

elif year_condition == "OR":
    # When nothing is selected on a side, treat it as "match all" for that side
    billed_or = (
        inv_f["invoice_date"].dt.year.isin(sel_billed_years)
        if sel_billed_years
        else pd.Series(False, index=inv_f.index)
    )
    paid_or = (
        inv_f["invoice_closed_date"].dt.year.isin(sel_paid_years)
        if sel_paid_years
        else pd.Series(False, index=inv_f.index)
    )
    # If neither side has selections, show everything
    if sel_billed_years or sel_paid_years:
        inv_f = inv_f[billed_or | paid_or]

else:  # Same Year (Billed = Paid)
    same_year_mask = (
        inv_f["invoice_date"].dt.year == inv_f["invoice_closed_date"].dt.year
    )
    inv_f = inv_f[billed_mask & same_year_mask]

# Status filter
if sel_statuses:
    inv_f = inv_f[inv_f["status"].isin(sel_statuses)]

li_f = df_li_raw[df_li_raw["salesforce_id"].isin(sfdc_ids)].copy()
if sel_billed_years:
    li_f = li_f[li_f["invoice_date"].dt.year.isin(sel_billed_years)]

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------
total_nre       = opps_f["nre_contract_value"].sum()
total_invoiced  = inv_f["amount"].sum()
total_paid      = inv_f["amount_paid"].sum()
total_remaining = inv_f["adjusted_unpaid_amount"].sum()
collection_rate = (total_paid / total_invoiced * 100) if total_invoiced else 0
num_deals       = len(opps_f)
num_invoices    = inv_f["invoice_number"].nunique()

k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
k1.metric("Deals",               f"{num_deals}")
k2.metric("Invoices",            f"{num_invoices}")
k3.metric("NRE Contract Value",  f"${total_nre:,.0f}")
k4.metric("Total Invoiced",      f"${total_invoiced:,.0f}")
k5.metric("Total Paid",          f"${total_paid:,.0f}")
k6.metric("Outstanding",         f"${total_remaining:,.0f}")
k7.metric("Collection Rate",     f"{collection_rate:.1f}%")

st.divider()

# ---------------------------------------------------------------------------
# Helper — sort controls (inline, above each chart)
# ---------------------------------------------------------------------------
SORT_FIELDS = {
    "Invoiced (Amount)":  "Invoiced",
    "Paid":               "Paid",
    "Remaining":          "Remaining",
    "Count":              "Count",
}

def sort_controls(key: str):
    """Returns (sort_col: str, ascending: bool)"""
    c1, c2 = st.columns([2, 1])
    sort_label = c1.selectbox(
        "Sort by", list(SORT_FIELDS.keys()),
        key=f"sort_field_{key}"
    )
    order = c2.selectbox(
        "Order", ["Descending", "Ascending"],
        key=f"sort_order_{key}"
    )
    return SORT_FIELDS[sort_label], order == "Ascending"


def build_account_agg(inv_df, opps_df):
    merged = inv_df.merge(
        opps_df[["sfdc_18_id", "account_name"]].drop_duplicates(),
        left_on="salesforce_id", right_on="sfdc_18_id", how="left"
    )
    return (
        merged.groupby("account_name", as_index=False)
        .agg(
            Invoiced=("amount",                "sum"),
            Paid=("amount_paid",               "sum"),
            Remaining=("adjusted_unpaid_amount","sum"),
            Count=("invoice_number",            "nunique"),
        )
    )

# ---------------------------------------------------------------------------
# Row 2 — Invoiced / Paid / Remaining by Account  +  Status Donut
# ---------------------------------------------------------------------------
c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("Invoiced vs Paid vs Remaining by Account")
    sort_col, asc = sort_controls("acct")
    by_acct = build_account_agg(inv_f, opps_f).sort_values(sort_col, ascending=asc).head(15)

    fig_acct = go.Figure([
        go.Bar(name="Invoiced",  x=by_acct["account_name"], y=by_acct["Invoiced"],  marker_color="#4C78A8"),
        go.Bar(name="Paid",      x=by_acct["account_name"], y=by_acct["Paid"],      marker_color="#54A24B"),
        go.Bar(name="Remaining", x=by_acct["account_name"], y=by_acct["Remaining"], marker_color="#E45756"),
    ])
    fig_acct.update_layout(
        barmode="group", height=380,
        xaxis_tickangle=-40, yaxis_title="Amount ($)",
        legend=dict(orientation="h", y=1.08), margin=dict(t=30, b=10),
    )
    st.plotly_chart(fig_acct, use_container_width=True)

with c2:
    st.subheader("Invoice Status (by Amount)")
    status_grp = inv_f.groupby("status", as_index=False).agg(amount=("amount", "sum"))
    fig_donut = px.pie(
        status_grp, names="status", values="amount",
        hole=0.45, color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_donut.update_layout(height=420, margin=dict(t=30, b=10))
    fig_donut.update_traces(textposition="outside", textinfo="percent+label")
    st.plotly_chart(fig_donut, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 3 — NRE by Sales Group  +  Monthly Trend
# ---------------------------------------------------------------------------
c3, c4 = st.columns(2)

with c3:
    st.subheader("NRE Contract Value by Sales Group")
    sort_col_grp, asc_grp = sort_controls("grp")
    by_grp = (
        opps_f.groupby("sales_group", as_index=False)
        .agg(
            Invoiced=("nre_contract_value", "sum"),
            Count=("opportunity_id", "count"),
        )
        .rename(columns={"Invoiced": "NRE"})
    )
    # align sort col name (NRE maps to Invoiced key)
    sort_col_grp = "NRE" if sort_col_grp == "Invoiced" else sort_col_grp
    if sort_col_grp not in by_grp.columns:
        sort_col_grp = "NRE"
    by_grp = by_grp.sort_values(sort_col_grp, ascending=asc_grp)

    fig_grp = px.bar(
        by_grp, x="sales_group", y="NRE",
        text=by_grp["Count"].astype(str) + " deals",
        color="sales_group",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        labels={"NRE": "NRE Contract Value ($)", "sales_group": "Sales Group"},
    )
    fig_grp.update_traces(textposition="outside")
    fig_grp.update_layout(height=360, showlegend=False, margin=dict(t=30, b=10))
    st.plotly_chart(fig_grp, use_container_width=True)

with c4:
    st.subheader("Monthly Invoice Trend")
    inv_trend = inv_f.dropna(subset=["invoice_date"]).copy()
    inv_trend["month"] = inv_trend["invoice_date"].dt.to_period("M").astype(str)
    monthly = (
        inv_trend.groupby("month", as_index=False)
        .agg(Invoiced=("amount", "sum"), Paid=("amount_paid", "sum"),
             Remaining=("adjusted_unpaid_amount", "sum"), Count=("invoice_number", "nunique"))
        .sort_values("month")
    )

    # Sort control for trend — which metric to highlight
    trend_metric = st.selectbox(
        "Highlight metric", ["Invoiced", "Paid", "Remaining", "Count"],
        key="trend_metric"
    )
    fig_trend = go.Figure([
        go.Scatter(x=monthly["month"], y=monthly["Invoiced"],  mode="lines+markers",
                   name="Invoiced",  line=dict(color="#4C78A8", width=2 if trend_metric=="Invoiced" else 1)),
        go.Scatter(x=monthly["month"], y=monthly["Paid"],      mode="lines+markers",
                   name="Paid",      line=dict(color="#54A24B", width=2 if trend_metric=="Paid" else 1)),
        go.Scatter(x=monthly["month"], y=monthly["Remaining"], mode="lines+markers",
                   name="Remaining", line=dict(color="#E45756", width=2 if trend_metric=="Remaining" else 1)),
    ])
    fig_trend.update_layout(
        height=340, xaxis_tickangle=-40, yaxis_title="Amount ($)",
        legend=dict(orientation="h", y=1.08), margin=dict(t=30, b=10),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 4 — Billed-Year × Status heatmap  (cross-filter visual)
# ---------------------------------------------------------------------------
st.subheader("Billed Year × Status — Invoice Amount Heatmap")
st.caption("Shows how much was billed in each year per status — useful for spotting open invoices from prior years.")

pivot_df = inv_f.copy()
pivot_df["billed_year"] = pivot_df["invoice_date"].dt.year.astype("Int64").astype(str)
pivot = (
    pivot_df.groupby(["billed_year", "status"], as_index=False)
    .agg(Amount=("amount", "sum"), Count=("invoice_number", "nunique"))
)
heat_metric = st.radio(
    "Heatmap value", ["Amount", "Count"], horizontal=True, key="heat_metric"
)
if not pivot.empty:
    pivot_wide = pivot.pivot(index="billed_year", columns="status", values=heat_metric).fillna(0)
    fig_heat = px.imshow(
        pivot_wide,
        text_auto=".2s" if heat_metric == "Amount" else True,
        aspect="auto",
        color_continuous_scale="Blues",
        labels=dict(x="Status", y="Billed Year", color=heat_metric),
    )
    fig_heat.update_layout(height=250, margin=dict(t=10, b=10))
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.info("No data for selected filters.")

st.divider()

# ---------------------------------------------------------------------------
# Row 5 — Line Item breakdown by description
# ---------------------------------------------------------------------------
st.subheader("Line Item Breakdown by Description")

sort_col_li, asc_li = sort_controls("li")
li_desc = (
    li_f.groupby("description", as_index=False)
    .agg(
        Invoiced=("amount",                "sum"),
        Paid=("amount_paid",               "sum"),
        Remaining=("adjusted_unpaid_amount","sum"),
        Count=("invoice_number",            "nunique"),
    )
    .sort_values(sort_col_li, ascending=asc_li)
    .tail(20)
)

fig_li = go.Figure([
    go.Bar(name="Invoiced",  y=li_desc["description"], x=li_desc["Invoiced"],
           orientation="h", marker_color="#4C78A8"),
    go.Bar(name="Paid",      y=li_desc["description"], x=li_desc["Paid"],
           orientation="h", marker_color="#54A24B"),
    go.Bar(name="Remaining", y=li_desc["description"], x=li_desc["Remaining"],
           orientation="h", marker_color="#E45756"),
])
fig_li.update_layout(
    barmode="group", height=520,
    xaxis_title="Amount ($)",
    legend=dict(orientation="h", y=1.02),
    margin=dict(t=10, b=10, l=300),
)
st.plotly_chart(fig_li, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 6 — Invoice Aging (outstanding only)
# ---------------------------------------------------------------------------
st.subheader("Invoice Aging (Outstanding Only)")

today  = pd.Timestamp.today().normalize()
aging  = inv_f[inv_f["adjusted_unpaid_amount"] > 0].copy()
aging["days_overdue"] = (today - aging["invoice_due_date"]).dt.days

BUCKET_ORDER = ["Current", "1–30 days", "31–60 days", "61–90 days", "90+ days", "No due date"]

def bucket(d):
    if pd.isna(d):  return "No due date"
    if d <= 0:      return "Current"
    if d <= 30:     return "1–30 days"
    if d <= 60:     return "31–60 days"
    if d <= 90:     return "61–90 days"
    return "90+ days"

aging["bucket"] = aging["days_overdue"].apply(bucket)
sort_col_ag, asc_ag = sort_controls("aging")

aging_grp = (
    aging.groupby("bucket", as_index=False)
    .agg(
        Remaining=("adjusted_unpaid_amount","sum"),
        Invoiced=("amount",                 "sum"),
        Count=("invoice_number",            "nunique"),
        Paid=("amount_paid",               "sum"),
    )
)
aging_grp["bucket"] = pd.Categorical(aging_grp["bucket"], categories=BUCKET_ORDER, ordered=True)
aging_grp = aging_grp.sort_values(sort_col_ag, ascending=asc_ag)

AGING_COLORS = {
    "Current":     "#54A24B",
    "1–30 days":   "#F5C518",
    "31–60 days":  "#FFA500",
    "61–90 days":  "#E45756",
    "90+ days":    "#8B0000",
    "No due date": "#AAAAAA",
}

fig_aging = px.bar(
    aging_grp, x="bucket", y="Remaining",
    text=aging_grp["Count"].astype(str) + " inv",
    color="bucket", color_discrete_map=AGING_COLORS,
    labels={"bucket": "Aging Bucket", "Remaining": "Outstanding ($)"},
)
fig_aging.update_traces(textposition="outside")
fig_aging.update_layout(height=360, showlegend=False, margin=dict(t=30, b=10))
st.plotly_chart(fig_aging, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Detail Tables
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Opportunities", "Invoice Headers", "Invoice Line Items"])

with tab1:
    st.dataframe(
        opps_f[[
            "opportunity_name", "account_name", "global_client_name",
            "sales_group", "product", "close_date", "nre_contract_value", "description",
        ]].rename(columns={
            "opportunity_name":   "Opportunity",
            "account_name":       "Account",
            "global_client_name": "Global Client",
            "sales_group":        "Sales Group",
            "product":            "Product",
            "close_date":         "Close Date",
            "nre_contract_value": "NRE Value",
            "description":        "Description",
        }),
        use_container_width=True, hide_index=True,
    )

with tab2:
    display_inv = inv_f[[
        "invoice_date", "invoice_number", "name", "status",
        "amount", "amount_paid", "adjusted_unpaid_amount",
        "invoice_due_date", "invoice_closed_date",
    ]].copy()
    display_inv["billed_year"] = display_inv["invoice_date"].dt.year
    display_inv["paid_year"]   = display_inv["invoice_closed_date"].dt.year
    st.dataframe(
        display_inv.rename(columns={
            "invoice_date":          "Invoice Date",
            "invoice_number":        "Invoice #",
            "name":                  "Customer",
            "status":                "Status",
            "amount":                "Amount",
            "amount_paid":           "Paid",
            "adjusted_unpaid_amount":"Outstanding",
            "invoice_due_date":      "Due Date",
            "invoice_closed_date":   "Closed Date",
            "billed_year":           "Billed Year",
            "paid_year":             "Paid Year",
        }),
        use_container_width=True, hide_index=True,
    )

with tab3:
    st.dataframe(
        li_f[[
            "invoice_date", "invoice_number", "name", "item",
            "description", "bitgo_lineitem", "amount", "amount_paid",
            "adjusted_unpaid_amount", "status",
        ]].rename(columns={
            "invoice_date":          "Invoice Date",
            "invoice_number":        "Invoice #",
            "name":                  "Customer",
            "item":                  "Item",
            "description":           "Description",
            "bitgo_lineitem":        "BitGo Line Item",
            "amount":                "Amount",
            "amount_paid":           "Paid",
            "adjusted_unpaid_amount":"Outstanding",
            "status":                "Status",
        }),
        use_container_width=True, hide_index=True,
    )
