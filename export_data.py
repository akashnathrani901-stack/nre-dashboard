"""
One-shot script: connects to Snowflake, runs the 3 dashboard queries,
and saves the results as parquet files in the data/ directory.
Run this once; teammates can then use the app without Snowflake access.
"""
import os
import sys
from pathlib import Path
import pandas as pd
import snowflake.connector

# ── Credentials ──────────────────────────────────────────────────────────────
ACCOUNT       = "LURETDR-BITGO_PROD"
USER          = "akashnathrani901@BITGO.COM"
AUTHENTICATOR = "externalbrowser"
WAREHOUSE     = "QUERY_WAREHOUSE_XS"
DATABASE      = "MARTS"
SCHEMA        = "PUBLIC"

# ── SQL ───────────────────────────────────────────────────────────────────────
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

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("Connecting to Snowflake (browser will open for Okta login)…")
    conn = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        authenticator=AUTHENTICATOR,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        client_store_temporary_credential=True,
    )
    print("Connected.")

    out = Path(__file__).parent / "data"
    out.mkdir(exist_ok=True)

    queries = [
        ("Opportunities",    OPPS_SQL,       out / "opps_cache.parquet"),
        ("Invoice Headers",  INVOICES_SQL,   out / "invoices_cache.parquet"),
        ("Invoice LineItems",LINE_ITEMS_SQL, out / "lineitems_cache.parquet"),
    ]

    for label, sql, path in queries:
        print(f"  Fetching {label}…", end=" ", flush=True)
        df = pd.read_sql(sql, conn)
        df.columns = df.columns.str.lower()
        df.to_parquet(path, index=False)
        print(f"{len(df):,} rows → {path.name}")

    conn.close()
    print("\nDone. Share the data/ folder with teammates who don't have Snowflake access.")

if __name__ == "__main__":
    main()
