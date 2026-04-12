"""
Deploys nre_dashboard_sf.py + environment.yml to Snowflake
and creates (or replaces) the Streamlit app.

Run:  python3 deploy_sf.py
      (browser will open for Okta login)
"""
import os
import snowflake.connector
from pathlib import Path

# ---------------------------------------------------------------------------
# Config — edit APP_NAME or STAGE_NAME if you want a different name
# ---------------------------------------------------------------------------
ACCOUNT       = "LURETDR-BITGO_PROD"
USER          = "akashnathrani901@BITGO.COM"
AUTHENTICATOR = "externalbrowser"
WAREHOUSE     = "QUERY_WAREHOUSE_XS"
DATABASE      = "MARTS"
SCHEMA        = "MARTS"           # Streamlit apps live in MARTS.MARTS

APP_NAME      = "NRE_DASHBOARD"   # MARTS.MARTS.NRE_DASHBOARD
STAGE_NAME    = "NRE_DASHBOARD_STAGE"

HERE = Path(__file__).parent

# ---------------------------------------------------------------------------
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
    cur = conn.cursor()
    print("Connected.\n")

    # 1. Create stage
    print(f"Creating stage {DATABASE}.{SCHEMA}.{STAGE_NAME} …")
    cur.execute(f"""
        CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE_NAME}
        DIRECTORY = (ENABLE = TRUE)
        COMMENT = 'NRE Dashboard Streamlit app files'
    """)
    print("  Stage ready.\n")

    # 2. Upload files
    files = [
        HERE / "nre_dashboard_sf.py",
        HERE / "environment.yml",
    ]
    for f in files:
        print(f"Uploading {f.name} …")
        cur.execute(f"""
            PUT file://{f}
            @{DATABASE}.{SCHEMA}.{STAGE_NAME}
            OVERWRITE = TRUE
            AUTO_COMPRESS = FALSE
        """)
        print(f"  {f.name} uploaded.\n")

    # 3. Create or replace Streamlit app
    print(f"Creating Streamlit app {DATABASE}.{SCHEMA}.{APP_NAME} …")
    cur.execute(f"""
        CREATE OR REPLACE STREAMLIT {DATABASE}.{SCHEMA}.{APP_NAME}
            ROOT_LOCATION = '@{DATABASE}.{SCHEMA}.{STAGE_NAME}'
            MAIN_FILE     = '/nre_dashboard_sf.py'
            QUERY_WAREHOUSE = {WAREHOUSE}
            COMMENT = 'NRE Sales and Invoice Dashboard'
    """)
    print("  Streamlit app created.\n")

    # 4. Print the URL
    cur.execute(f"""
        SELECT SYSTEM$STREAMLIT_NOTEBOOK_URL(
            '{DATABASE}.{SCHEMA}.{APP_NAME}'
        )
    """)
    row = cur.fetchone()
    if row and row[0]:
        print(f"Dashboard URL:\n  {row[0]}\n")
    else:
        print(
            f"App created. Open it at:\n"
            f"  https://app.snowflake.com/luretdr/bitgo_prod/"
            f"#/streamlit-apps/{DATABASE}.{SCHEMA}.{APP_NAME}\n"
        )

    cur.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
