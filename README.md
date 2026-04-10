# NRE Sales & Invoice Dashboard

Streamlit dashboard for tracking Closed Won NRE opportunities, NetSuite invoice status, and payment collection — with live Snowflake queries or cached parquet data.

## Features

- KPI cards: NRE contract value, invoiced, paid, outstanding, collection rate
- Invoiced vs Paid vs Remaining by account
- Invoice Status donut, Monthly trend, Billed Year × Status heatmap
- Line item breakdown by description
- Invoice aging buckets
- Sidebar filters: Sales Group, Account, Invoice Status, Billed Year, Paid Year (with AND / OR / Same Year logic)
- Chart sort controls (by amount, paid, remaining, or count)

## Setup

### Option A — With Snowflake access

1. Clone the repo and install dependencies:
   ```bash
   git clone https://github.com/akashnathrani901-stack/nre-dashboard.git
   cd nre-dashboard
   pip install -r requirements.txt
   ```

2. Create `.streamlit/secrets.toml` (never committed — see `.gitignore`):
   ```toml
   [snowflake]
   user          = "your.email@bitgo.com"
   account       = "LURETDR-BITGO_PROD"
   authenticator = "externalbrowser"
   warehouse     = "QUERY_WAREHOUSE_XS"
   database      = "MARTS"
   schema        = "PUBLIC"
   ```

3. Run the dashboard:
   ```bash
   streamlit run nre_dashboard.py
   ```
   A browser window will open for Okta/SSO login. After authenticating, click **"Save to cache"** in the sidebar to generate the parquet files for teammates.

### Option B — Without Snowflake access (cached data)

Get the `data/` folder (3 parquet files) from a teammate with Snowflake access, place them in the repo root under `data/`, then:

```bash
pip install -r requirements.txt
streamlit run nre_dashboard.py
```

The app will automatically detect and load from the cache — no Snowflake credentials needed.

## Requirements

- Python 3.11+
- See `requirements.txt`
