# 🧮 Equity Valuation Mini-Mart — Snowflake + dbt

This project demonstrates a modern data-analytics pipeline for **equity valuation** using
**Snowflake** for warehousing, **dbt** for transformation and testing, and a small Python ingestion script for loading data.

It mirrors how a buy-side or internal analytics team would structure a production-grade stack for valuation and portfolio analytics.

---

## 🚀 Overview

**Goal:** Build a clean, reproducible analytics mart that answers

> “Which companies appear over- or undervalued based on fundamental metrics and a normalized FCF yield assumption?”

**Stack**

| Layer          | Tool                         | Purpose                                                     |
| -------------- | ---------------------------- | ----------------------------------------------------------- |
| Ingestion      | Python + Snowflake Connector | Load price & fundamentals into Snowflake                    |
| Storage        | Snowflake                    | Centralized, queryable data warehouse                       |
| Transformation | dbt (Snowflake adapter)      | Modeling, testing, lineage, documentation                   |
| Analytics      | SQL                          | Compute valuation metrics (P/E, EV/EBITDA, FCF yield, etc.) |

---

## 🧱 Architecture

```
CSV data → Snowflake RAW tables → dbt staging models → dbt mart (valuation view)
```

**Schemas**

* `RAW`: source data (prices & fundamentals)
* `ANALYTICS`: dbt-generated staging + valuation models

---

## 📂 Project Structure

```text
equity-valuation-snowflake-dbt/
├── ingest/
│   └── ingest_to_snowflake.py        # loads CSVs into Snowflake RAW tables
├── data/
│   ├── prices.csv
│   └── fundamentals.csv
├── models/
│   ├── staging/
│   │   ├── stg_prices.sql
│   │   └── stg_fundamentals.sql
│   ├── marts/
│   │   └── equity_valuations.sql
│   └── schema.yml
├── dbt_project.yml
├── README.md
└── .env
```

---

## ⚙️ Setup

### 1. Prerequisites

* Python ≥ 3.10
* Snowflake trial or account
* dbt (Snowflake adapter)
* Git

### 2. Environment Setup

```bash
git clone https://github.com/<your_user>/equity-valuation-snowflake-dbt.git
cd equity-valuation-snowflake-dbt
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install dbt-snowflake snowflake-connector-python python-dotenv
```

### 3. Configure Snowflake

Create the database, warehouse, and schemas in Snowflake:

```sql
create or replace warehouse EQUITY_WH warehouse_size = 'XSMALL' auto_suspend = 60 auto_resume = true;
create or replace database EQUITY_DB;
create or replace schema EQUITY_DB.RAW;
create or replace schema EQUITY_DB.ANALYTICS;
```

Add your Snowflake credentials in `.env`:

```env
SNOWFLAKE_ACCOUNT=AYWHMSC-KWA49735
SNOWFLAKE_USER=<your_username>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=EQUITY_WH
SNOWFLAKE_DATABASE=EQUITY_DB
```

### 4. Configure dbt Profile

Create `~/.dbt/profiles.yml`:

```yaml
equity_valuation:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: AYWHMSC-KWA49735
      user: <your_username>
      password: <your_password>
      role: ACCOUNTADMIN
      database: EQUITY_DB
      warehouse: EQUITY_WH
      schema: ANALYTICS
      threads: 4
      client_session_keep_alive: false
```

---

## 🧩 Run the Pipeline

### Step 1 — Load Data into Snowflake

```bash
python ingest/ingest_to_snowflake.py
```

This loads the sample CSVs into:

* `RAW.PRICES`
* `RAW.FUNDAMENTALS`

### Step 2 — Build dbt Models

```bash
dbt debug       # test connection
dbt run         # build models
dbt test        # run data tests
dbt docs generate && dbt docs serve   # open lineage & documentation UI
```

dbt creates:

* `ANALYTICS.STG_PRICES`
* `ANALYTICS.STG_FUNDAMENTALS`
* `ANALYTICS.EQUITY_VALUATIONS`

---

## 📊 Outputs

**`EQUITY_VALUATIONS` columns include:**

| Metric                     | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| `price`                    | Latest closing price                                         |
| `market_cap`               | Price × shares outstanding                                   |
| `enterprise_value`         | Market cap + debt − cash                                     |
| `pe`                       | Price-to-earnings ratio                                      |
| `ev_ebitda`                | EV / EBITDA                                                  |
| `ev_sales`                 | EV / Revenue                                                 |
| `fcf_yield`                | Free Cash Flow / Market Cap                                  |
| `div_yield`                | Dividend / Market Cap                                        |
| `iv_fcf_yield_6pct`        | Intrinsic value per share (assuming 6% normalized FCF yield) |
| `iv_fcf_yield_6pct_upside` | % upside vs current price                                    |

Example:

| symbol | price | pe   | ev_ebitda | fcf_yield | iv_fcf_yield_6pct_upside |
| ------ | ----- | ---- | --------- | --------- | ------------------------ |
| AAPL   | 210   | 28.4 | 21.1      | 3.8%      | -17%                     |
| MSFT   | 420   | 35.2 | 24.7      | 3.3%      | -24%                     |
| META   | 390   | 21.0 | 13.9      | 6.5%      | +10%                     |

---

## 🧠 How to Interpret It

Analysts would use this table to:

* **Screen** for cheap or expensive names.
* **Compare** valuation multiples across peers.
* **Estimate** fair value via normalized FCF yield.
* **Rank** by upside/downside for further research.

---

**Author:** Bay Hodge
**Environment:** Python 3.13, dbt 1.11, Snowflake X-Small WH
