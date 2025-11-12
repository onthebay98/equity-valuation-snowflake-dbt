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

---

### 🧮 Example Results — FY2024, Prices as of Jan 31 2025

| SYMBOL    |  PRICE | FISCAL_YEAR | REVENUE ($B) | EBITDA ($B) | NET_INCOME ($B) | FCF ($B) |        PE | EV/EBITDA |  EV/SALES |  FCF Yield |  Div Yield | Intrinsic Value (6% FCF Yield) | Upside vs Price |
| :-------- | -----: | ----------: | -----------: | ----------: | --------------: | -------: | --------: | --------: | --------: | ---------: | ---------: | -----------------------------: | --------------: |
| **AAPL**  | 236.00 |        2024 |        391.0 |       125.0 |            93.7 |    108.8 | **38.1×** | **28.1×** |  **9.0×** | **3.05 %** | **0.43 %** |                         119.96 |           −49 % |
| **MSFT**  | 415.06 |        2024 |        245.1 |       127.0 |            88.4 |    76.96 | **34.9×** | **24.1×** | **12.5×** | **2.49 %** | **0.68 %** |                         172.57 |           −58 % |
| **GOOGL** | 204.02 |        2024 |        350.0 |       130.0 |           100.1 |    72.76 | **25.1×** | **19.1×** |  **7.1×** | **2.90 %** | **0.29 %** |                          98.60 |           −52 % |
| **META**  | 689.18 |        2024 |        164.5 |        75.0 |            62.4 |     52.1 | **28.0×** | **22.6×** | **10.3×** | **2.98 %** | **0.29 %** |                         342.67 |           −50 % |

---

### 🧭 Interpretation

Even with strong earnings, all four megacaps screen as **overvalued** under a conservative 6 % FCF-yield framework — implying roughly 50 % downside to “fair” value if investors demanded higher cash yields.

Notes:

* **High multiples (P/E 30–40×, EV/EBITDA 20–25×):** justified only if growth and margins stay exceptional.
* **Low FCF yields (< 3 %):** the market prices these firms like long-duration growth assets, not cash cows.
* **Intrinsic value vs price:** all trade well above the fair-value curve; this aligns with 2025’s high-multiple tech environment.

---

**Author:** Bay Hodge

**Environment:** Python 3.13, dbt 1.11, Snowflake X-Small WH