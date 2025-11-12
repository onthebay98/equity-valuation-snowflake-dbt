import os
import csv
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "EQUITY_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "EQUITY_DB")

PRICES_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "prices.csv")
FUNDS_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "fundamentals.csv")


def get_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )


def load_prices(conn):
    cur = conn.cursor()
    try:
        cur.execute("USE SCHEMA RAW")
        cur.execute("TRUNCATE TABLE IF EXISTS PRICES")
        with open(PRICES_CSV, newline="") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    r["symbol"],
                    r["date"],
                    float(r["close"]),
                    int(r["volume"]),
                )
                for r in reader
            ]
        cur.executemany(
            "INSERT INTO PRICES(symbol, date, close, volume) VALUES (%s, %s, %s, %s)",
            rows,
        )
    finally:
        cur.close()


def load_fundamentals(conn):
    cur = conn.cursor()
    try:
        cur.execute("USE SCHEMA RAW")
        cur.execute("TRUNCATE TABLE IF EXISTS FUNDAMENTALS")
        with open(FUNDS_CSV, newline="") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    r["symbol"],
                    int(r["fiscal_year"]),
                    float(r["revenue"]),
                    float(r["ebitda"]),
                    float(r["net_income"]),
                    float(r["fcf"]),
                    float(r["total_debt"]),
                    float(r["cash"]),
                    float(r["shares_out"]),
                    float(r["dividends"]),
                )
                for r in reader
            ]
        cur.executemany(
            """
            INSERT INTO FUNDAMENTALS(
              symbol, fiscal_year, revenue, ebitda,
              net_income, fcf, total_debt, cash,
              shares_out, dividends
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    finally:
        cur.close()


def main():
    required = [
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_WAREHOUSE,
        SNOWFLAKE_DATABASE,
    ]
    if not all(required):
        raise RuntimeError("Missing Snowflake env vars in .env")

    conn = get_conn()
    try:
        load_prices(conn)
        load_fundamentals(conn)
        conn.commit()
        print("[INFO] Loaded CSV data into Snowflake RAW.PRICES and RAW.FUNDAMENTALS")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
