# Lab 3. Analytical dashboard — Option 25: Cryptocurrency Market Analysis

## 1. Goal
Build an end-to-end pipeline **Kaggle → Apache Airflow → PostgreSQL → Apache Superset** and a dashboard comparing Bitcoin (BTC) and Ethereum (ETH).

Dataset: `maharshipandya/-cryptocurrency-historical-prices-dataset` (Kaggle).

## 2. Architecture
Everything runs in Docker Compose on an Ubuntu 20.04 VM.

| Service | Port | Purpose |
|---|---|---|
| Airflow webserver | 8080 | orchestration UI |
| Airflow scheduler | – | runs DAGs |
| postgres | 5432 | Airflow metadata |
| analytics_postgres | 5433 (host) / 5432 (inside Docker) | RAW, STG, DATAMART |
| Superset | 8088 | charts and dashboard |
| pgAdmin | 5050 | DB inspection |
| Redis | 6379 | Superset cache |

Data flow: `Kaggle CSV → load (RAW) → transform (STG) → create VIEW (DATAMART) → Superset`.

## 3. Data layers
| Layer | Object | Content |
|---|---|---|
| RAW | `raw_crypto` | unchanged copy of the CSV, all fields as text |
| STG | `stg_crypto` | typed and cleaned data (BTC and ETH only, no duplicates/NULL prices) |
| DATAMART | `crypto_datamart` (VIEW) | `date, btc_price_close, eth_price_close, btc_market_cap, eth_market_cap` |

Superset is connected **to the VIEW**, not to `stg_*`.

## 4. Airflow DAG (`dags/crypto_dag.py`)

```python
from datetime import datetime, timedelta
import glob, os

import pandas as pd
import kagglehub
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DATASET = "maharshipandya/-cryptocurrency-historical-prices-dataset"
CONN = "postgresql+psycopg2://analytics:analytics@analytics_postgres:5432/analytics"

default_args = {"retries": 2, "retry_delay": timedelta(minutes=1)}


def load_raw():
    path = kagglehub.dataset_download(DATASET)
    files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)
    df = pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df.to_sql("raw_crypto", create_engine(CONN), if_exists="replace", index=False)


def transform_stg():
    eng = create_engine(CONN)
    df = pd.read_sql("SELECT * FROM raw_crypto", eng)

    # the coin identifier column may be called differently in the file
    coin_col = next(c for c in ("symbol", "crypto_name", "name", "coin") if c in df.columns)
    cap_col = "marketcap" if "marketcap" in df.columns else "market_cap"

    df = df.rename(columns={coin_col: "symbol", cap_col: "market_cap"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["symbol"] = df["symbol"].replace({"BITCOIN": "BTC", "ETHEREUM": "ETH"})

    before = len(df)
    df = df[df["symbol"].isin(["BTC", "ETH"])]
    df = df.dropna(subset=["date", "close", "market_cap"])
    df = df.drop_duplicates(subset=["date", "symbol"])
    print(f"Rows removed during cleaning: {before - len(df)}") # deletions are logged

    df[["date", "symbol", "close", "market_cap"]].to_sql(
        "stg_crypto", eng, if_exists="replace", index=False
    )


with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    load = PythonOperator(task_id="load_raw", python_callable=load_raw)
    transform = PythonOperator(task_id="transform_stg", python_callable=transform_stg)

    create_view = PostgresOperator(
        task_id="create_datamart_view",
        postgres_conn_id="analytics_postgres",
        sql="""
        CREATE OR REPLACE VIEW crypto_datamart AS
        SELECT b.date,
               b.close AS btc_price_close,
               e.close AS eth_price_close,
               b.market_cap AS btc_market_cap,
               e.market_cap AS eth_market_cap
        FROM stg_crypto b
        JOIN stg_crypto e ON e.date = b.date AND e.symbol = 'ETH'
        WHERE b.symbol = 'BTC';
        """,
    )

    load >> transform >> create_view
```

## 5. Airflow connection `analytics_postgres`
Admin → Connections → +

| Field | Value |
|---|---|
| Connection ID | `analytics_postgres` |
| Type | Postgres |
| Host | `analytics_postgres` |
| Schema | `analytics` |
| Login / Password | `analytics` / `analytics` |
| Port | `5432` (internal Docker port) |

## 6. Superset
1. Add database: `postgresql+psycopg2://analytics:analytics@analytics_postgres:5432/analytics`
2. Dataset: `crypto_datamart`
3. Charts (all five required types):

| Type | Chart | Settings |
|---|---|---|
| Indicator (Big Number) | Latest BTC price | metric: last `btc_price_close`, sorted by `date` |
| Linear | BTC price dynamics | X: `date`, metric: `btc_price_close` |
| Columnar (Bar) | Average BTC price by year | X: year of `date`, metric: AVG(`btc_price_close`) |
| Circular (Pie) | Capitalization BTC vs ETH | metrics: latest `btc_market_cap`, `eth_market_cap` |
| Combined (Mixed) | BTC and ETH price by date | Query A: `btc_price_close` (line), Query B: `eth_price_close` (line) |

4. Add all charts to one dashboard with a date filter.

## 7. Run order
```bash
git clone <repo-url> && cd lpw_04
chmod +x setup_kaggle.sh && ./setup_kaggle.sh
docker compose up -d
docker compose ps
# Airflow :8080 -> create analytics_postgres, enable and trigger crypto_pipeline
# pgAdmin :5050 -> check raw_crypto, stg_crypto, crypto_datamart
# Superset :8088 -> connect DB, create dataset, charts, dashboard
```

## 8. Conclusions and insights
> 

- BTC price and ETH price move in the same direction most of the time (high correlation), so ETH is largely driven by overall market sentiment and BTC trends.
- BTC holds the larger share of the combined BTC+ETH capitalization (see pie chart: _78__% vs __67_%).
- Average BTC price by year shows the growth/decline periods: the highest year is _89__, the lowest is __14_.
- Sharp rises/falls on the line chart (around _56__) are periods of high volatility; they are risky for short-term investors.
- Business recommendation: diversify between BTC (stability, larger cap) and ETH (higher growth potential, higher volatility).

## 9. Security
`kaggle.json` is **not** stored in the repository; it is listed in `.gitignore`.
