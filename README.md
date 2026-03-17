## Crypto Data Pipeline

I built this project as a **beginner-friendly data engineering exercise**. It’s a small Python ETL pipeline that collects cryptocurrency prices (Bitcoin and Ethereum) from the **CoinGecko API** and stores them in **PostgreSQL**.

The goal is to practice a simple **ETL (Extract–Transform–Load)** workflow using Python, `requests`, `pandas`, and `psycopg2`.

---

## Project Structure

- `data/`  
  (Legacy) Stores the old CSV file version of historical prices from the first version of the project.

- `database/`  
  Stores the SQL schema I use to create the PostgreSQL table.
  - `schema.sql` – creates the `crypto_prices` table.

- `scripts/`  
  Contains the Python scripts that implement the pipeline.
  - `main.py` – orchestrates the pipeline (recommended entrypoint).
  - `extract.py` – handles CoinGecko API requests.
  - `transform.py` – cleans/formats the data and validates it.
  - `load.py` – inserts/upserts data into PostgreSQL.
  - `db.py` – PostgreSQL connection helper (reads env vars).
  - `etl_crypto_prices.py` – legacy entrypoint that forwards to `main.py` (kept for backwards compatibility).
  - `plot_bitcoin_trend.py` – visualization script:
    - Reads Bitcoin prices from the PostgreSQL `crypto_prices` table.
    - Generates a line chart and saves it to `charts/bitcoin_price_trend.png`.

- `charts/`  
  Contains generated charts.
  - `bitcoin_price_trend.png` – created by `plot_bitcoin_trend.py`.

- `requirements.txt`  
  Lists the Python dependencies required to run the project.

---

## Setup

### 1. Prerequisites

- Python 3.10+ (any recent 3.x version should work)
- `pip` for installing Python packages

### 2. Install Dependencies

From the project root (where `requirements.txt` is located), run:

```bash
pip install -r requirements.txt
```

This installs:

- `requests` – to call the CoinGecko API.
- `pandas` – to work with tabular data (DataFrame).
- `matplotlib` – to create the Bitcoin price chart.
- `psycopg2-binary` – to connect to PostgreSQL from Python.

---

## PostgreSQL Setup (Local)

Before running the ETL script, two things is needed:

1. Create a PostgreSQL database (example name: `crypto_db`)
2. Apply the schema in `database/schema.sql` to create the `crypto_prices` table

Example command (requires `psql`):

```bash
psql -h localhost -U postgres -d crypto_db -f database/schema.sql
```

### Environment Variables for DB Connection

The ETL script reads connection settings from environment variables:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Example (PowerShell):

```powershell
$env:PGHOST="localhost"
$env:PGPORT="5432"
$env:PGDATABASE="crypto_db"
$env:PGUSER="postgres"
$env:PGPASSWORD="your_password"
```

---

## Running the ETL Pipeline

The ETL pipeline:

1. **Extracts** Bitcoin and Ethereum prices (in USD) from the CoinGecko API.
2. **Transforms** the response into a tidy `pandas.DataFrame` with:
   - `timestamp` – when the data was fetched (UTC).
   - `coin` – the coin name (`bitcoin` or `ethereum`).
   - `price_usd` – price in US dollars.
3. **Validates**:
   - Bitcoin and Ethereum prices are not `None`
   - Prices are numbers and greater than 0
4. **Loads** (upserts) a row into PostgreSQL table `crypto_prices` with columns:
   - `timestamp`
   - `bitcoin_price`
   - `ethereum_price`

From the project root, run:

```bash
python scripts/main.py
```

After running:

- The script writes to PostgreSQL using **UPSERT** (`ON CONFLICT DO UPDATE`) on `timestamp`.
- A unique index on `timestamp` prevents duplicates at the database level.

---

## Generating the Bitcoin Price Trend Chart

From the project root, run:

```bash
python scripts/plot_bitcoin_trend.py
```

The script will:
1. Read Bitcoin prices from the PostgreSQL `crypto_prices` table.
2. Parse the `timestamp` column into proper datetime objects.
3. Sort the records by time.
4. Plot **timestamp vs. bitcoin_price** using `matplotlib`.
5. Save the chart to:

```text
charts/bitcoin_price_trend.png
```

If the Postgres is missing or there is no Bitcoin data, the script will raise a clear error message explaining what to fix.

---

## ETL Architecture Overview

At a high level, the pipeline follows a simple ETL flow.

```mermaid
flowchart LR
  userRun[User_runs_script] --> etl[ETL_Script]
  etl --> extract[Extract_from_CoinGecko]
  extract --> transform[Transform_to_DataFrame]
  transform --> addTs[Add_timestamp_column]
  addTs --> insertDb[Insert_into_PostgreSQL]
  insertDb --> chartScript[Chart_Script]
  chartScript --> btcChart[Bitcoin_price_chart.png]
```

### Components

- **Extract** (`scripts/extract.py` – `fetch_prices`)
  - Calls the CoinGecko Simple Price API:
    - Endpoint: `https://api.coingecko.com/api/v3/simple/price`
    - Coins: `bitcoin`, `ethereum`
    - Currency: `usd`
  - Handles basic HTTP errors and JSON parsing.

- **Transform** (`scripts/transform.py` – `transform_prices`)
  - Converts the API response into DataFrames and validates required values.

- **Load** (`scripts/load.py` – `upsert_crypto_prices`)
  - Upserts one row into PostgreSQL table `crypto_prices` per run.
  - Connection settings come from environment variables (so I don’t hardcode secrets).

- **Visualization** (`scripts/plot_bitcoin_trend.py`)
  - Reads prices from PostgreSQL and produces the chart in `charts/bitcoin_price_trend.png`.

---

## How to Extend This Project

This is just a starting point for me. Here are some directions I might take it:

- **Scheduling**  
  - Use Windows Task Scheduler (on Windows) or `cron` (on Linux/macOS) to run:
    - `python scripts/etl_crypto_prices.py` once per day.

- **More Coins / Currencies**  
  - Add more coin IDs (e.g., `litecoin`, `cardano`) or track prices in other fiat currencies.

- **Database Storage**  
  - I already migrated the pipeline to PostgreSQL. Next, I might add indexing and basic data quality checks.

- **Dashboards**  
  - Build a simple dashboard using tools like Streamlit or a BI tool to visualize trends interactively.

This project is my way of learning ETL fundamentals with a real-world dataset that I find interesting.

