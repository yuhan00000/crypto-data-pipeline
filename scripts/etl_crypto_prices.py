import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import requests
from requests import Response

from db import get_connection


COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"


def fetch_prices(coin_ids: List[str], vs_currency: str = "usd") -> Dict[str, Any]:
    """
    Call the CoinGecko simple price endpoint and return the parsed JSON.

    This function:
    - Builds the request URL and query parameters
    - Sends the HTTP GET request
    - Raises for HTTP errors so we fail fast with a clear message
    - Returns the response JSON as a Python dictionary
    """
    params = {
        "ids": ",".join(coin_ids),
        "vs_currencies": vs_currency,
    }

    response: Response = requests.get(COINGECKO_SIMPLE_PRICE_URL, params=params, timeout=10)

    # If the status code is not 2xx, this will raise an HTTPError.
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        # Include response text to make debugging easier for beginners.
        raise RuntimeError(f"Error calling CoinGecko API: {exc}\nResponse: {response.text}") from exc

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError("Failed to decode JSON from CoinGecko response.") from exc


def transform_to_dataframe(api_data: Dict[str, Any], vs_currency: str = "usd") -> pd.DataFrame:
    """
    Transform the CoinGecko JSON into a pandas DataFrame.

    Expected input format (example):
    {
        "bitcoin": {"usd": 12345.67},
        "ethereum": {"usd": 2345.89}
    }

    Output DataFrame columns:
    - timestamp: UTC timestamp when data was fetched
    - coin:      name of the coin (e.g., "bitcoin")
    - price_usd: price in USD (float)
    """
    timestamp = datetime.now(timezone.utc)

    rows = []
    for coin, prices in api_data.items():
        # For each coin, we pull the price in the requested currency (default: usd).
        price = prices.get(vs_currency)
        if price is None:
            # Skip coins that do not have the expected currency field.
            continue
        rows.append(
            {
                "timestamp": timestamp.isoformat(),
                "coin": coin,
                "price_usd": float(price),
            }
        )

    # Convert list of dictionaries into a DataFrame.
    df = pd.DataFrame(rows, columns=["timestamp", "coin", "price_usd"])
    return df


def to_wide_row(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the "long" DataFrame (one row per coin) into a single-row "wide" DataFrame.

    Input (long):
      timestamp | coin     | price_usd
      --------- | -------- | ---------
      ...       | bitcoin  | 65000.0
      ...       | ethereum | 3500.0

    Output (wide):
      timestamp | bitcoin_price | ethereum_price
    """
    if df_long.empty:
        raise ValueError("No rows found in long DataFrame (df_long is empty).")

    # Use the first timestamp; both coins were fetched in one API call.
    timestamp = df_long["timestamp"].iloc[0]

    prices = df_long.set_index("coin")["price_usd"].to_dict()
    if "bitcoin" not in prices or "ethereum" not in prices:
        raise ValueError(f"Expected bitcoin and ethereum in API data, got: {sorted(prices.keys())}")

    return pd.DataFrame(
        [
            {
                "timestamp": timestamp,
                "bitcoin_price": float(prices["bitcoin"]),
                "ethereum_price": float(prices["ethereum"]),
            }
        ],
        columns=["timestamp", "bitcoin_price", "ethereum_price"],
    )


def insert_row_into_postgres(df_wide: pd.DataFrame) -> None:
    """
    Insert a single row into the PostgreSQL table `crypto_prices`.

    This function assumes you already applied the schema in `database/schema.sql`.
    """
    if len(df_wide) != 1:
        raise ValueError("df_wide must contain exactly 1 row (one ETL run = one insert).")

    row = df_wide.iloc[0]
    # Timestamp is stored in the DB as TIMESTAMP (no timezone). We generate UTC timestamps.
    # psycopg2 accepts ISO strings, but passing a Python datetime is cleaner.
    ts = datetime.fromisoformat(str(row["timestamp"]))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crypto_prices ("timestamp", bitcoin_price, ethereum_price)
                VALUES (%s, %s, %s)
                """,
                (ts, float(row["bitcoin_price"]), float(row["ethereum_price"])),
            )
        conn.commit()


def run_etl() -> None:
    """
    Run the full ETL pipeline:
    1. Extract Bitcoin and Ethereum prices from CoinGecko.
    2. Transform into a pandas DataFrame with a timestamp.
    3. Insert a new row into PostgreSQL (table: crypto_prices).
    """
    coin_ids = ["bitcoin", "ethereum"]
    vs_currency = "usd"

    print("Starting ETL pipeline for crypto prices...")
    print(f"Fetching prices for: {', '.join(coin_ids)} in {vs_currency.upper()}.")

    api_data = fetch_prices(coin_ids=coin_ids, vs_currency=vs_currency)
    print("Raw API response:", api_data)

    df_long = transform_to_dataframe(api_data, vs_currency=vs_currency)
    print("Transformed long DataFrame (one row per coin):")
    print(df_long)

    df_wide = to_wide_row(df_long)
    print("Transformed wide DataFrame (one row per run):")
    print(df_wide)

    insert_row_into_postgres(df_wide)
    print("Inserted 1 row into PostgreSQL table: crypto_prices")
    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    # Allow the script to be run directly from the command line.
    # Example:
    #   python scripts/etl_crypto_prices.py
    run_etl()

