import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import requests
from requests import Response


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


def append_to_csv(df: pd.DataFrame, csv_path: Path) -> None:
    """
    Append the DataFrame to a CSV file.

    - If the file does not exist, it will be created with a header.
    - If the file exists, new rows will be appended without duplicating the header.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = csv_path.exists()

    df.to_csv(
        csv_path,
        mode="a" if file_exists else "w",
        header=not file_exists,
        index=False,
    )


def run_etl() -> None:
    """
    Run the full ETL pipeline:
    1. Extract Bitcoin and Ethereum prices from CoinGecko.
    2. Transform into a pandas DataFrame with a timestamp.
    3. Append the records into data/crypto_prices.csv.
    """
    coin_ids = ["bitcoin", "ethereum"]
    vs_currency = "usd"

    print("Starting ETL pipeline for crypto prices...")
    print(f"Fetching prices for: {', '.join(coin_ids)} in {vs_currency.upper()}.")

    api_data = fetch_prices(coin_ids=coin_ids, vs_currency=vs_currency)
    print("Raw API response:", api_data)

    df = transform_to_dataframe(api_data, vs_currency=vs_currency)
    print("Transformed DataFrame:")
    print(df)

    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "data" / "crypto_prices.csv"

    append_to_csv(df, csv_path)

    print(f"Data appended to CSV at: {csv_path}")
    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    # Allow the script to be run directly from the command line.
    # Example:
    #   python scripts/etl_crypto_prices.py
    run_etl()

