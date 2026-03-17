import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

import pandas as pd


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformResult:
    """
    Result of the transform step.

    - df_long: one row per coin (useful for debugging)
    - df_wide: one row per ETL run (ready to load into Postgres)
    """

    df_long: pd.DataFrame
    df_wide: pd.DataFrame


def _validate_prices(prices: Dict[str, Any]) -> None:
    """
    Data validation rules:
    - bitcoin and ethereum must exist
    - values must not be None
    - values must be > 0
    """
    required = ["bitcoin", "ethereum"]
    missing = [c for c in required if c not in prices]
    if missing:
        raise ValueError(f"Missing required coins in API data: {missing}")

    for coin in required:
        value = prices[coin]
        if value is None:
            raise ValueError(f"{coin} price is None")
        try:
            value_f = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{coin} price is not a number: {value!r}") from exc
        if value_f <= 0:
            raise ValueError(f"{coin} price must be > 0, got {value_f}")


def transform_prices(api_data: Dict[str, Any], fetched_at_utc: datetime, vs_currency: str = "usd") -> TransformResult:
    """
    Transform step: turn the raw CoinGecko JSON into pandas DataFrames.

    Input (example):
      {\"bitcoin\": {\"usd\": 123}, \"ethereum\": {\"usd\": 45}}

    Output:
    - df_long columns: timestamp, coin, price_usd
    - df_wide columns: timestamp, bitcoin_price, ethereum_price
    """
    logger.info("Transform: start")

    # Build long-form rows first (one row per coin).
    rows = []
    for coin, prices in api_data.items():
        price = None
        if isinstance(prices, dict):
            price = prices.get(vs_currency)
        rows.append(
            {
                "timestamp": fetched_at_utc.isoformat(),
                "coin": coin,
                "price_usd": price,
            }
        )

    df_long = pd.DataFrame(rows, columns=["timestamp", "coin", "price_usd"])

    # Extract bitcoin/ethereum prices for validation and the wide row.
    prices_map = df_long.set_index("coin")["price_usd"].to_dict()
    _validate_prices(prices_map)

    df_wide = pd.DataFrame(
        [
            {
                "timestamp": fetched_at_utc.isoformat(),
                "bitcoin_price": float(prices_map["bitcoin"]),
                "ethereum_price": float(prices_map["ethereum"]),
            }
        ],
        columns=["timestamp", "bitcoin_price", "ethereum_price"],
    )

    logger.info("Transform: success")
    return TransformResult(df_long=df_long, df_wide=df_wide)

