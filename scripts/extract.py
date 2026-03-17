import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from requests import Response


logger = logging.getLogger(__name__)

COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"


@dataclass(frozen=True)
class ExtractResult:
    """
    Result of the extract step.

    - fetched_at_utc: timestamp for when we fetched the prices (UTC)
    - data: raw JSON dict returned by CoinGecko
    """

    fetched_at_utc: datetime
    data: Dict[str, Any]


def fetch_prices(coin_ids: List[str], vs_currency: str = "usd", timeout_s: int = 10) -> ExtractResult:
    """
    Extract step: call CoinGecko and return the raw JSON plus a fetch timestamp.

    We keep this function focused on I/O (HTTP request + JSON parsing).
    Validation is handled in the transform step.
    """
    logger.info("Extract: fetching prices from CoinGecko (%s)", COINGECKO_SIMPLE_PRICE_URL)
    logger.info("Extract: coins=%s vs_currency=%s", coin_ids, vs_currency)

    params = {"ids": ",".join(coin_ids), "vs_currencies": vs_currency}
    fetched_at = datetime.now(timezone.utc)

    response: Response = requests.get(COINGECKO_SIMPLE_PRICE_URL, params=params, timeout=timeout_s)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.error("Extract: CoinGecko returned HTTP error. response=%s", response.text)
        raise RuntimeError(f"CoinGecko API request failed: {exc}") from exc

    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        logger.error("Extract: failed to decode JSON. response=%s", response.text)
        raise RuntimeError("CoinGecko response was not valid JSON.") from exc

    logger.info("Extract: success")
    return ExtractResult(fetched_at_utc=fetched_at, data=data)

