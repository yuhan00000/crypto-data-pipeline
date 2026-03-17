import logging
import os
from typing import List

from extract import fetch_prices
from load import upsert_crypto_prices
from transform import transform_prices


def configure_logging() -> None:
    """
    Configure structured-ish logging once for the whole pipeline.

    You can set LOG_LEVEL=DEBUG for more verbose logs.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_pipeline(coin_ids: List[str] | None = None, vs_currency: str = "usd") -> None:
    """
    Orchestrate the pipeline: Extract -> Transform (validate) -> Load (upsert).
    """
    logger = logging.getLogger(__name__)

    if coin_ids is None:
        coin_ids = ["bitcoin", "ethereum"]

    logger.info("Pipeline: start")
    extract_result = fetch_prices(coin_ids=coin_ids, vs_currency=vs_currency)

    transform_result = transform_prices(
        api_data=extract_result.data,
        fetched_at_utc=extract_result.fetched_at_utc,
        vs_currency=vs_currency,
    )

    # Helpful for debugging: keep a short preview in logs.
    logger.debug("Transform df_long tail:\n%s", transform_result.df_long.tail())
    logger.debug("Transform df_wide:\n%s", transform_result.df_wide)

    upsert_crypto_prices(transform_result.df_wide)
    logger.info("Pipeline: success")


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    try:
        run_pipeline()
    except Exception:
        # Log full stack trace. In production you'd also exit with non-zero code.
        logger.exception("Pipeline: failed")
        raise


if __name__ == "__main__":
    # Run with:
    #   python scripts/main.py
    main()

