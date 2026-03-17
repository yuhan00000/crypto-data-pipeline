import logging
from datetime import datetime

import pandas as pd

from db import get_connection


logger = logging.getLogger(__name__)


def upsert_crypto_prices(df_wide: pd.DataFrame) -> None:
    """
    Load step: insert the row into Postgres, updating if timestamp already exists.

    Requires a UNIQUE constraint/index on crypto_prices(\"timestamp\").
    """
    logger.info("Load: start")

    if len(df_wide) != 1:
        raise ValueError("df_wide must have exactly 1 row (one ETL run = one upsert).")

    row = df_wide.iloc[0]
    ts = datetime.fromisoformat(str(row["timestamp"]))
    btc = float(row["bitcoin_price"])
    eth = float(row["ethereum_price"])

    sql = """
    INSERT INTO crypto_prices ("timestamp", bitcoin_price, ethereum_price)
    VALUES (%s, %s, %s)
    ON CONFLICT ("timestamp")
    DO UPDATE SET
      bitcoin_price = EXCLUDED.bitcoin_price,
      ethereum_price = EXCLUDED.ethereum_price
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ts, btc, eth))
        conn.commit()

    logger.info("Load: upserted row for timestamp=%s", ts)

