from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from db import get_connection  # assumes you run: python scripts/plot_bitcoin_trend.py


def load_bitcoin_from_postgres() -> pd.DataFrame:
    """
    Load Bitcoin price data from the PostgreSQL table `crypto_prices`.

    - Reads timestamp and bitcoin_price
    - Sorts by time
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                SELECT "timestamp", bitcoin_price
                FROM crypto_prices
                ORDER BY "timestamp";
                '''
            )
            rows = cur.fetchall()

    if not rows:
        raise ValueError("No Bitcoin data found in crypto_prices table.")

    df = pd.DataFrame(rows, columns=["timestamp", "bitcoin_price"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    return df


def plot_bitcoin_trend(btc_df: pd.DataFrame, output_path: Path) -> None:
    """
    Plot the Bitcoin price trend over time and save it as a PNG file.

    - X-axis: timestamp
    - Y-axis: price in USD
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(10, 5))
    plt.plot(btc_df["timestamp"], btc_df["bitcoin_price"], marker="o", linestyle="-", label="Bitcoin (USD)")

    plt.title("Bitcoin Price Trend (USD) from PostgreSQL")
    plt.xlabel("Timestamp")
    plt.ylabel("Price (USD)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()


def run_plot() -> None:
    """
    Load Bitcoin data from PostgreSQL and generate a trend chart.
    The chart is saved into charts/bitcoin_price_trend.png.
    """
    project_root = Path(__file__).resolve().parents[1]
    chart_path = project_root / "charts" / "bitcoin_price_trend.png"

    print("Loading Bitcoin data from PostgreSQL (table: crypto_prices)...")
    btc_df = load_bitcoin_from_postgres()

    print("Preview of Bitcoin data:")
    print(btc_df.tail())

    print(f"Saving Bitcoin price chart to: {chart_path}")
    plot_bitcoin_trend(btc_df, chart_path)

    print("Chart generation completed successfully.")


if __name__ == "__main__":
    # Example:
    #   python scripts/plot_bitcoin_trend.py
    run_plot()