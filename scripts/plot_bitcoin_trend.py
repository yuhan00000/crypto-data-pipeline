from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def load_bitcoin_data(csv_path: Path) -> pd.DataFrame:
    """
    Load the CSV file containing crypto prices and filter for Bitcoin only.

    - Parses the timestamp column as datetime
    - Sorts the records chronologically
    - Returns a DataFrame with at least: timestamp, coin, price_usd
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found at {csv_path}. "
            "Run the ETL script first: python scripts/etl_crypto_prices.py"
        )

    df = pd.read_csv(csv_path)

    if "coin" not in df.columns or "timestamp" not in df.columns or "price_usd" not in df.columns:
        raise ValueError("CSV file does not have the expected columns: timestamp, coin, price_usd.")

    # Keep only Bitcoin rows.
    btc_df = df[df["coin"] == "bitcoin"].copy()

    if btc_df.empty:
        raise ValueError("No Bitcoin data found in the CSV. Make sure the ETL script is configured correctly.")

    # Convert timestamp strings into datetime objects for plotting.
    btc_df["timestamp"] = pd.to_datetime(btc_df["timestamp"], utc=True, errors="coerce")
    btc_df = btc_df.sort_values("timestamp")

    return btc_df


def plot_bitcoin_trend(btc_df: pd.DataFrame, output_path: Path) -> None:
    """
    Plot the Bitcoin price trend over time and save it as a PNG file.

    - X-axis: timestamp
    - Y-axis: price in USD
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(10, 5))
    plt.plot(btc_df["timestamp"], btc_df["price_usd"], marker="o", linestyle="-", label="Bitcoin (USD)")

    plt.title("Bitcoin Price Trend (USD)")
    plt.xlabel("Timestamp")
    plt.ylabel("Price (USD)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()


def run_plot() -> None:
    """
    Load Bitcoin data from the CSV file and generate a trend chart.
    The chart is saved into charts/bitcoin_price_trend.png.
    """
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "data" / "crypto_prices.csv"
    chart_path = project_root / "charts" / "bitcoin_price_trend.png"

    print(f"Loading Bitcoin data from: {csv_path}")
    btc_df = load_bitcoin_data(csv_path)

    print("Preview of Bitcoin data:")
    print(btc_df.tail())

    print(f"Saving Bitcoin price chart to: {chart_path}")
    plot_bitcoin_trend(btc_df, chart_path)

    print("Chart generation completed successfully.")


if __name__ == "__main__":
    # Allow the script to be run directly from the command line.
    # Example:
    #   python scripts/plot_bitcoin_trend.py
    run_plot()

