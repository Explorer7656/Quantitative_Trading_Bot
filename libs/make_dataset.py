import pandas as pd
import numpy as np
import os

from libs.data_loader import get_stock_data  # your loader
from libs.bull_detector import detect_and_label_bull_runs  # your bull detector

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def generate_labels(close_prices, sl, tp, max_holding):
    n = len(close_prices)
    labels = np.full(n, -1)  # default undecided

    for i in range(n - 1):
        entry = close_prices[i]
        future_prices = close_prices[i + 1 : i + 1 + max_holding]

        if len(future_prices) == 0:
            break

        tp_hit = np.where(future_prices >= entry * (1 + tp))[0]
        sl_hit = np.where(future_prices <= entry * (1 - sl))[0]

        if tp_hit.size > 0 and (sl_hit.size == 0 or tp_hit[0] < sl_hit[0]):
            labels[i] = 1
        elif sl_hit.size > 0 and (tp_hit.size == 0 or sl_hit[0] < tp_hit[0]):
            labels[i] = 0

    return labels

def make_dataset(sl=0.03, tp=0.05, max_holding=20, tickers=None,
                 start="2015-01-01", end="2025-01-01",
                 outdir=r"C:\Users\HP\PycharmProjects\Trading_Bot\libs\datasets"):
    """
    Create trade dataset with features + labels.
    """

    if tickers is None:
        tickers = ["AAPL"]

    # --- Load raw OHLCV data ---
    df = get_stock_data(tickers, start=start, end=end)

    # --- Add bull regime column ---
    df_clean, runs_df = detect_and_label_bull_runs(
        df, tickers,
        trend_window=30,
        slope_threshold_ppd=0.0001,
        min_bull_duration_days=10,
        use_log=True
    )

    # --- Sort for consistency ---
    df_clean = df_clean.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    # --- Compute features ---
    df_clean["SMA_20"] = df_clean.groupby("Ticker")["Close"].transform(lambda x: x.rolling(20).mean())
    df_clean["SMA_50"] = df_clean.groupby("Ticker")["Close"].transform(lambda x: x.rolling(50).mean())
    df_clean["RSI_14"] = df_clean.groupby("Ticker")["Close"].transform(lambda x: compute_rsi(x, 14))
    # TODO: Add more indicators here later

    # --- Generate labels efficiently per ticker ---
    labels_all = []
    for ticker, df_ticker in df_clean.groupby("Ticker"):
        close_arr = df_ticker["Close"].values
        labels = generate_labels(close_arr, sl, tp, max_holding)
        df_ticker["Label"] = labels
        labels_all.append(df_ticker)

    df_clean = pd.concat(labels_all, ignore_index=True)

    # --- Save dataset ---
    os.makedirs(outdir, exist_ok=True)
    fname = f"train_sl_{sl}_tp_{tp}_mh_{max_holding}.csv"
    path = os.path.join(outdir, fname)
    df_clean.to_csv(path, index=False)
    print(f"[+] Dataset saved to {path}")

    return df_clean

# Example usage:
if __name__ == "__main__":
    top100_sp500 = [
        "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AIG", "AMD", "AMGN", "AMT",
        "AMZN", "AVGO", "AXP", "BA", "BAC", "BK", "BKNG", "BLK", "BMY",
        "C", "CAT", "CHTR", "CL", "CMCSA", "COF", "COP", "COST",
        "CRM", "CSCO", "CVS", "CVX", "DE", "DHR", "DIS", "DUK", "EMR",
        "FDX", "GD", "GE", "GILD", "GM", "GOOG", "GOOGL", "GS", "HD",
        "HON", "IBM", "INTC", "INTU", "ISRG", "JNJ", "JPM", "KO", "LIN",
        "LLY", "LMT", "LOW", "MA", "MCD", "MDLZ", "MDT", "MET", "META",
        "MMM", "MO", "MRK", "MS", "MSFT", "NEE", "NFLX", "NKE", "NOW",
        "NVDA", "ORCL", "PEP", "PFE", "PG", "PLTR", "PM", "PYPL", "QCOM",
        "RTX", "SBUX", "SCHW", "SO", "SPG", "T", "TGT", "TMO", "TMUS",
        "TSLA", "TXN", "UNH", "UNP", "UPS", "USB", "V", "VZ", "WFC",
        "WMT", "XOM"
    ]

    make_dataset(sl=0.03, tp=0.05, max_holding=20, tickers=top100_sp500)
