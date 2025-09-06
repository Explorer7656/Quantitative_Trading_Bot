import pandas as pd
from libs.data_loader import get_stock_data
from libs.bull_detector import detect_and_label_bull_runs


class SignalGenerator:
    def __init__(self, tickers: list[str], start="2020-01-01", end=None,
                 trend_window=60, slope_threshold_ppd=0.001, min_bull_duration_days=7):
        self.tickers = tickers
        self.start = start
        self.end = end
        self.data = None
        self.trend_window = trend_window
        self.slope_threshold_ppd = slope_threshold_ppd
        self.min_bull_duration_days = min_bull_duration_days

    def load_data(self):
        """Fetch stock data and label bull runs."""
        self.data = get_stock_data(self.tickers, start=self.start, end=self.end)
        self.data, _ = detect_and_label_bull_runs(
            self.data, self.tickers,
            trend_window=self.trend_window,
            slope_threshold_ppd=self.slope_threshold_ppd,
            min_bull_duration_days=self.min_bull_duration_days
        )
        return self.data

    def moving_average_crossover(self, ticker: str, short_window=20, long_window=50) -> pd.DataFrame:
        """Generate SMA signals only when in bull run."""
        df = self.data[self.data["Ticker"] == ticker].copy()
        df = df.sort_values("Date")

        df["SMA_short"] = df["Close"].rolling(window=short_window).mean()
        df["SMA_long"] = df["Close"].rolling(window=long_window).mean()

        df["signal"] = 0
        # Only generate signals if the stock is currently in a bull run
        df.loc[(df["SMA_short"] > df["SMA_long"]) & (df["InBullRun"]), "signal"] = 1  # Buy
        df.loc[(df["SMA_short"] < df["SMA_long"]) & (df["InBullRun"]), "signal"] = -1  # Sell

        return df[["Date", "Ticker", "Close", "SMA_short", "SMA_long", "InBullRun", "signal"]]

    def get_latest_signal(self, ticker: str) -> int:
        """Return the latest signal for a ticker."""
        df = self.moving_average_crossover(ticker)
        latest_signal = df["signal"].iloc[-1]
        return int(latest_signal)