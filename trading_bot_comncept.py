import pandas as pd
import yfinance as yf
import glob
import json
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import os
from dotenv import load_dotenv

from libs.data_loader import get_stock_data, load_fundamentals
from libs.bull_detector import detect_and_label_bull_runs
from libs.bull_detector import summarize_bull_durations
from libs.bull_detector import summarize_last_bull_runs
from libs.stock_selector import calculate_ev_on_bull_runs
from libs.logging_utils import get_logger, Logger

load_dotenv('env/.env')


time_prefix = datetime.now().strftime('%Y-%m-%d')
os.makedirs(LOGS_PATH, exist_ok=True)
LOG_FILENAME = os.path.join(LOGS_PATH, time_prefix + '_' + LOG_FILENAME)
logger = get_logger(LOG_LEVEL, LOG_MSG_FORMAT, LOG_DATE_FORMAT, LOG_FILENAME)






if __name__ == "__main__":
    # Universe
    upstream_oil_gas_tickers = ["XOM", "CVX", "BP", "TTE", "ENIC", "PBR"]
    beverage_tickers = ["KO", "PEP", "MNST", "KDP"]
    green_energy_tickers = ["TSLA", "NEE", "ENPH", "SEDG", "BE"]
    existing_tickers = ["AAPL", "MSFT", "NVDA", "JPM", "GS", "BAC", "JNJ", "PFE",
                        "DAL", "GLD", "TLT", "XLP", "XLE", "SPY"]
    tickers = existing_tickers + upstream_oil_gas_tickers + beverage_tickers + green_energy_tickers

    # Params
    stop_loss_pct = 0.02
    take_profits = [0.04, 0.08, 0.10]
    lookahead_days = 5
    trend_window = 60
    slope_threshold_ppd = 0.001       # ~0.05% per day slope on log-price
    min_bull_duration_days = 7
    cost_per_trade = 0.001
    json_folder = r"C:\Users\HP\PycharmProjects\Trading_Bot\fundamentals_jsons"
    columns_needed = ['Ticker', 'Date', 'netIncome', 'operatingCashFlow', 'freeCashFlow', 'capitalExpenditure']

    # 1) Prices

    df_clean = get_stock_data(tickers , start="2020-01-01")

    # 2) Fundamentals (optional downstream usage)
    fundamentals_df = load_fundamentals(json_folder, columns_needed)
    latest_prices = df_clean.groupby('Ticker').last().reset_index()
    numeric_df = pd.merge(
        latest_prices,
        fundamentals_df.groupby('Ticker').last().reset_index(),
        on='Ticker', how='inner'
    )
    # Align tickers to those with fundamentals (if you want to restrict universe)
    tickers = numeric_df['Ticker'].values

    # simple scale (kept from your original; not directly used in EV)
    numeric_features = numeric_df.select_dtypes(include=['float64', 'int64'])
    if not numeric_features.empty:
        _ = StandardScaler().fit_transform(numeric_features)

    # 3) Detect & label bull runs
    df_clean, runs_df = detect_and_label_bull_runs(
        df_clean, tickers,
        trend_window=trend_window,
        slope_threshold_ppd=slope_threshold_ppd,
        min_bull_duration_days=min_bull_duration_days,
        use_log=True
    )

    # 4) Summaries
    bull_stats = summarize_bull_durations(runs_df)  # Avg/Median per ticker
    last_bull_df = summarize_last_bull_runs(df_clean, runs_df)

    print("\n=== Last Qualified Bull Run per Ticker ===")
    print(last_bull_df)

    # 5) EV during qualified bull runs
    ev_df = calculate_ev_on_bull_runs(
        df_clean, tickers, bull_stats, take_profits,
        lookahead_days=lookahead_days,
        stop_loss_pct=stop_loss_pct,
        cost_per_trade=cost_per_trade
    )
    merged_df = pd.merge(
        ev_df,
        last_bull_df[['Ticker', 'DaysSinceLastBull']],
        on='Ticker',
        how='left'
    )

    # Filter by recency: only stocks whose last bull ended within 60 days
    recency_threshold = 60
    filtered_df = merged_df[merged_df['DaysSinceLastBull'] <= recency_threshold]

    # Sort by EV_with_costs descending
    filtered_df = filtered_df.sort_values('EV_with_costs', ascending=False).reset_index(drop=True)

    best_tp_df = filtered_df.loc[filtered_df.groupby('Ticker')['EV_with_costs'].idxmax()].reset_index(drop=True)

    print("\n=== Best TP per Ticker ===")
    print(best_tp_df[['Ticker', 'TakeProfit', 'EV_with_costs', 'DaysSinceLastBull', 'MedianBullDuration']])


