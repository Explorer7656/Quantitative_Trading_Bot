import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler


def detect_and_label_bull_runs(df, tickers, trend_window=20, slope_threshold_ppd=0.001,
                               min_bull_duration_days=3, use_log=True):
    df = df.copy()
    df['Slope'] = np.nan
    df['InBullRun'] = False
    runs_summary = []

    for ticker in tickers:
        tdf = df[df['Ticker'] == ticker].sort_values('Date').copy()
        n = len(tdf)
        if n == 0:
            continue

        prices = tdf['Close'].values
        series = np.log(prices) if use_log else prices

        # Rolling slope on the trailing window
        slope_arr = np.full(n, np.nan)
        flag_arr = np.zeros(n, dtype=bool)

        X = np.arange(trend_window).reshape(-1, 1)
        lr = LinearRegression()

        for i in range(trend_window, n):
            y = series[i - trend_window:i]
            lr.fit(X, y)
            slope = lr.coef_[0]
            slope_arr[i] = slope
            flag_arr[i] = (slope >= slope_threshold_ppd)

        # Qualify only runs whose length >= min_bull_duration_days
        qualified_mask = np.zeros(n, dtype=bool)
        in_run = False
        run_start = None

        for i in range(n):
            if flag_arr[i] and not in_run:
                in_run = True
                run_start = i
            # run ends when next is False or we've reached the last index
            end_condition = (not flag_arr[i]) or (i == n - 1)
            if in_run and end_condition:
                run_end = i if flag_arr[i] else i - 1
                length = run_end - run_start + 1
                if length >= min_bull_duration_days:
                    qualified_mask[run_start:run_end + 1] = True
                    avg_slope = np.nanmean(slope_arr[run_start:run_end + 1])
                    runs_summary.append({
                        'Ticker': ticker,
                        'Start': tdf.iloc[run_start]['Date'],
                        'End': tdf.iloc[run_end]['Date'],
                        'Length': length,
                        'AvgSlope': avg_slope
                    })
                in_run = False
                run_start = None

        # write back into the main df using original indices
        df.loc[tdf.index, 'Slope'] = slope_arr
        df.loc[tdf.index, 'InBullRun'] = qualified_mask

    runs_df = pd.DataFrame(runs_summary)
    return df, runs_df


def summarize_bull_durations(runs_df):
    if runs_df.empty:
        return pd.DataFrame(columns=['Ticker', 'AvgBullDuration', 'MedianBullDuration'])
    g = runs_df.groupby('Ticker')['Length']
    return pd.DataFrame({
        'AvgBullDuration': g.mean(),
        'MedianBullDuration': g.median()
    }).reset_index()


def summarize_last_bull_runs(df, runs_df):
    # use dataset end as reference (not "today")
    dataset_last_date = df['Date'].max()
    rows = []
    tickers = df['Ticker'].unique()
    for ticker in tickers:
        r = runs_df[runs_df['Ticker'] == ticker]
        if r.empty:
            continue
        last_row = r.sort_values('End').iloc[-1]
        last_start = last_row['Start']
        last_end = last_row['End']
        days_since = (dataset_last_date - last_end).days
        rows.append({
            'Ticker': ticker,
            'LastBullStart': last_start,
            'LastBullEnd': last_end,
            'DaysSinceLastBull': days_since
        })
    return pd.DataFrame(rows).sort_values('DaysSinceLastBull')
