import pandas as pd
import glob
import json
import yfinance as yf


def get_stock_data(tickers: list[str], start: str = "2010-01-01", end: str = None) -> pd.DataFrame:
    """
    Retrieves stock price and volume from yfinance.
    Args:
        tickers:list of tickers
        start:starting date from which retrieves data for. Format YYYY-MM-DD.
        end:latest date for which retrieves data. Format YYYY-MM-DD.
    Returns:
        Dataframe with extracted data.

    """
    df = yf.download(tickers, start=start, end=end, auto_adjust=True)
    df = df.reset_index()
    df.columns = ['Date'] + [f"{col[0]}_{col[1]}" for col in df.columns[1:]]
    df_long = df.melt(id_vars=["Date"], var_name="Feature_Ticker", value_name="Value")
    df_long[["Feature", "Ticker"]] = df_long["Feature_Ticker"].str.split("_", expand=True)
    df_long = df_long.drop(columns=["Feature_Ticker"])
    df_clean = df_long.pivot_table(index=["Date", "Ticker"],
                                   columns="Feature",
                                   values="Value").reset_index()
    df_clean = df_clean.dropna()
    return df_clean

# -------------------------
# 2) Load fundamentals from JSONs
# -------------------------
def load_fundamentals(json_folder, columns_needed):
    all_data = []
    for file in glob.glob(f"{json_folder}/*.json"):
        with open(file) as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df = df.rename(columns={'symbol': 'Ticker', 'ticker': 'Ticker', 'date': 'Date'})
        df['Date'] = pd.to_datetime(df['Date'])
        cols_to_keep = [c for c in columns_needed if c in df.columns]
        df = df[cols_to_keep]
        all_data.append(df)
    fundamentals_df = pd.concat(all_data, ignore_index=True).drop_duplicates(subset=['Ticker', 'Date'])
    return fundamentals_df



df=get_stock_data(['AAPL', 'TSLA'])
print(df.shape)
print(df.info())
print(df.columns.to_list())
