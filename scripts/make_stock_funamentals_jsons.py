import requests
import json
import os
import time
from logger import Logger


if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

API_KEY = "WxPYF2obhO2X7s9Bcem8B1KW4l6FxH1i"  # Replace with your FMP free API key
SLEEP_SECONDS = 1.5  # Sleep between requests to avoid rate limiting
OUTPUT_DIR = "fundamentals_jsons"

def download_financials(logger: Logger, ticker: list[str], statement_type: str) -> None:
    url = f"https://financialmodelingprep.com/api/v3/{statement_type}/{ticker}?apikey={API_KEY}&period=annual"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:  # Only save if data exists
                filename = f"{OUTPUT_DIR}/{ticker}_{statement_type}.json"
                with open(filename, "w") as f:
                    json.dump(data, f, indent=4)
                logger.info(f"[DOWNLOAD] Saved {ticker} ({statement_type})")
            else:
                logger.error(f"[INFO] No data for {ticker} ({statement_type})")
        else:
            logger.error(f"[ERROR] Failed to download {ticker} ({statement_type}): {response.status_code}")
    except Exception as e:

        logger.error(f"[ERROR] Exception for {ticker} ({statement_type}): {e}")
    finally:
        time.sleep(SLEEP_SECONDS)


# =========================
# MAIN LOOP
# =========================

if __name__ == '__main__':
    tickers = [
        "XOM", "CVX", "BP", "TTE", "E", "PBR",
        "KO", "PEP", "MNST", "KDP",
        "TSLA", "NEE", "ENPH", "SEDG", "BE",
        "AAPL", "MSFT", "NVDA", "JPM", "GS", "BAC",
        "JNJ", "PFE", "DAL", "GLD", "TLT", "XLP", "XLE", "SPY"
    ]

    financial_types = ["balance-sheet-statement", "income-statement", "cash-flow-statement"]

    for ticker in tickers:
        for statement in financial_types:
            download_financials(ticker, statement)

    print("All annual financials downloaded.")
