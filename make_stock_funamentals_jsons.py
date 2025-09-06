import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

from libs.logging_utils import get_logger, Logger

load_dotenv('env/.env')
API_KEY=os.getenv('API_KEY')
SLEEP_SECONDS=float(os.getenv('SLEEP_SECONDS'))
LOG_LEVEL = os.getenv('LOG_LEVEL')
LOG_MSG_FORMAT =os.getenv('LOG_MSG_FORMAT')
LOG_DATE_FORMAT =os.getenv('LOG_DATE_FORMAT')
LOGS_PATH = os.getenv('LOGS_PATH')
LOG_FILENAME = os.getenv('LOG_FILENAME')

time_prefix = datetime.now().strftime('%Y-%m-%d')
os.makedirs(LOGS_PATH, exist_ok=True)
LOG_FILENAME = os.path.join(LOGS_PATH, time_prefix + '_' + LOG_FILENAME)
logger = get_logger(LOG_LEVEL, LOG_MSG_FORMAT, LOG_DATE_FORMAT, LOG_FILENAME)



OUTPUT_DIR=os.getenv('OUTPUT_DIR')
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_financials(logger: Logger, ticker: list[str], statement_type: str) -> None:
    url = f"https://financialmodelingprep.com/api/v3/{statement_type}/{ticker}?apikey={API_KEY}&period=annual"
    logger.debug(url)
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
            download_financials(logger ,ticker, statement)

    print("All annual financials downloaded.")
