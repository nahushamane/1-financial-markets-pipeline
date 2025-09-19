# src/ingest.py

"""
1. Ingest stock data from Yahoo Finance using yfinance.
2. Fetches OHLCV data for give tickers in the arguments.
3. Saves each ticker data as a Parquet file in data/raw/.
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
import argparse
import datetime

# Directory to store raw data
DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_ticker(ticker, start=None, end=None):
    """
    Fetch historical stock data for a given ticker.
    :param ticker: Stock symbol (e.g., 'AAPL')
    :param start: Start date (YYYY-MM-DD)
    :param end: End date (YYYY-MM-DD)
    :return: DataFrame with stock data or None if empty
    """
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    if df.empty:
        print(f"No data for {ticker}")
        return None
    df = df.reset_index()
    df['Ticker'] = ticker
    return df

def save_parquet(df, ticker):
    """Save DataFrame as a Parquet file"""
    fname = DATA_DIR / f"{ticker}.parquet"
    df.to_parquet(fname, index=False)
    print("Saved:", fname)

def main(tickers, start=None, end=None):
    """Fetch and save data for each ticker"""
    for t in tickers:
        df = fetch_ticker(t, start=start, end=end)
        if df is not None:
            save_parquet(df, t)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", help="Comma separated tickers", default="AAPL, MSFT, TSLA")
    parser.add_argument("--start", help="YYYY-MM-DD", default=None)
    parser.add_argument("--end", help="YYYY-MM-DD", default=None)
    args = parser.parse_args()
    tickers = [t.strip().upper() for t in args.tickers.split(",")]
    main(tickers, start=args.start, end=args.end)