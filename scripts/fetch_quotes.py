import os
import json
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"
WATCHLIST_FILE = "config/watchlist.txt"
OUTPUT_FILE = "data/bronze_quotes.jsonl"
POLL_INTERVAL_SECONDS = 30
RUN_MINUTES = 3


def iso_now():
    """
    Generate the current UTC timestamp in ISO 8601 format.

    Returns
    -------
    str
        Current timestamp in ISO 8601 format with timezone information.
    """
    return datetime.now(timezone.utc).isoformat()


def load_watchlist(file_path):
    """
    Load stock tickers from a watchlist file.

    The watchlist file should contain one ticker per line.

    Parameters
    ----------
    file_path : str
        Path to the watchlist text file.

    Returns
    -------
    list
        List of ticker symbols in uppercase.
    """
    with open(file_path, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


def fetch_quote(ticker):
    """
    Fetch the latest stock quote for a given ticker from Finnhub API.

    Parameters
    ----------
    ticker : str
        Stock ticker symbol (e.g., AAPL, MSFT).

    Returns
    -------
    dict
        Raw JSON response from Finnhub containing quote data.

    Raises
    ------
    HTTPError
        If the API request fails or returns a non-200 response.
    """
    params = {"symbol": ticker, "token": API_KEY}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def normalize_quote(ticker, raw):
    """
    Normalize the raw Finnhub API response into the Bronze event schema.

    This function transforms the Finnhub quote response into a consistent
    event format used by the ingestion pipeline.

    Parameters
    ----------
    ticker : str
        Stock ticker symbol.
    raw : dict
        Raw JSON response returned by Finnhub.

    Returns
    -------
    dict
        Normalized event record matching the Bronze schema.
    """
    quote_timestamp = raw.get("t")

    if quote_timestamp:
        event_time = datetime.fromtimestamp(
            quote_timestamp, tz=timezone.utc
        ).isoformat()
    else:
        event_time = iso_now()

    return {
        "event_time": event_time,
        "ticker": ticker,
        "price": float(raw["c"]) if raw.get("c") is not None else None,
        "bid": None,
        "ask": None,
        "volume": None,
        "source": "finnhub",
        "ingest_time": iso_now()
    }


def append_jsonl(file_path, record):
    """
    Append a single event record to a JSON Lines (JSONL) file.

    JSONL format stores one JSON object per line and is commonly used
    for streaming data pipelines.

    Parameters
    ----------
    file_path : str
        Path to the output JSONL file.
    record : dict
        Event record to append.
    """
    with open(file_path, "a") as f:
        f.write(json.dumps(record) + "\n")


def main():
    """
    Main pipeline execution function.

    This function orchestrates the real-time data ingestion process by:
    1. Loading the ticker watchlist
    2. Fetching quotes from Finnhub API
    3. Normalizing data into the Bronze schema
    4. Writing events to a JSONL file

    The script runs for a configured duration and polls the API
    at a fixed interval.
    """
    if not API_KEY:
        raise ValueError("FINNHUB_API_KEY is missing in .env")

    tickers = load_watchlist(WATCHLIST_FILE)
    print(f"Loaded watchlist: {tickers}")

    total_iterations = int((RUN_MINUTES * 60) / POLL_INTERVAL_SECONDS)

    for i in range(total_iterations):
        print(f"\nPolling cycle {i + 1}/{total_iterations} at {iso_now()}")

        for ticker in tickers:
            try:
                raw = fetch_quote(ticker)
                record = normalize_quote(ticker, raw)

                print(json.dumps(record, indent=2))
                append_jsonl(OUTPUT_FILE, record)

            except requests.HTTPError as http_err:
                print(f"HTTP error for {ticker}: {http_err}")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

        if i < total_iterations - 1:
            time.sleep(POLL_INTERVAL_SECONDS)

    print(f"\nDone. Data written to {OUTPUT_FILE}")


if __name__ == "__main__":
    """
    Entry point of the script.

    Ensures that the main() function runs only when the script
    is executed directly and not when it is imported as a module.
    """
    main()
