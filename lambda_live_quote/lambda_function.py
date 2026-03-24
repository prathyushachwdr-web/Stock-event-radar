import os
import json
import ssl
import logging
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

import pg8000

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FINNHUB_KEY = os.environ["FINNHUB_KEY"]
BASE_URL = "https://finnhub.io/api/v1/quote"

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def get_ticker_from_event(event):
    """
    Read ticker from API Gateway query string.
    Example: GET /quote?ticker=AMD
    """
    query_params = event.get("queryStringParameters") or {}
    return query_params.get("ticker", "").strip().upper()


def get_db_connection():
    """
    Create PostgreSQL connection.
    """
    ssl_context = ssl.create_default_context()

    return pg8000.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        ssl_context=ssl_context
    )


def ensure_ticker_is_tracked(ticker):
    """
    Check if ticker exists in tracked_tickers.
    If not, insert it.
    Return tracking status.
    """
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT ticker
            FROM tracked_tickers
            WHERE ticker = %s
              AND is_active = TRUE
        """, (ticker,))
        row = cursor.fetchone()

        if row:
            return "already_tracked"

        cursor.execute("""
            INSERT INTO tracked_tickers (ticker, is_active)
            VALUES (%s, TRUE)
            ON CONFLICT (ticker)
            DO UPDATE SET is_active = TRUE
        """, (ticker,))
        conn.commit()

        return "added_to_watchlist"

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def fetch_quote(ticker):
    """
    Call Finnhub quote API.
    """
    query = urlencode({
        "symbol": ticker,
        "token": FINNHUB_KEY
    })
    url = f"{BASE_URL}?{query}"

    with urlopen(url, timeout=30) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def normalize_quote(ticker, raw, tracking_status):
    """
    Convert Finnhub response into clean API response.
    """
    quote_timestamp = raw.get("t")

    if quote_timestamp:
        event_time = datetime.fromtimestamp(
            quote_timestamp,
            tz=timezone.utc
        ).isoformat()
    else:
        event_time = iso_now()

    return {
        "ticker": ticker,
        "price": float(raw["c"]) if raw.get("c") is not None else None,
        "event_time": event_time,
        "source": "finnhub",
        "tracking_status": tracking_status
    }


def lambda_handler(event, context):
    """
    Main handler:
    - read ticker from request
    - ensure ticker is tracked
    - fetch live quote
    - return response
    """
    logger.info("Received event: %s", event)

    try:
        ticker = get_ticker_from_event(event)

        if not ticker:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "message": "ticker query parameter is required"
                })
            }

        tracking_status = ensure_ticker_is_tracked(ticker)
        raw = fetch_quote(ticker)
        result = normalize_quote(ticker, raw, tracking_status)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(result)
        }

    except HTTPError as e:
        logger.exception("HTTP error while fetching ticker: %s", e)
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Failed to fetch quote from Finnhub"
            })
        }

    except URLError as e:
        logger.exception("URL error while fetching ticker: %s", e)
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Network error while fetching quote"
            })
        }

    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Internal server error"
            })
        }
