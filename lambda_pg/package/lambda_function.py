import os
import json
import ssl
import logging
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

import boto3
import pg8000

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FINNHUB_KEY = os.environ["FINNHUB_KEY"]
KINESIS_STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

BASE_URL = "https://finnhub.io/api/v1/quote"

kinesis_client = boto3.client("kinesis")


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def get_tracked_tickers():
    """
    Read active tickers from PostgreSQL.
    """
    conn = None
    cursor = None

    try:
        ssl_context = ssl.create_default_context()

        conn = pg8000.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            ssl_context=ssl_context
        )

        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker
            FROM tracked_tickers
            WHERE is_active = TRUE
            ORDER BY ticker
        """)
        rows = cursor.fetchall()

        tickers = [row[0].strip().upper() for row in rows if row and row[0]]
        return tickers

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def fetch_quote(ticker):
    """
    Fetch one ticker quote from Finnhub.
    """
    query = urlencode({
        "symbol": ticker,
        "token": FINNHUB_KEY
    })
    url = f"{BASE_URL}?{query}"

    with urlopen(url, timeout=30) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def normalize_quote(ticker, raw):
    """
    Convert Finnhub response into your standard event schema.
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
        "event_time": event_time,
        "ticker": ticker,
        "price": float(raw["c"]) if raw.get("c") is not None else None,
        "bid": None,
        "ask": None,
        "volume": None,
        "source": "finnhub",
        "ingest_time": iso_now()
    }


def build_kinesis_record(event):
    """
    Convert event dict into Kinesis PutRecords format.
    """
    return {
        "Data": (json.dumps(event) + "\n").encode("utf-8"),
        "PartitionKey": event["ticker"]
    }


def lambda_handler(event, context):
    logger.info("Lambda invocation started")

    try:
        tickers = get_tracked_tickers()
        logger.info("Tickers from PostgreSQL: %s", tickers)
    except Exception as e:
        logger.exception("Failed to read tickers from PostgreSQL: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps("Failed to read tickers from PostgreSQL")
        }

    if not tickers:
        logger.warning("No active tickers found")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No active tickers found",
                "records_attempted": 0,
                "failed_record_count": 0
            })
        }

    records = []

    for ticker in tickers:
        try:
            raw = fetch_quote(ticker)
            normalized = normalize_quote(ticker, raw)
            records.append(build_kinesis_record(normalized))
            logger.info("Prepared event for %s", ticker)
        except HTTPError as e:
            logger.exception("HTTP error for %s: %s", ticker, e)
        except URLError as e:
            logger.exception("URL error for %s: %s", ticker, e)
        except Exception as e:
            logger.exception("Unexpected error for %s: %s", ticker, e)

    if not records:
        logger.warning("No records prepared; nothing sent to Kinesis")
        return {
            "statusCode": 500,
            "body": json.dumps("No records prepared")
        }

    response = kinesis_client.put_records(
        StreamName=KINESIS_STREAM_NAME,
        Records=records
    )

    logger.info("PutRecords response: %s", response)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Lambda run complete",
            "tickers_read_from_db": len(tickers),
            "records_attempted": len(records),
            "failed_record_count": response.get("FailedRecordCount", 0)
        })
    }
