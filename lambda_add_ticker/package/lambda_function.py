import os
import json
import ssl
import logging

import pg8000

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]


def get_request_body(event):
    """
    Read and parse the JSON body from API Gateway event.
    """
    body = event.get("body")

    if not body:
        return {}

    if isinstance(body, str):
        return json.loads(body)

    return body


def insert_ticker(ticker):
    """
    Insert or reactivate a ticker in tracked_tickers.
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
            INSERT INTO tracked_tickers (ticker, is_active)
            VALUES (%s, TRUE)
            ON CONFLICT (ticker)
            DO UPDATE SET is_active = TRUE
        """, (ticker,))

        conn.commit()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def lambda_handler(event, context):
    """
    API Gateway Lambda handler for adding a ticker to tracking.
    """
    logger.info("Received event: %s", event)

    try:
        data = get_request_body(event)
        ticker = data.get("ticker", "").strip().upper()

        if not ticker:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "message": "ticker is required"
                })
            }

        insert_ticker(ticker)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Ticker added for tracking",
                "ticker": ticker
            })
        }

    except Exception as e:
        logger.exception("Failed to add ticker: %s", e)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Internal server error"
            })
        }
