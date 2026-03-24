import json
from datetime import datetime, timezone
import boto3

STREAM_NAME = "stock-events-stream"

client = boto3.client("kinesis", region_name="us-east-1")

def create_event(ticker, price):
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "price": price,
        "bid": None,
        "ask": None,
        "volume": None,
        "source": "finnhub",
        "ingest_time": datetime.now(timezone.utc).isoformat()
    }

events = [
    create_event("AAPL", 215.12),
    create_event("MSFT", 412.45),
    create_event("NVDA", 901.33)
]

for event in events:
    response = client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event) + "\n",
        PartitionKey=event["ticker"]
    )
    print(f"Sent {event['ticker']} → Seq: {response['SequenceNumber']}")
