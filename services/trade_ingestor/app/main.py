import json
import logging
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [trade-ingestor] %(message)s",
)
logger = logging.getLogger(__name__)


QUERY = """
query GetOrderFilledEvents($first: Int!, $where: OrderFilledEvent_filter) {
  orderFilledEvents(first: $first, where: $where, orderBy: timestamp, orderDirection: asc) {
    id
    transactionHash
    timestamp
    orderHash
    maker
    taker
    makerAssetId
    takerAssetId
    makerAmountFilled
    takerAmountFilled
    fee
  }
}
"""


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST"),
        port=int(env("CLICKHOUSE_PORT", "8123")),
        username=env("CLICKHOUSE_USER"),
        password=env("CLICKHOUSE_PASSWORD"),
        database=env("CLICKHOUSE_DATABASE"),
    )


def ensure_clickhouse_client(client):
    if client is None:
        return get_clickhouse_client()
    try:
        client.query("SELECT 1")
        return client
    except Exception:  # noqa: BLE001
        logger.warning("reconnecting to ClickHouse")
        return get_clickhouse_client()


def get_checkpoint(client) -> dict | None:
    result = client.query(
        """
        SELECT last_event_timestamp, last_event_id
        FROM trade_ingestion_checkpoints
        WHERE source = 'goldsky_order_filled_events'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )
    if not result.result_rows:
        return None

    timestamp, event_id = result.result_rows[0]
    return {
        "last_event_timestamp": timestamp,
        "last_event_id": event_id,
    }


def fetch_order_filled_events(batch_size: int, where: dict | None) -> list[dict]:
    response = requests.post(
        env("POLYMARKET_SUBGRAPH_URL"),
        json={"query": QUERY, "variables": {"first": batch_size, "where": where}},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "errors" in payload:
        raise RuntimeError(f"GraphQL errors: {json.dumps(payload['errors'])}")
    return payload["data"]["orderFilledEvents"]


def to_datetime(timestamp: str) -> datetime:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).replace(tzinfo=None)


def persist_events(client, events: list[dict]) -> None:
    if not events:
        return

    rows = []
    now = datetime.utcnow()
    for event in events:
        rows.append(
            [
                event["id"],
                event["transactionHash"],
                to_datetime(event["timestamp"]),
                event["orderHash"],
                event["maker"],
                event["taker"],
                event["makerAssetId"],
                event["takerAssetId"],
                event["makerAmountFilled"],
                event["takerAmountFilled"],
                event["fee"],
                "goldsky",
                now,
            ]
        )

    client.insert(
        "raw_order_filled_events",
        rows,
        column_names=[
            "event_id",
            "transaction_hash",
            "event_timestamp",
            "order_hash",
            "maker",
            "taker",
            "maker_asset_id",
            "taker_asset_id",
            "maker_amount_filled",
            "taker_amount_filled",
            "fee",
            "source",
            "ingested_at",
        ],
    )


def update_checkpoint(client, events: list[dict]) -> None:
    if not events:
        return

    latest = max(events, key=lambda item: (int(item["timestamp"]), item["id"]))
    row = [
        [
            "goldsky_order_filled_events",
            to_datetime(latest["timestamp"]),
            latest["id"],
            datetime.utcnow(),
        ]
    ]
    client.insert(
        "trade_ingestion_checkpoints",
        row,
        column_names=["source", "last_event_timestamp", "last_event_id", "updated_at"],
    )


def normalize_events(events: list[dict]) -> list[dict]:
    return sorted(events, key=lambda item: (int(item["timestamp"]), item["id"]))


def filter_after_checkpoint(events: list[dict], checkpoint: dict | None) -> list[dict]:
    if checkpoint is None:
        return normalize_events(events)

    checkpoint_timestamp = int(checkpoint["last_event_timestamp"].timestamp())
    checkpoint_id = checkpoint["last_event_id"]
    filtered = [
        event
        for event in normalize_events(events)
        if (int(event["timestamp"]) > checkpoint_timestamp)
        or (
            int(event["timestamp"]) == checkpoint_timestamp
            and event["id"] > checkpoint_id
        )
    ]
    return filtered


def fetch_next_batch(batch_size: int, checkpoint: dict | None) -> list[dict]:
    if checkpoint is None:
        return normalize_events(fetch_order_filled_events(batch_size, where=None))

    checkpoint_timestamp = str(int(checkpoint["last_event_timestamp"].timestamp()))
    checkpoint_id = checkpoint["last_event_id"]

    same_timestamp_events = fetch_order_filled_events(
        batch_size,
        where={
            "timestamp": checkpoint_timestamp,
            "id_gt": checkpoint_id,
        },
    )
    same_timestamp_events = filter_after_checkpoint(same_timestamp_events, checkpoint)

    if len(same_timestamp_events) >= batch_size:
        return same_timestamp_events[:batch_size]

    newer_events = fetch_order_filled_events(
        batch_size - len(same_timestamp_events),
        where={"timestamp_gt": checkpoint_timestamp},
    )
    newer_events = filter_after_checkpoint(newer_events, checkpoint)

    merged = {
        event["id"]: event
        for event in same_timestamp_events + newer_events
    }
    return normalize_events(list(merged.values()))[:batch_size]


def main() -> None:
    batch_size = int(env("BATCH_SIZE", "100"))
    poll_interval_seconds = int(env("POLL_INTERVAL_SECONDS", "30"))
    client = None

    logger.info("trade ingestor started with batch_size=%s", batch_size)
    while True:
        try:
            client = ensure_clickhouse_client(client)
            checkpoint = get_checkpoint(client)
            events = fetch_next_batch(batch_size, checkpoint)
            persist_events(client, events)
            update_checkpoint(client, events)
            if events:
                logger.info(
                    "fetched and stored %s new orderFilledEvents ending at timestamp=%s id=%s",
                    len(events),
                    events[-1]["timestamp"],
                    events[-1]["id"],
                )
            else:
                logger.info("no new orderFilledEvents found")
        except Exception as exc:  # noqa: BLE001
            logger.exception("trade ingestion loop failed: %s", exc)

        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main()
