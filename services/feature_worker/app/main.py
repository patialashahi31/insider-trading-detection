import logging
import os
import time
from datetime import datetime

import clickhouse_connect


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [feature-worker] %(message)s",
)
logger = logging.getLogger(__name__)


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


def compute_features(client) -> None:
    result = client.query(
        """
        WITH wallet_trades AS (
            SELECT maker AS wallet, event_timestamp, taker_amount_filled AS notional, taker_asset_id AS market_id
            FROM raw_order_filled_events
            UNION ALL
            SELECT taker AS wallet, event_timestamp, maker_amount_filled AS notional, maker_asset_id AS market_id
            FROM raw_order_filled_events
        ),
        trade_stats AS (
            SELECT
                wallet,
                min(event_timestamp) AS first_trade_timestamp,
                max(event_timestamp) AS latest_trade_timestamp,
                count() AS trade_count,
                uniqExact(market_id) AS distinct_markets,
                sum(toFloat64OrZero(notional)) AS total_volume,
                max(toFloat64OrZero(notional)) AS max_single_trade_volume
            FROM wallet_trades
            GROUP BY wallet
        )
        SELECT
            t.wallet,
            t.first_trade_timestamp,
            t.latest_trade_timestamp,
            t.trade_count,
            t.distinct_markets,
            t.total_volume,
            t.max_single_trade_volume,
            if(
                isNull(f.first_funding_timestamp),
                NULL,
                dateDiff('second', f.first_funding_timestamp, t.first_trade_timestamp)
            ) AS wallet_age_seconds_at_first_trade,
            if(isNull(f.wallet), 0, 1) AS has_first_funding,
            f.first_funding_timestamp,
            toFloat64OrZero(f.value) AS first_funding_value
        FROM trade_stats t
        LEFT JOIN wallet_first_funding f ON t.wallet = f.wallet
        """
    )

    rows = []
    now = datetime.utcnow()
    for row in result.result_rows:
        rows.append([*row, now])

    if not rows:
        logger.info("no wallet features to compute yet")
        return

    client.insert(
        "wallet_features",
        rows,
        column_names=[
            "wallet",
            "first_trade_timestamp",
            "latest_trade_timestamp",
            "trade_count",
            "distinct_markets",
            "total_volume",
            "max_single_trade_volume",
            "wallet_age_seconds_at_first_trade",
            "has_first_funding",
            "first_funding_timestamp",
            "first_funding_value",
            "updated_at",
        ],
    )
    logger.info("upserted features for %s wallets", len(rows))


def main() -> None:
    client = None
    poll_interval_seconds = int(env("POLL_INTERVAL_SECONDS", "120"))

    logger.info("feature worker started")
    while True:
        try:
            client = ensure_clickhouse_client(client)
            compute_features(client)
        except Exception as exc:  # noqa: BLE001
            logger.exception("feature worker loop failed: %s", exc)
        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main()
