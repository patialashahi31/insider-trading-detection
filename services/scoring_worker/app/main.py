import logging
import os
import time
from datetime import datetime

import clickhouse_connect


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [scoring-worker] %(message)s",
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


def classify(score: float) -> str:
    if score >= 8:
        return "high"
    if score >= 4:
        return "medium"
    return "low"


def compute_scores(client) -> None:
    result = client.query(
        """
        SELECT
            wallet,
            trade_count,
            distinct_markets,
            total_volume,
            max_single_trade_volume,
            wallet_age_seconds_at_first_trade,
            has_first_funding,
            first_funding_value
        FROM wallet_features
        """
    )

    rows = []
    for (
        wallet,
        trade_count,
        distinct_markets,
        total_volume,
        max_single_trade_volume,
        wallet_age_seconds_at_first_trade,
        has_first_funding,
        first_funding_value,
    ) in result.result_rows:
        score = 0.0
        reasons = []

        if trade_count <= 3:
            score += 2
            reasons.append("few_markets_or_trades")
        if distinct_markets <= 3:
            score += 2
            reasons.append("high_trade_concentration")
        if max_single_trade_volume >= 1_000_000:
            score += 3
            reasons.append("large_single_trade")
        if has_first_funding and wallet_age_seconds_at_first_trade is not None and wallet_age_seconds_at_first_trade <= 86_400:
            score += 3
            reasons.append("newly_funded_wallet")
        if first_funding_value >= 1_000_000:
            score += 2
            reasons.append("large_initial_funding")
        if total_volume >= 5_000_000:
            score += 2
            reasons.append("high_total_volume")

        rows.append(
            [
                wallet,
                score,
                classify(score),
                ",".join(reasons) if reasons else "none",
                datetime.utcnow(),
            ]
        )

    if not rows:
        logger.info("no wallet features available for scoring")
        return

    client.insert(
        "insider_scores",
        rows,
        column_names=["wallet", "score", "risk_level", "reasons", "computed_at"],
    )
    logger.info("computed insider scores for %s wallets", len(rows))


def main() -> None:
    client = None
    poll_interval_seconds = int(env("POLL_INTERVAL_SECONDS", "120"))

    logger.info("scoring worker started")
    while True:
        try:
            client = ensure_clickhouse_client(client)
            compute_scores(client)
        except Exception as exc:  # noqa: BLE001
            logger.exception("scoring worker loop failed: %s", exc)
        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main()
