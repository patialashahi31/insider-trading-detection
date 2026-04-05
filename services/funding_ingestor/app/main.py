import logging
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [funding-ingestor] %(message)s",
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


def get_candidate_wallets(client, batch_size: int) -> list[str]:
    result = client.query(
        f"""
        WITH wallets AS (
            SELECT maker AS wallet FROM raw_order_filled_events
            UNION DISTINCT
            SELECT taker AS wallet FROM raw_order_filled_events
        )
        SELECT wallet
        FROM wallets
        WHERE wallet NOT IN (
            SELECT wallet FROM wallet_first_funding
        )
        ORDER BY wallet ASC
        LIMIT {batch_size}
        """
    )
    return [row[0] for row in result.result_rows]


def fetch_first_usdc_transfer(wallet: str) -> dict | None:
    api_key = env("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.warning("ETHERSCAN_API_KEY is not set; funding ingestion is idle")
        return None

    response = requests.get(
        env("ETHERSCAN_BASE_URL"),
        params={
            "chainid": env("POLYGON_CHAIN_ID", "137"),
            "module": "account",
            "action": "tokentx",
            "contractaddress": env("USDC_E_CONTRACT"),
            "address": wallet,
            "page": 1,
            "offset": 1,
            "sort": "asc",
            "apikey": api_key,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("status") == "0":
        if payload.get("message") in {"No transactions found", "No records found"}:
            return None
        raise RuntimeError(f"Etherscan error for {wallet}: {payload}")

    results = payload.get("result", [])
    if not results:
        return None
    return results[0]


def parse_timestamp(value: str) -> datetime:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).replace(tzinfo=None)


def persist_transfer(client, wallet: str, transfer: dict) -> None:
    raw_row = [[
        wallet,
        transfer["hash"],
        int(transfer["blockNumber"]),
        parse_timestamp(transfer["timeStamp"]),
        transfer["from"],
        transfer["to"],
        transfer["value"],
        int(transfer["tokenDecimal"]),
        "etherscan",
        datetime.utcnow(),
    ]]
    client.insert(
        "raw_usdc_transfers",
        raw_row,
        column_names=[
            "wallet",
            "transaction_hash",
            "block_number",
            "event_timestamp",
            "from_address",
            "to_address",
            "value",
            "token_decimal",
            "source",
            "ingested_at",
        ],
    )

    first_funding_row = [[
        wallet,
        transfer["hash"],
        int(transfer["blockNumber"]),
        parse_timestamp(transfer["timeStamp"]),
        transfer["from"],
        transfer["to"],
        transfer["value"],
        "etherscan",
        datetime.utcnow(),
    ]]
    client.insert(
        "wallet_first_funding",
        first_funding_row,
        column_names=[
            "wallet",
            "first_funding_transaction_hash",
            "first_funding_block_number",
            "first_funding_timestamp",
            "from_address",
            "to_address",
            "value",
            "source",
            "updated_at",
        ],
    )


def main() -> None:
    client = None
    wallet_batch_size = int(env("WALLET_BATCH_SIZE", "25"))
    poll_interval_seconds = int(env("POLL_INTERVAL_SECONDS", "60"))

    logger.info("funding ingestor started with wallet_batch_size=%s", wallet_batch_size)
    while True:
        try:
            client = ensure_clickhouse_client(client)
            wallets = get_candidate_wallets(client, wallet_batch_size)
            if not wallets:
                logger.info("no candidate wallets pending first-funding lookup")
                time.sleep(poll_interval_seconds)
                continue

            logger.info("processing %s candidate wallets", len(wallets))
            resolved = 0
            for wallet in wallets:
                transfer = fetch_first_usdc_transfer(wallet)
                if transfer is None:
                    continue
                persist_transfer(client, wallet, transfer)
                resolved += 1

            logger.info("resolved first funding for %s wallets", resolved)
        except Exception as exc:  # noqa: BLE001
            logger.exception("funding ingestion loop failed: %s", exc)

        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main()
