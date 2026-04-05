import os
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Insider Trading Assignment API", version="0.1.0")


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


def rows_to_dicts(result) -> list[dict[str, Any]]:
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def normalize_wallet(wallet: str) -> str:
    return wallet.strip().lower()


@app.get("/health")
def health() -> dict[str, str]:
    client = get_clickhouse_client()
    client.query("SELECT 1")
    return {"status": "ok"}


@app.get("/summary")
def summary() -> dict[str, Any]:
    client = get_clickhouse_client()
    result = client.query(
        """
        SELECT
            (SELECT countDistinct(event_id) FROM raw_order_filled_events) AS total_trade_events,
            (SELECT countDistinct(wallet) FROM wallet_first_funding) AS funded_wallets,
            (SELECT countDistinct(wallet) FROM wallet_features) AS wallets_with_features,
            (SELECT countDistinct(wallet) FROM insider_scores) AS scored_wallets,
            (SELECT countIf(risk_level = 'high') FROM insider_scores FINAL) AS high_risk_wallets
        """
    )
    row = result.result_rows[0]
    return {
        "total_trade_events": row[0],
        "funded_wallets": row[1],
        "wallets_with_features": row[2],
        "scored_wallets": row[3],
        "high_risk_wallets": row[4],
    }


@app.get("/scores")
def scores(limit: int = 50, risk_level: str | None = None) -> dict[str, Any]:
    client = get_clickhouse_client()
    allowed_risk_levels = {"high", "medium", "low"}
    where_clause = ""
    if risk_level:
        if risk_level not in allowed_risk_levels:
            raise HTTPException(status_code=400, detail="invalid risk_level")
        where_clause = "WHERE risk_level = %(risk_level)s"

    result = client.query(
        f"""
        SELECT wallet, score, risk_level, reasons, computed_at
        FROM insider_scores FINAL
        {where_clause}
        ORDER BY score DESC, computed_at DESC
        LIMIT %(limit)s
        """,
        parameters={"limit": limit, "risk_level": risk_level},
    )
    return {"items": rows_to_dicts(result)}


@app.get("/wallets/{wallet}")
def wallet_detail(wallet: str) -> dict[str, Any]:
    client = get_clickhouse_client()
    normalized_wallet = normalize_wallet(wallet)

    feature_result = client.query(
        """
        SELECT
            wallet,
            first_trade_timestamp,
            latest_trade_timestamp,
            trade_count,
            distinct_markets,
            total_volume,
            max_single_trade_volume,
            wallet_age_seconds_at_first_trade,
            has_first_funding,
            first_funding_timestamp,
            first_funding_value,
            updated_at
        FROM wallet_features FINAL
        WHERE wallet = %(wallet)s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        parameters={"wallet": normalized_wallet},
    )

    score_result = client.query(
        """
        SELECT wallet, score, risk_level, reasons, computed_at
        FROM insider_scores FINAL
        WHERE wallet = %(wallet)s
        ORDER BY computed_at DESC
        LIMIT 1
        """,
        parameters={"wallet": normalized_wallet},
    )

    trade_result = client.query(
        """
        SELECT
            event_id,
            transaction_hash,
            event_timestamp,
            maker,
            taker,
            maker_asset_id,
            taker_asset_id,
            maker_amount_filled,
            taker_amount_filled,
            fee
        FROM raw_order_filled_events FINAL
        WHERE maker = %(wallet)s OR taker = %(wallet)s
        ORDER BY event_timestamp DESC
        LIMIT 20
        """,
        parameters={"wallet": normalized_wallet},
    )

    funding_result = client.query(
        """
        SELECT
            wallet,
            first_funding_transaction_hash,
            first_funding_block_number,
            first_funding_timestamp,
            from_address,
            to_address,
            value,
            updated_at
        FROM wallet_first_funding FINAL
        WHERE wallet = %(wallet)s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        parameters={"wallet": normalized_wallet},
    )

    if (
        not feature_result.result_rows
        and not score_result.result_rows
        and not trade_result.result_rows
        and not funding_result.result_rows
    ):
        raise HTTPException(status_code=404, detail="wallet not found")

    return {
        "wallet": normalized_wallet,
        "feature": rows_to_dicts(feature_result)[0] if feature_result.result_rows else None,
        "score": rows_to_dicts(score_result)[0] if score_result.result_rows else None,
        "funding": rows_to_dicts(funding_result)[0] if funding_result.result_rows else None,
        "recent_trades": rows_to_dicts(trade_result),
    }
