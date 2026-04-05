CREATE TABLE IF NOT EXISTS polymarket.raw_order_filled_events
(
    event_id String,
    transaction_hash String,
    event_timestamp DateTime,
    order_hash String,
    maker String,
    taker String,
    maker_asset_id String,
    taker_asset_id String,
    maker_amount_filled String,
    taker_amount_filled String,
    fee String,
    source String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (event_id, event_timestamp);

CREATE TABLE IF NOT EXISTS polymarket.trade_ingestion_checkpoints
(
    source String,
    last_event_timestamp Nullable(DateTime),
    last_event_id Nullable(String),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source);

CREATE TABLE IF NOT EXISTS polymarket.raw_usdc_transfers
(
    wallet String,
    transaction_hash String,
    block_number UInt64,
    event_timestamp DateTime,
    from_address String,
    to_address String,
    value String,
    token_decimal UInt8,
    source String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (wallet, transaction_hash);

CREATE TABLE IF NOT EXISTS polymarket.wallet_first_funding
(
    wallet String,
    first_funding_transaction_hash String,
    first_funding_block_number UInt64,
    first_funding_timestamp DateTime,
    from_address String,
    to_address String,
    value String,
    source String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (wallet);

CREATE TABLE IF NOT EXISTS polymarket.wallet_features
(
    wallet String,
    first_trade_timestamp Nullable(DateTime),
    latest_trade_timestamp Nullable(DateTime),
    trade_count UInt64,
    distinct_markets UInt64,
    total_volume Float64,
    max_single_trade_volume Float64,
    wallet_age_seconds_at_first_trade Nullable(Int64),
    has_first_funding UInt8,
    first_funding_timestamp Nullable(DateTime),
    first_funding_value Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (wallet);

CREATE TABLE IF NOT EXISTS polymarket.insider_scores
(
    wallet String,
    score Float64,
    risk_level String,
    reasons String,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (wallet);
