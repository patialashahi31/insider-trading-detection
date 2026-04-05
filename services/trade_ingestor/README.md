# Trade Ingestor

## Purpose

The trade ingestor indexes Polymarket trade fill events from the Goldsky-hosted Polymarket subgraph.

Its job is to:

- fetch `orderFilledEvents`
- resume from the last successful checkpoint
- store raw trade rows in ClickHouse

## Input

External source:

- Goldsky Polymarket subgraph

Entity used:

- `orderFilledEvents`

## Output Tables

- `raw_order_filled_events`
- `trade_ingestion_checkpoints`

## Checkpoint Strategy

The service stores:

- `last_event_timestamp`
- `last_event_id`

This allows resumable ingestion across container restarts and avoids re-reading the full dataset every time.

## Key Stored Fields

- `event_id`
- `transaction_hash`
- `event_timestamp`
- `order_hash`
- `maker`
- `taker`
- `maker_asset_id`
- `taker_asset_id`
- `maker_amount_filled`
- `taker_amount_filled`
- `fee`

## Why This Service Exists

This is the entry point for wallet discovery. Downstream services depend on the wallets observed here.

Domain relationship:

`trade events -> wallets -> funding enrichment -> features -> score`
