# Feature Worker

## Purpose

The feature worker converts raw trade and funding data into wallet-level behavioral features.

It is the bridge between:

- indexed events
- suspiciousness scoring

## Inputs

- `raw_order_filled_events`
- `wallet_first_funding`

## Output Table

- `wallet_features`

## Current Features

- `first_trade_timestamp`
- `latest_trade_timestamp`
- `trade_count`
- `distinct_markets`
- `total_volume`
- `max_single_trade_volume`
- `wallet_age_seconds_at_first_trade`
- `has_first_funding`
- `first_funding_timestamp`
- `first_funding_value`

## Why This Service Exists

Raw events are too low-level for ranking suspicious wallets directly.

This service turns event data into structured evidence that the scoring worker can interpret.
