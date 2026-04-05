# Funding Ingestor

## Purpose

The funding ingestor enriches traded wallets with their first observed USDC.e funding event.

Its job is to:

- read wallets from indexed trade data
- query Etherscan for USDC.e transfer history
- persist first funding information in ClickHouse

## Input

Internal dependency:

- `raw_order_filled_events`

External source:

- Etherscan Polygon API

Contract used:

- `USDC.e`: `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`

## Output Tables

- `raw_usdc_transfers`
- `wallet_first_funding`

## Why This Service Exists

Trade data tells us which wallets are active.

Funding data tells us:

- when the wallet first got money
- how large the first funding was

These signals are later used in suspiciousness scoring.
