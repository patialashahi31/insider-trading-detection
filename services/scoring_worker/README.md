# Scoring Worker

## Purpose

The scoring worker reads wallet features and assigns a heuristic insider-risk score.

It is the first place where the system turns behavior into a ranked suspiciousness output.

## Input

- `wallet_features`

## Output Table

- `insider_scores`

## Current Scoring Signals

The current heuristic model adds risk points for patterns such as:

- few trades
- few distinct markets
- large single trade
- high total volume
- large initial funding
- very short time from first funding to first trade

## Output

Each scored wallet gets:

- `score`
- `risk_level`
- `reasons`

## Why This Service Exists

This service creates a compact analyst-facing risk signal so wallets can be ranked and investigated.
