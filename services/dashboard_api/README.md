# Dashboard API

## Purpose

The dashboard API is the query layer for analysts and the Streamlit frontend.

It exposes ClickHouse-backed endpoints for:

- system summary
- ranked scores
- wallet drill-down

## Technology

- FastAPI

## Endpoints

- `GET /health`
- `GET /summary`
- `GET /scores`
- `GET /wallets/{wallet}`

## Data Sources

This service reads from:

- `raw_order_filled_events`
- `wallet_first_funding`
- `wallet_features`
- `insider_scores`

## Why This Service Exists

It isolates query logic from the frontend and provides a clean backend boundary for future expansion.
