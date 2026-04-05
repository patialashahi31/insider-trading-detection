# Dashboard Frontend

## Purpose

The dashboard frontend gives an analyst-friendly interface over the detection pipeline.

It is intentionally lightweight and consumes the FastAPI backend rather than querying ClickHouse directly.

## Technology

- Streamlit

## Current Views

- summary metrics
- scored wallets table
- wallet selection from scores
- wallet lookup
- wallet detail with feature, funding, and recent trade context

## Backend Dependency

This frontend talks to:

- `dashboard-api`

via:

- `DASHBOARD_API_URL`

## Why This Service Exists

It makes the pipeline inspectable without requiring direct SQL access and provides a simple demo surface for the assignment.
