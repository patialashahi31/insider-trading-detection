import os

import requests
import streamlit as st


API_BASE_URL = os.getenv("DASHBOARD_API_URL", "http://dashboard-api:8000")

st.set_page_config(page_title="Insider Trading Assignment", layout="wide")


def api_get(path: str, params: dict | None = None):
    response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


st.title("Polymarket Insider Detection Dashboard")

try:
    summary = api_get("/summary")
except Exception as exc:  # noqa: BLE001
    st.error(f"Failed to load dashboard summary: {exc}")
    st.stop()


col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Trade Events", summary["total_trade_events"])
col2.metric("Funded Wallets", summary["funded_wallets"])
col3.metric("Wallet Features", summary["wallets_with_features"])
col4.metric("Scored Wallets", summary["scored_wallets"])
col5.metric("High Risk Wallets", summary["high_risk_wallets"])

st.divider()

left, right = st.columns([2, 1])

effective_wallet_lookup = ""

with right:
    selected_risk = st.selectbox("Risk Level", ["all", "high", "medium", "low"], index=0)
    limit = st.slider("Rows", min_value=10, max_value=100, value=25, step=5)
    wallet_lookup = st.text_input("Wallet Lookup", placeholder="0x...", key="wallet_lookup")
    effective_wallet_lookup = wallet_lookup.strip()

with left:
    params = {"limit": limit}
    if selected_risk != "all":
        params["risk_level"] = selected_risk

    score_payload = api_get("/scores", params=params)
    score_items = score_payload["items"]
    st.subheader("Top Wallet Scores")
    st.dataframe(score_items, use_container_width=True)

    wallet_options = [item["wallet"] for item in score_items]
    if wallet_options:
        selected_wallet = st.selectbox(
            "Select Wallet From Scores",
            wallet_options,
            index=None,
            placeholder="Choose a wallet to inspect",
        )
        if st.button("Open Selected Wallet", use_container_width=True) and selected_wallet:
            effective_wallet_lookup = selected_wallet


if effective_wallet_lookup:
    st.divider()
    st.subheader(f"Wallet Detail: {effective_wallet_lookup}")
    try:
        wallet_payload = api_get(f"/wallets/{effective_wallet_lookup}")
        feature = wallet_payload["feature"] or {}
        score = wallet_payload["score"] or {}
        funding = wallet_payload["funding"] or {}

        a, b, c, d = st.columns(4)
        a.metric("Trades", feature.get("trade_count", len(wallet_payload["recent_trades"])))
        b.metric("Distinct Markets", feature.get("distinct_markets", "n/a"))
        c.metric("Score", score.get("score", "n/a"))
        d.metric("Risk", score.get("risk_level", "n/a"))

        if feature:
            st.write("Feature Snapshot")
            st.json(feature)
        else:
            st.info("Feature computation has not finished for this wallet yet. Showing raw trade data instead.")

        if funding:
            st.write("Funding Snapshot")
            st.json(funding)

        st.write("Recent Trades")
        st.dataframe(wallet_payload["recent_trades"], use_container_width=True)
    except requests.HTTPError as exc:
        if exc.response.status_code == 404:
            st.warning("Wallet not found in current trades, funding, or computed features.")
        else:
            st.error(f"Failed to load wallet detail: {exc}")
