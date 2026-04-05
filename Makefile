COMPOSE := docker compose

.PHONY: up down reset logs trade-logs funding-logs feature-logs scoring-logs api-logs frontend-logs clickhouse-logs ps build dashboard-up dashboard-ps backfill-stop trade-shell funding-shell feature-shell scoring-shell api-shell frontend-shell clickhouse-client trade-verify funding-verify feature-verify scoring-verify dashboard-verify

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f --tail=200

trade-logs:
	$(COMPOSE) logs -f --tail=200 trade-ingestor

funding-logs:
	$(COMPOSE) logs -f --tail=200 funding-ingestor

feature-logs:
	$(COMPOSE) logs -f --tail=200 feature-worker

scoring-logs:
	$(COMPOSE) logs -f --tail=200 scoring-worker

api-logs:
	$(COMPOSE) logs -f --tail=200 dashboard-api

frontend-logs:
	$(COMPOSE) logs -f --tail=200 dashboard-frontend

clickhouse-logs:
	$(COMPOSE) logs -f --tail=200 clickhouse

ps:
	$(COMPOSE) ps

build:
	$(COMPOSE) build

dashboard-up:
	$(COMPOSE) up --build -d dashboard-api dashboard-frontend

dashboard-ps:
	$(COMPOSE) ps dashboard-api dashboard-frontend

backfill-stop:
	$(COMPOSE) stop trade-ingestor funding-ingestor

trade-shell:
	$(COMPOSE) exec trade-ingestor sh

funding-shell:
	$(COMPOSE) exec funding-ingestor sh

feature-shell:
	$(COMPOSE) exec feature-worker sh

scoring-shell:
	$(COMPOSE) exec scoring-worker sh

api-shell:
	$(COMPOSE) exec dashboard-api sh

frontend-shell:
	$(COMPOSE) exec dashboard-frontend sh

clickhouse-client:
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket

trade-verify:
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SHOW TABLES"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT count() AS raw_rows, count(DISTINCT event_id) AS distinct_events FROM raw_order_filled_events"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT source, last_event_timestamp, last_event_id, updated_at FROM trade_ingestion_checkpoints ORDER BY updated_at DESC LIMIT 3 FORMAT Vertical"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT event_id, transaction_hash, event_timestamp, maker, taker FROM raw_order_filled_events ORDER BY ingested_at DESC LIMIT 5 FORMAT Vertical"

funding-verify:
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT count() AS raw_transfer_rows, count(DISTINCT wallet) AS distinct_wallets FROM raw_usdc_transfers"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT count() AS wallets_with_first_funding FROM wallet_first_funding"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT wallet, first_funding_transaction_hash, first_funding_timestamp, value FROM wallet_first_funding ORDER BY updated_at DESC LIMIT 5 FORMAT Vertical"

feature-verify:
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT count() AS wallets_with_features FROM wallet_features"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT wallet, trade_count, distinct_markets, total_volume, wallet_age_seconds_at_first_trade FROM wallet_features ORDER BY updated_at DESC LIMIT 5 FORMAT Vertical"

scoring-verify:
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT count() AS scored_wallets FROM insider_scores"
	$(COMPOSE) exec clickhouse clickhouse-client --user app --password app_password --database polymarket --query "SELECT wallet, score, risk_level, reasons, computed_at FROM insider_scores ORDER BY computed_at DESC LIMIT 5 FORMAT Vertical"

dashboard-verify:
	curl -sS http://localhost:8000/health
	curl -sS http://localhost:8000/summary
