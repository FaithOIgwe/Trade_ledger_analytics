-include .env
export

.PHONY: up seed tick dbt docs lakehouse iceberg trino-pruning cdc connector clickhouse rt down clean

PY ?= python
DBT = cd transform && DBT_PROFILES_DIR=. dbt

# ---- batch path ----

up:
	docker compose up -d --build

seed:
	$(PY) ingestion/load_market_prices.py --days 130
	$(PY) generator/generate.py seed --accounts 500 --days 120

tick:
	$(PY) generator/generate.py tick

dbt:
	$(DBT) source freshness
	$(DBT) build

docs:
	$(DBT) docs generate && $(DBT) docs serve --port 8085

# ---- lakehouse ----

lakehouse:
	docker compose --profile lakehouse up -d

iceberg:
	docker compose exec spark spark-submit \
		--packages org.postgresql:postgresql:42.7.4 \
		/home/iceberg/jobs/trades_to_iceberg.py

trino-pruning:
	docker compose exec trino trino -f /queries/01_partition_pruning.sql

# ---- cdc ----

cdc:
	docker compose --profile cdc up -d

connector:
	curl -s -X POST -H "Content-Type: application/json" \
		--data @cdc/debezium/brokerage-connector.json \
		http://localhost:8083/connectors | python -m json.tool

clickhouse:
	docker compose exec -T clickhouse clickhouse-client -u analyst --password analyst --multiquery < cdc/clickhouse/01_tables.sql
	docker compose exec -T clickhouse clickhouse-client -u analyst --password analyst --multiquery < cdc/clickhouse/02_realtime_views.sql

rt:
	docker compose exec clickhouse clickhouse-client -u analyst --password analyst \
		--query "select * from brokerage.rt_intraday_volume format PrettyCompact"

# ---- teardown ----

down:
	docker compose --profile lakehouse --profile cdc down

clean:
	docker compose --profile lakehouse --profile cdc down -v
