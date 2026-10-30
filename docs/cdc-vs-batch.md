# CDC or batch?

Both paths run side by side in this repo, so this is less about which one is "better" and more about which tables actually need streaming.

## How each one works here

**Batch (dbt, hourly).** dbt reads the source tables directly, rebuilds staging views, adds new executions to `fct_trades` by `ingested_at`, and rebuilds the other marts. Freshness is checked first. Worst-case staleness is roughly one schedule interval plus build time.

**CDC (Debezium → Redpanda → ClickHouse).** Debezium reads the Postgres write-ahead log through a logical replication slot and publishes one message per row change. ClickHouse consumes the topics and keeps the latest version of each row with `ReplacingMergeTree`. Changes are queryable seconds after they commit.

## Measured on my laptop

`brokerage.rt_lag` in ClickHouse reports the gap between the source commit and the row landing. For batch, I timed `make tick` → `make dbt`.

| | batch | CDC |
| --- | --- | --- |
| commit → queryable, p50 | _fill in_ | _fill in_ |
| commit → queryable, p95 | _fill in_ | _fill in_ |
| containers needed | 2 | 5 |
| a full rebuild | `dbt build --full-refresh` | new replication slot + re-snapshot |

## Where they differ

**Updates and deletes.** Batch sees the current state of a row at the moment it runs. If an order goes new → partially filled → filled between two runs, batch only ever sees "filled". CDC sees every step. That matters for questions like "how long did orders sit on the book", and doesn't matter for end-of-day positions.

**Late and duplicate data.** Neither path makes these go away. CDC delivers the duplicate execution reports faithfully, so the ClickHouse views dedupe on `exec_id` exactly like the dbt staging model does. The logic has to live in both places, which is a real cost: two implementations of the same rule can drift.

**Correctness checks.** The batch side has contracts, reconciliation tests and freshness gates. The streaming side has none of that yet. If a malformed message arrives, ClickHouse skips it (`kafka_skip_broken_messages`) and nobody is told.

**Operational load.** CDC means a replication slot on the production database. If the consumer stops, the slot holds WAL and the source disk fills up. That needs monitoring before it's safe to run against a real system. Batch just queries the database for a few seconds an hour.

**Query patterns.** `FINAL` collapses versions at query time. It's fine at this volume; at real scale I'd either rely on background merges and accept a short window of duplicates, or keep a separate aggregated table updated by the materialized view.

## What I'd stream and what I wouldn't

| table | path | why |
| --- | --- | --- |
| orders | CDC | open orders, rejections and time-on-book are only useful live, and intermediate states matter |
| fills | CDC + batch | live volume for ops; batch stays the reconciled record that finance uses |
| cash movements | batch | balances need to reconcile to the ledger; a few minutes' delay is fine, wrong numbers aren't |
| accounts | batch (snapshot) | changes rarely; the snapshot already keeps history |
| market prices | batch | daily bars, nothing to stream |

In short: stream where someone acts on the data within minutes, and keep batch as the system of record because that's where the tests and contracts are.
