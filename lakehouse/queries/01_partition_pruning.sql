-- Same question asked of both tables: one week of NVDA volume by account.
-- Run each EXPLAIN ANALYZE and compare "Input:" rows/bytes and the number
-- of splits on the table scan. Paste what you get into docs/lakehouse-notes.md.
--
-- docker compose exec trino trino -f /queries/01_partition_pruning.sql

explain analyze
select account_id, sum(qty) as shares, sum(abs(signed_notional_usd)) as notional
from iceberg.brokerage.trades
where symbol = 'NVDA'
  and executed_at >= current_timestamp - interval '7' day
group by account_id;

explain analyze
select account_id, sum(qty) as shares, sum(abs(signed_notional_usd)) as notional
from iceberg.brokerage.trades_unpartitioned
where symbol = 'NVDA'
  and executed_at >= current_timestamp - interval '7' day
group by account_id;
