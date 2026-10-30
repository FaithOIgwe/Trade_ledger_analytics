-- The three Iceberg things worth being able to explain in an interview.
-- Run these one at a time in the Trino CLI:
--   docker compose exec trino trino

-- 1. What's in the table right now: snapshots and files
select snapshot_id, committed_at, operation
from iceberg.brokerage."trades$snapshots"
order by committed_at;

select count(*) as data_files, sum(record_count) as rows, round(avg(file_size_in_bytes) / 1024.0, 1) as avg_kb
from iceberg.brokerage."trades$files";

-- 2. Schema evolution: add a column without rewriting any data
alter table iceberg.brokerage.trades add column venue varchar;
update iceberg.brokerage.trades set venue = 'IEX' where symbol in ('SPY', 'QQQ');

-- 3. Time travel: the table as it was before that update
--    (replace the id with the first snapshot_id from step 1)
-- select count(*) from iceberg.brokerage.trades for version as of <snapshot_id> where venue is not null;

-- 4. Compaction: the update above wrote small files. Merge them.
alter table iceberg.brokerage.trades execute optimize(file_size_threshold => '64MB');

select count(*) as data_files_after_optimize
from iceberg.brokerage."trades$files";
