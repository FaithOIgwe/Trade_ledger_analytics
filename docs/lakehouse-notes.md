# Lakehouse notes

`make iceberg` copies `analytics.fct_trades` into two Iceberg tables with the same rows:

- `iceberg.brokerage.trades`, partitioned by `days(executed_at)`
- `iceberg.brokerage.trades_unpartitioned`

Then `make trino-pruning` runs the same filter (one symbol, last 7 days) against both with `EXPLAIN ANALYZE`.

## Partition pruning

| | partitioned | unpartitioned |
| --- | --- | --- |
| data files in table | _fill in_ | _fill in_ |
| files / splits scanned | _fill in_ | _fill in_ |
| input rows | _fill in_ | _fill in_ |
| input size | _fill in_ | _fill in_ |
| wall time | _fill in_ | _fill in_ |

What to look for in the plan: on the partitioned table, the scan on `trades` should only read the files for the last 7 days of partitions. The `symbol` filter can't prune partitions (it isn't a partition column), but Iceberg's per-file min/max stats can still skip files when the data is sorted. A next step would be `write.sort-order` on `symbol` and comparing again.

One trade-off showed up straight away: daily partitions on a small dataset produce lots of tiny files. Partitioning only pays off once each partition holds enough data, otherwise file-open overhead outweighs what you skip.

## Iceberg features (02_iceberg_features.sql)

- **Snapshots:** every write (the CTAS, the `ALTER`, the `UPDATE`, the `OPTIMIZE`) creates a snapshot listed in `trades$snapshots`.
- **Schema evolution:** `ADD COLUMN venue` is a metadata-only change. No data files are rewritten; old files just return null for the new column.
- **Time travel:** `FOR VERSION AS OF <snapshot_id>` queries the table as it was before the update.
- **Compaction:** the `UPDATE` writes small delete and data files. `OPTIMIZE` merges them. File count before: _fill in_, after: _fill in_.
