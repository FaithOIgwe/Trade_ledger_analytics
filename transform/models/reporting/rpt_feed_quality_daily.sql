-- One row per ingestion day (UTC): how clean was the execution feed?
--
-- Staging drops duplicate execution reports and fct_trades flags late
-- arrivals, but a number nobody looks at is the same as no check at all.
-- This puts both on one row per day so a jump is visible, and so an alert
-- has something simple to read.
--
-- Built from int_executions rather than fct_trades because the duplicate
-- count lives there, and by ingestion date rather than execution date
-- because feed problems happen when the data arrives, not when it traded.

select
    (ingested_at at time zone 'UTC')::date                               as ingested_date,
    count(*)::integer                                                    as executions_kept,
    sum(duplicate_reports_dropped)::integer                              as duplicate_reports_dropped,
    (sum(duplicate_reports_dropped)::numeric
        / (count(*) + sum(duplicate_reports_dropped)))::numeric(9, 4)    as duplicate_report_rate,
    count(*) filter (where ingested_at - executed_at > interval '1 hour')::integer
                                                                         as late_arrivals,
    max((extract(epoch from ingested_at - executed_at) / 60)::integer)   as max_arrival_lag_minutes,
    count(*) filter (where is_reversal)::integer                         as reversals_received
from {{ ref('int_executions') }}
group by 1
