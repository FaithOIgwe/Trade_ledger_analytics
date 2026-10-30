-- The feed sometimes sends the same execution twice (same exec_id, new
-- fill_id). Keep the first report we received and count the extras so
-- they can be monitored instead of silently dropped.

with ranked as (

    select
        *,
        row_number() over (partition by exec_id order by ingested_at, fill_id) as report_seq,
        count(*)     over (partition by exec_id)                               as report_count
    from {{ source('brokerage', 'fills') }}

)

select
    exec_id,
    fill_id,
    order_id,
    qty::numeric(18, 4)      as fill_qty,
    price::numeric(18, 4)    as fill_price,
    executed_at,
    ingested_at,
    is_reversal,
    reverses_exec_id,
    report_count - 1         as duplicate_reports_dropped
from ranked
where report_seq = 1
