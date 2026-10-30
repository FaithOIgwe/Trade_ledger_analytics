-- One row per order that has at least one execution.
-- Reversed executions are netted out, so filled_qty is what actually stuck.

select
    order_id,
    sum(case when is_reversal then -fill_qty else fill_qty end)                 as filled_qty,
    sum(case when is_reversal then -fill_qty else fill_qty end * fill_price)
        / nullif(sum(case when is_reversal then -fill_qty else fill_qty end), 0) as avg_fill_price,
    count(*) filter (where not is_reversal)                                      as execution_count,
    count(*) filter (where is_reversal)                                          as reversal_count,
    min(executed_at)                                                             as first_executed_at,
    max(executed_at)                                                             as last_executed_at
from {{ ref('int_executions') }}
group by order_id
