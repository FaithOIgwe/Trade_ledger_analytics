select
    o.order_id,
    o.client_order_id,
    o.account_id,
    o.symbol,
    o.side,
    o.order_type,
    o.order_status,
    o.order_qty,
    o.limit_price,
    coalesce(f.filled_qty, 0)::numeric(18, 4)                      as filled_qty,
    f.avg_fill_price::numeric(18, 4)                               as avg_fill_price,
    (coalesce(f.filled_qty, 0) / o.order_qty)::numeric(9, 4)       as fill_ratio,
    coalesce(f.execution_count, 0)::integer                        as execution_count,
    coalesce(f.reversal_count, 0)::integer                         as reversal_count,
    o.submitted_at,
    f.first_executed_at,
    f.last_executed_at,
    (extract(epoch from f.first_executed_at - o.submitted_at))::numeric(12, 3) as seconds_to_first_fill,
    o.updated_at
from {{ ref('stg_brokerage__orders') }} o
left join {{ ref('int_order_fill_summary') }} f using (order_id)
