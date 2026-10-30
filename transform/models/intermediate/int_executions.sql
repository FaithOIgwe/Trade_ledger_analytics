-- Executions with the order context attached and quantities signed from the
-- customer's side: buys add to the position, sells take away, and a reversal
-- undoes whatever the original execution did.

with fills as (
    select * from {{ ref('stg_brokerage__fills') }}
),

orders as (
    select order_id, account_id, symbol, side from {{ ref('stg_brokerage__orders') }}
)

select
    f.exec_id,
    f.order_id,
    o.account_id,
    o.symbol,
    o.side,
    f.is_reversal,
    f.reverses_exec_id,
    f.fill_qty,
    f.fill_price,
    f.fill_qty
        * case when o.side = 'buy' then 1 else -1 end
        * case when f.is_reversal then -1 else 1 end           as signed_qty,
    f.executed_at,
    f.ingested_at,
    f.duplicate_reports_dropped
from fills f
join orders o using (order_id)
