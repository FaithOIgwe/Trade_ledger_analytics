select
    order_id,
    client_order_id,
    account_id,
    upper(symbol)                    as symbol,
    lower(side)                      as side,
    lower(order_type)                as order_type,
    qty::numeric(18, 4)              as order_qty,
    limit_price::numeric(18, 4)      as limit_price,
    lower(status)                    as order_status,
    submitted_at,
    updated_at
from {{ source('brokerage', 'orders') }}
