-- Every cash movement with the account's running balance after it.

select
    movement_id,
    account_id,
    movement_type,
    amount,
    currency,
    reference_id,
    created_at,
    (created_at at time zone 'UTC')::date as created_date,
    sum(amount) over (
        partition by account_id
        order by created_at, movement_id
        rows between unbounded preceding and current row
    )::numeric(18, 4) as balance_after
from {{ ref('stg_brokerage__cash_movements') }}
