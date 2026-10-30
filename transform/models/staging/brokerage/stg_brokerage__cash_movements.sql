select
    movement_id,
    account_id,
    lower(movement_type)        as movement_type,
    amount::numeric(18, 4)      as amount,
    upper(currency)             as currency,
    reference_id,
    created_at
from {{ source('brokerage', 'cash_movements') }}
