{{
    config(
        materialized='incremental',
        unique_key='exec_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- One row per execution that actually happened (duplicates already gone,
-- reversals kept as their own negative rows so history is never rewritten).
--
-- Incremental on ingested_at, not executed_at. A fill that executed on
-- Monday but only reached us on Wednesday still gets picked up on
-- Wednesday's run, because it's new by ingested_at.

select
    exec_id,
    order_id,
    account_id,
    symbol,
    side,
    is_reversal,
    reverses_exec_id,
    fill_qty                                                  as qty,
    signed_qty,
    fill_price                                                as price,
    (signed_qty * fill_price)::numeric(18, 4)                 as signed_notional_usd,
    executed_at,
    (executed_at at time zone 'UTC')::date                    as executed_date,
    ingested_at,
    (extract(epoch from ingested_at - executed_at) / 60)::integer as arrival_lag_minutes,
    ingested_at - executed_at > interval '1 hour'             as is_late_arrival
from {{ ref('int_executions') }}

{% if is_incremental() %}
where ingested_at > (
    select coalesce(max(ingested_at), '1900-01-01'::timestamptz)
         - interval '{{ var("trades_lookback_hours") }} hours'
    from {{ this }}
)
{% endif %}
