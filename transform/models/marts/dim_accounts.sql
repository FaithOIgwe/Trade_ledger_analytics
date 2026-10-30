-- Current state of each account from the snapshot, plus a few things
-- people always ask for alongside it.

with current_version as (

    select *
    from {{ ref('snap_accounts') }}
    where dbt_valid_to is null

),

history as (

    select account_id, count(*) - 1 as times_changed
    from {{ ref('snap_accounts') }}
    group by account_id

),

funding as (

    select account_id, min(created_at) as first_deposit_at
    from {{ ref('stg_brokerage__cash_movements') }}
    where movement_type = 'deposit'
    group by account_id

),

trading as (

    select account_id, min(executed_at) as first_trade_at, max(executed_at) as last_trade_at,
           count(*) filter (where not is_reversal) as lifetime_executions
    from {{ ref('fct_trades') }}
    group by account_id

)

select
    c.account_id,
    trim(c.customer_name)                          as customer_name,
    upper(c.country)                               as country_code,
    lower(c.account_type)                          as account_type,
    lower(c.status)                                as account_status,
    lower(c.tier)                                  as account_tier,
    c.created_at,
    f.first_deposit_at,
    t.first_trade_at,
    t.last_trade_at,
    coalesce(t.lifetime_executions, 0)::integer    as lifetime_executions,
    h.times_changed::integer                       as times_changed,
    c.dbt_valid_from                               as current_since
from current_version c
left join history h using (account_id)
left join funding f using (account_id)
left join trading t using (account_id)
