-- Activation funnel by signup cohort: opened -> funded -> traded.
--
-- Grain is one row per cohort month, and every account is counted once, in
-- the month it was opened. That is deliberate. Counting "accounts that
-- funded in March" and "accounts that traded in March" side by side mixes
-- cohorts, and adding up daily or weekly distinct counts double-counts
-- anyone who shows up twice. Both produce a funnel that can't be compared
-- month to month.

with accounts as (

    select
        account_id,
        date_trunc('month', created_at at time zone 'UTC')::date       as cohort_month,
        first_deposit_at,
        first_trade_at,
        extract(epoch from first_trade_at - created_at) / 86400.0       as days_to_first_trade
    from {{ ref('dim_accounts') }}

)

select
    cohort_month,
    count(*)::integer                                                    as accounts_opened,
    count(first_deposit_at)::integer                                     as accounts_funded,
    count(first_trade_at)::integer                                       as accounts_traded,
    (count(first_deposit_at)::numeric / count(*))::numeric(9, 4)         as funded_rate,
    (count(first_trade_at)::numeric / count(*))::numeric(9, 4)           as traded_rate,
    (percentile_cont(0.5) within group (order by days_to_first_trade))::numeric(9, 2)
                                                                         as median_days_to_first_trade
from accounts
group by cohort_month
