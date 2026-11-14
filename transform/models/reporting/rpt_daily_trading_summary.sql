-- One row per trading day (UTC). This is the table someone opens first thing
-- in the morning: how much traded, by how many customers, and how much of it
-- needed a second look (reversals, fills that arrived late).
--
-- Reversals are counted and valued separately instead of being netted into
-- the headline numbers, so a busted trade never makes yesterday look quieter
-- than it was.

select
    executed_date                                                        as trade_date,
    count(distinct account_id) filter (where not is_reversal)::integer   as active_accounts,
    count(*) filter (where not is_reversal)::integer                     as executions,
    count(*) filter (where is_reversal)::integer                         as reversals,
    coalesce(sum(qty * price) filter (where not is_reversal), 0)::numeric(18, 4) as gross_notional_usd,
    coalesce(sum(qty * price) filter (where is_reversal), 0)::numeric(18, 4)     as reversed_notional_usd,
    sum(signed_notional_usd)::numeric(18, 4)                             as net_buy_notional_usd,
    count(*) filter (where is_late_arrival)::integer                     as late_arrivals,
    (count(*) filter (where is_late_arrival)::numeric / count(*))::numeric(9, 4) as late_arrival_rate
from {{ ref('fct_trades') }}
group by executed_date
