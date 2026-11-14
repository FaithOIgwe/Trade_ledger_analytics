-- The daily summary has to add back up to the fact table it was built from.
-- If a filter or a join in the summary ever drops or doubles rows, the
-- totals drift and this fails.

with summary as (

    select
        sum(executions)            as executions,
        sum(reversals)             as reversals,
        sum(net_buy_notional_usd)  as net_notional
    from {{ ref('rpt_daily_trading_summary') }}

),

trades as (

    select
        count(*) filter (where not is_reversal)  as executions,
        count(*) filter (where is_reversal)      as reversals,
        sum(signed_notional_usd)                 as net_notional
    from {{ ref('fct_trades') }}

)

select s.*, t.executions as trade_executions, t.reversals as trade_reversals, t.net_notional as trade_net_notional
from summary s
cross join trades t
where s.executions   is distinct from t.executions
   or s.reversals    is distinct from t.reversals
   or s.net_notional is distinct from t.net_notional
