-- Every execution should have exactly one settlement in the cash ledger,
-- and the amount should equal the trade value (opposite sign: buying
-- stock takes cash out). Allow a cent for rounding.

with settlements as (

    select reference_id as exec_id, sum(amount) as settled_amount, count(*) as settlement_rows
    from {{ ref('fct_cash_ledger') }}
    where movement_type = 'trade_settlement'
    group by reference_id

)

select
    t.exec_id,
    t.signed_notional_usd,
    s.settled_amount,
    s.settlement_rows
from {{ ref('fct_trades') }} t
left join settlements s using (exec_id)
where s.exec_id is null
   or s.settlement_rows <> 1
   or abs(s.settled_amount + t.signed_notional_usd) > 0.01
