-- End-of-day position per account and symbol, valued at that day's close
-- (or the last close before it on weekends and holidays).
-- Days where the holding is flat are left out.

with daily_changes as (

    select account_id, symbol, executed_date as position_date, sum(signed_qty) as qty_change
    from {{ ref('fct_trades') }}
    group by 1, 2, 3

),

first_trade as (

    select account_id, symbol, min(position_date) as first_date
    from daily_changes
    group by 1, 2

),

spine as (

    select f.account_id, f.symbol, d::date as position_date
    from first_trade f
    cross join lateral generate_series(f.first_date, current_date, interval '1 day') as d

),

positions as (

    select
        s.account_id,
        s.symbol,
        s.position_date,
        sum(coalesce(c.qty_change, 0)) over (
            partition by s.account_id, s.symbol
            order by s.position_date
        ) as quantity
    from spine s
    left join daily_changes c using (account_id, symbol, position_date)

),

prices as (

    -- carry the last close forward over days with no bar
    select
        p.symbol,
        d::date as price_date,
        (
            select close_price
            from {{ ref('stg_market__daily_prices') }} m
            where m.symbol = p.symbol and m.bar_date <= d::date
            order by m.bar_date desc
            limit 1
        ) as close_price
    from (select symbol, min(position_date) as start_date from spine group by symbol) p
    cross join lateral generate_series(p.start_date, current_date, interval '1 day') as d

)

select
    pos.account_id,
    pos.symbol,
    pos.position_date,
    pos.quantity::numeric(18, 4)                         as quantity,
    pr.close_price,
    (pos.quantity * pr.close_price)::numeric(18, 4)      as market_value_usd
from positions pos
left join prices pr
    on pr.symbol = pos.symbol
   and pr.price_date = pos.position_date
where pos.quantity <> 0
