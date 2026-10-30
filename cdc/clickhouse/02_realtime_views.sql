-- Live views over the CDC tables. FINAL collapses each key to its latest
-- version at query time; fine at this size, and the trade-off is noted in
-- docs/cdc-vs-batch.md.

-- What's sitting open on the book right now
create or replace view brokerage.rt_open_orders as
select
    symbol,
    side,
    count()                       as open_orders,
    sum(qty)                      as open_qty,
    min(submitted_at)             as oldest_open_since
from brokerage.orders final
where status in ('new', 'partially_filled')
group by symbol, side;

-- Today's traded volume per symbol. The CDC stream carries the raw fills,
-- duplicate execution reports included, so dedupe on exec_id here the same
-- way the dbt staging model does.
create or replace view brokerage.rt_intraday_volume as
with fills_today as (
    select exec_id, order_id, qty, price, is_reversal
    from brokerage.fills final
    where executed_at >= toStartOfDay(now64(6, 'UTC'))
    order by ingested_at
    limit 1 by exec_id
)
select
    o.symbol,
    count()                                                            as executions,
    sum(if(f.is_reversal = 1, -f.qty, f.qty))                          as shares,
    sum(if(f.is_reversal = 1, -f.qty, f.qty) * f.price)                as notional_usd
from fills_today f
inner join (select order_id, symbol from brokerage.orders final) o on o.order_id = f.order_id
group by o.symbol
order by notional_usd desc;

-- How far behind Postgres is ClickHouse? Compare the source commit time
-- of each row with when it landed here.
create or replace view brokerage.rt_lag as
select
    'orders'                                                   as tbl,
    quantile(0.5)(dateDiff('millisecond', fromUnixTimestamp64Milli(toInt64(_version)), _landed_at) / 1000) as p50_seconds,
    quantile(0.95)(dateDiff('millisecond', fromUnixTimestamp64Milli(toInt64(_version)), _landed_at) / 1000) as p95_seconds,
    count()                                                     as rows_last_hour
from brokerage.orders
where _landed_at >= now() - interval 1 hour
union all
select
    'fills',
    quantile(0.5)(dateDiff('millisecond', fromUnixTimestamp64Milli(toInt64(_version)), _landed_at) / 1000),
    quantile(0.95)(dateDiff('millisecond', fromUnixTimestamp64Milli(toInt64(_version)), _landed_at) / 1000),
    count()
from brokerage.fills
where _landed_at >= now() - interval 1 hour;
