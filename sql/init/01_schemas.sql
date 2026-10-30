-- Runs once when the Postgres container is first created.
-- brokerage = the "app" database we pretend belongs to the trading system
-- raw       = things we load from outside (market data)
-- analytics = where dbt builds everything

create schema if not exists brokerage;
create schema if not exists raw;
create schema if not exists analytics;

create table if not exists brokerage.accounts (
    account_id      uuid primary key,
    customer_name   text not null,
    country         text not null,
    account_type    text not null,          -- cash | margin
    status          text not null,          -- active | restricted | closed
    tier            text not null,          -- basic | plus | pro
    created_at      timestamptz not null,
    updated_at      timestamptz not null
);

create table if not exists brokerage.orders (
    order_id         uuid primary key,
    client_order_id  text not null,
    account_id       uuid not null references brokerage.accounts (account_id),
    symbol           text not null,
    side             text not null,         -- buy | sell
    order_type       text not null,         -- market | limit
    qty              numeric(18, 4) not null,
    limit_price      numeric(18, 4),
    status           text not null,         -- new | partially_filled | filled | canceled | rejected
    submitted_at     timestamptz not null,
    updated_at       timestamptz not null
);

-- One row per execution report we receive. The venue's exec_id is what
-- identifies an execution; fill_id is ours. When the upstream feed retries
-- we can get the same exec_id twice with different fill_ids.
create table if not exists brokerage.fills (
    fill_id           uuid primary key,
    exec_id           text not null,
    order_id          uuid not null references brokerage.orders (order_id),
    qty               numeric(18, 4) not null,
    price             numeric(18, 4) not null,
    executed_at       timestamptz not null,
    ingested_at       timestamptz not null,
    is_reversal       boolean not null default false,
    reverses_exec_id  text
);

create table if not exists brokerage.cash_movements (
    movement_id     uuid primary key,
    account_id      uuid not null references brokerage.accounts (account_id),
    movement_type   text not null,          -- deposit | withdrawal | trade_settlement | fee
    amount          numeric(18, 4) not null,  -- signed, from the customer's point of view
    currency        text not null default 'USD',
    reference_id    text,                   -- exec_id for trade_settlement / fee rows
    created_at      timestamptz not null
);

create table if not exists raw.market_prices (
    symbol      text not null,
    bar_date    date not null,
    open        numeric(18, 4),
    high        numeric(18, 4),
    low         numeric(18, 4),
    close       numeric(18, 4) not null,
    volume      bigint,
    source      text not null,              -- alpaca | synthetic
    loaded_at   timestamptz not null default now(),
    primary key (symbol, bar_date)
);

create index if not exists ix_orders_account on brokerage.orders (account_id);
create index if not exists ix_orders_updated on brokerage.orders (updated_at);
create index if not exists ix_fills_order on brokerage.fills (order_id);
create index if not exists ix_fills_ingested on brokerage.fills (ingested_at);
create index if not exists ix_cash_account on brokerage.cash_movements (account_id);
