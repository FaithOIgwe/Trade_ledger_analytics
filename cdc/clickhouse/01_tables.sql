-- Orders and fills streamed from Postgres (Debezium -> Redpanda -> here).
--
-- Pattern per table:
--   kafka_*      Kafka engine table, reads the topic, stores nothing
--   mv_*         materialized view, parses each message and inserts it
--   <table>      ReplacingMergeTree keyed on the source primary key. Every
--                change lands as a new row; the newest version (by the
--                source commit time) wins when parts merge or when you
--                query with FINAL. Deletes arrive with __deleted = 'true'.
--
-- docker compose exec clickhouse clickhouse-client -u analyst --password analyst --multiquery < cdc/clickhouse/01_tables.sql

create database if not exists brokerage;

-- ---------------- orders ----------------

create table if not exists brokerage.orders
(
    order_id        UUID,
    client_order_id String,
    account_id      UUID,
    symbol          LowCardinality(String),
    side            LowCardinality(String),
    order_type      LowCardinality(String),
    qty             Decimal(18, 4),
    limit_price     Nullable(Decimal(18, 4)),
    status          LowCardinality(String),
    submitted_at    DateTime64(6, 'UTC'),
    updated_at      DateTime64(6, 'UTC'),
    _version        UInt64,
    _is_deleted     UInt8,
    _landed_at      DateTime64(3, 'UTC') default now64(3)
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by order_id;

create table if not exists brokerage.kafka_orders
(
    order_id String, client_order_id String, account_id String, symbol String,
    side String, order_type String, qty String, limit_price Nullable(String),
    status String, submitted_at String, updated_at String,
    __op String, __source_ts_ms UInt64, __deleted String
)
engine = Kafka
settings kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'cdc.brokerage.orders',
         kafka_group_name = 'clickhouse_orders',
         kafka_format = 'JSONEachRow',
         kafka_skip_broken_messages = 10;

create materialized view if not exists brokerage.mv_orders to brokerage.orders as
select
    toUUID(order_id)                                   as order_id,
    client_order_id,
    toUUID(account_id)                                 as account_id,
    symbol, side, order_type,
    toDecimal64(qty, 4)                                as qty,
    toDecimal64OrNull(limit_price, 4)                  as limit_price,
    status,
    parseDateTime64BestEffort(submitted_at, 6, 'UTC')  as submitted_at,
    parseDateTime64BestEffort(updated_at, 6, 'UTC')    as updated_at,
    __source_ts_ms                                     as _version,
    __deleted = 'true'                                 as _is_deleted
from brokerage.kafka_orders;

-- ---------------- fills ----------------

create table if not exists brokerage.fills
(
    fill_id          UUID,
    exec_id          String,
    order_id         UUID,
    qty              Decimal(18, 4),
    price            Decimal(18, 4),
    executed_at      DateTime64(6, 'UTC'),
    ingested_at      DateTime64(6, 'UTC'),
    is_reversal      UInt8,
    reverses_exec_id Nullable(String),
    _version         UInt64,
    _is_deleted      UInt8,
    _landed_at       DateTime64(3, 'UTC') default now64(3)
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by fill_id;

create table if not exists brokerage.kafka_fills
(
    fill_id String, exec_id String, order_id String, qty String, price String,
    executed_at String, ingested_at String, is_reversal Bool,
    reverses_exec_id Nullable(String),
    __op String, __source_ts_ms UInt64, __deleted String
)
engine = Kafka
settings kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'cdc.brokerage.fills',
         kafka_group_name = 'clickhouse_fills',
         kafka_format = 'JSONEachRow',
         kafka_skip_broken_messages = 10;

create materialized view if not exists brokerage.mv_fills to brokerage.fills as
select
    toUUID(fill_id)                                    as fill_id,
    exec_id,
    toUUID(order_id)                                   as order_id,
    toDecimal64(qty, 4)                                as qty,
    toDecimal64(price, 4)                              as price,
    parseDateTime64BestEffort(executed_at, 6, 'UTC')   as executed_at,
    parseDateTime64BestEffort(ingested_at, 6, 'UTC')   as ingested_at,
    toUInt8(is_reversal)                               as is_reversal,
    reverses_exec_id,
    __source_ts_ms                                     as _version,
    __deleted = 'true'                                 as _is_deleted
from brokerage.kafka_fills;
