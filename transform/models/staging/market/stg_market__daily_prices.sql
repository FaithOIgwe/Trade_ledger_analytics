select
    upper(symbol)                as symbol,
    bar_date,
    open::numeric(18, 4)         as open_price,
    high::numeric(18, 4)         as high_price,
    low::numeric(18, 4)          as low_price,
    close::numeric(18, 4)        as close_price,
    volume,
    source                       as price_source,
    loaded_at
from {{ source('market', 'market_prices') }}
