select
    account_id,
    trim(customer_name)   as customer_name,
    upper(country)        as country_code,
    lower(account_type)   as account_type,
    lower(status)         as account_status,
    lower(tier)           as account_tier,
    created_at,
    updated_at
from {{ source('brokerage', 'accounts') }}
