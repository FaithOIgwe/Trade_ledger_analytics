-- Retail accounts here can't short. A negative position means a sell was
-- recorded without the buy, or a reversal was applied twice.

select account_id, symbol, position_date, quantity
from {{ ref('fct_daily_positions') }}
where quantity < 0
