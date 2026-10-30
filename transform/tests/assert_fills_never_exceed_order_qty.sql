-- An order can't be filled for more than was asked.
-- If this fails it's usually duplicate execution reports getting through staging.

select
    order_id,
    order_qty,
    filled_qty
from {{ ref('fct_orders') }}
where filled_qty > order_qty
