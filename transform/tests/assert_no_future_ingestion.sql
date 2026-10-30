-- fct_trades loads incrementally from max(ingested_at). One row stamped
-- in the future would quietly stop every later run from picking anything
-- up, so catch it here.

select exec_id, ingested_at
from {{ ref('fct_trades') }}
where ingested_at > now() + interval '5 minutes'
