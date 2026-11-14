-- How well does each signup cohort convert, and is it getting faster?
-- Recent cohorts have had less time to fund and trade, so read the last
-- month or two as "so far", not as a drop.

select
    cohort_month,
    accounts_opened,
    funded_rate,
    traded_rate,
    median_days_to_first_trade
from {{ ref('rpt_account_activation') }}
order by cohort_month
