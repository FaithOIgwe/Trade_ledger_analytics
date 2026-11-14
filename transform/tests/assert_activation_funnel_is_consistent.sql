-- Two things that must always hold for the activation funnel:
--   1. no stage is bigger than the cohort it came from
--   2. the cohorts add up to exactly the number of accounts (each account
--      counted once, none lost)

with funnel as (
    select * from {{ ref('rpt_account_activation') }}
),

totals as (
    select
        (select sum(accounts_opened) from funnel)           as opened_in_funnel,
        (select count(*) from {{ ref('dim_accounts') }})    as accounts
)

select cohort_month::text as failing_row, 'stage larger than cohort' as reason
from funnel
where accounts_funded > accounts_opened
   or accounts_traded > accounts_opened

union all

select 'all cohorts', 'cohorts do not add up to dim_accounts'
from totals
where opened_in_funnel is distinct from accounts
