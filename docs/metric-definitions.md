# Metric definitions

The reporting models are small on purpose. Most arguments about numbers are really arguments about definitions, so each metric here says what it counts, at what grain, and what it leaves out. The column-level versions live next to the models in `transform/models/reporting/_reporting.yml` and show up in `dbt docs`.

## Daily trading summary (`rpt_daily_trading_summary`)

Grain: one row per trading day, UTC.

| metric | definition | what it leaves out, and why |
| --- | --- | --- |
| `active_accounts` | distinct accounts with at least one execution that day | accounts whose only row that day is a reversal. Having a trade busted is not activity. |
| `executions` | execution rows for the day | reversal rows, which are counted in `reversals` |
| `gross_notional_usd` | sum of quantity x price, buys and sells both positive | reversals. Netting them in would make a day with a large busted trade look quieter than it was. |
| `net_buy_notional_usd` | signed notional: buys positive, sells negative | nothing. Reversals are included here so the number ties to positions. |
| `late_arrival_rate` | rows that arrived more than an hour after executing, over all rows for the day | |

`active_accounts` can't be added up across days to get weekly or monthly actives. A customer who trades on Monday and Thursday is one weekly active, not two. Anything above daily grain needs its own distinct count at that grain.

## Account activation (`rpt_account_activation`)

Grain: one row per signup cohort month.

The funnel is opened, then funded, then traded, and every account is counted once, in the month it was opened. `funded_rate` for the March cohort is "of the accounts opened in March, how many have ever deposited", not "how many accounts deposited in March". The second version mixes cohorts, moves around with signup volume, and can go above 100%.

Recent cohorts have had less time to convert, so their rates are "so far". Comparing a three-week-old cohort with a three-month-old one will always make the newer one look worse.

`median_days_to_first_trade` is a median because a few accounts that sit idle for months would drag a mean around. It only covers accounts that have traded.

## Feed quality (`rpt_feed_quality_daily`)

Grain: one row per ingestion day, UTC.

By ingestion date, not execution date, because a feed problem happens when the data arrives. A burst of duplicates on Wednesday about Monday's trades is a Wednesday problem.

| metric | definition |
| --- | --- |
| `executions_kept` | distinct executions first received that day, after deduplication |
| `duplicate_reports_dropped` | extra reports for those executions that staging discarded |
| `duplicate_report_rate` | dropped reports over all reports received (kept plus dropped) |
| `late_arrivals` | executions received more than an hour after they happened |
| `max_arrival_lag_minutes` | the single longest delay that day |

## How these are kept honest

Two tests tie the reporting layer back to the tables it was built from:

- `assert_daily_summary_ties_to_trades`: executions, reversals and net notional in the summary add up exactly to `fct_trades`.
- `assert_activation_funnel_is_consistent`: no funnel stage is bigger than its cohort, and the cohorts add up to exactly the number of accounts in `dim_accounts`.
