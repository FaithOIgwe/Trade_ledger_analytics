-- Which days had an unusual number of duplicate execution reports?
-- Compares each day to its own trailing 14-day average and returns the
-- days that ran at more than double it. `dbt compile` writes the runnable
-- SQL to target/compiled/.

with daily as (

    select
        ingested_date,
        duplicate_report_rate,
        avg(duplicate_report_rate) over (
            order by ingested_date
            rows between 14 preceding and 1 preceding
        ) as trailing_avg_rate
    from {{ ref('rpt_feed_quality_daily') }}

)

select *
from daily
where trailing_avg_rate > 0
  and duplicate_report_rate > 2 * trailing_avg_rate
order by ingested_date
