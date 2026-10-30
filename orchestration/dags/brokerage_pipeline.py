"""
Hourly batch pipeline.

    simulate_activity ─┐
                       ├─> source_freshness ─> dbt_snapshot ─> dbt_build
    load_prices ───────┘

source_freshness is the gate: if orders or fills are more than 6 hours
stale, dbt exits non-zero and nothing downstream rebuilds on bad data.
dbt build runs models and tests together, so a failed test on staging
stops the marts from being rebuilt on top of it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

VENV = "/opt/dbt_venv/bin"
PROJECT = "/opt/project"
DBT = f"cd {PROJECT}/transform && {VENV}/dbt"


def notify_failure(context):
    # Swap this for Slack/email in a real setup. Logging keeps the demo
    # self-contained.
    ti = context["task_instance"]
    print(f"[ALERT] {ti.dag_id}.{ti.task_id} failed for {context['logical_date']}. Log: {ti.log_url}")


default_args = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="brokerage_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "brokerage"],
) as dag:

    # stands in for the trading system doing its thing between runs
    simulate_activity = BashOperator(
        task_id="simulate_activity",
        bash_command=f"{VENV}/python {PROJECT}/generator/generate.py tick --orders 80",
    )

    load_prices = BashOperator(
        task_id="load_prices",
        bash_command=f"{VENV}/python {PROJECT}/ingestion/load_market_prices.py --days 7",
    )

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT} source freshness",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --exclude resource_type:snapshot",
    )

    [simulate_activity, load_prices] >> source_freshness >> dbt_snapshot >> dbt_build
