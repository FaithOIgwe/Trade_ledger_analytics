FROM apache/airflow:2.10.5-python3.11

# dbt and the generator get their own virtualenv so their dependencies
# can't fight with Airflow's.
USER root
RUN python -m venv /opt/dbt_venv && chown -R airflow: /opt/dbt_venv
USER airflow
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt
