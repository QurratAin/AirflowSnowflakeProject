#syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.9-base

#ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

COPY requirements-snowpark.txt /tmp
RUN python -m pip install -r /tmp/requirements-snowpark.txt

RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake \
    && deactivate  