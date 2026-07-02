# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user first
USER airflow

# Install dbt core, dbt snowflake adapter, and the required Airflow providers
RUN pip install --no-cache-dir dbt-core dbt-snowflake