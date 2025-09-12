from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dbt_pipeline",
    description="Run dbt models with Airflow",
    schedule="@daily",  
    start_date=datetime(2025, 9, 11),
    catchup=False,
    tags=["dbt", "analytics", "taxi-data-hub"],
) as dag:

    # Run dbt deps (to install packages)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /usr/local/airflow/dbt_taxi_data_hub && dbt deps --profiles-dir ..",
    )

    # Run dbt seed
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /usr/local/airflow/dbt_taxi_data_hub && dbt seed --profiles-dir ..",
    )

    # Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /usr/local/airflow/dbt_taxi_data_hub && dbt run --profiles-dir ..",
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /usr/local/airflow/dbt_taxi_data_hub && dbt test --profiles-dir ..",
    )

    # Task dependencies
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test