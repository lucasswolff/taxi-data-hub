from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset

dataset_raw_taxi = Dataset("file:///raw_data/*.parquet")
dataset_taxi_location = Dataset("file:///dbt_taxi_data_hub/seeds/seed_taxi_zone.csv")
dataset_weather = Dataset("file:///weather/*.parquet")

@dag(
    dag_id="dbt_pipeline",
    description="Run dbt models with Airflow",
    schedule=[dataset_raw_taxi, dataset_taxi_location, dataset_weather],
    start_date=datetime(2025, 9, 11),
    catchup=False,
    params={"dbt_full_refresh": False},
    tags=["dbt", "analytics", "taxi-data-hub"],
)
def dbt_pipeline():
    
    # Run dbt clean to avoid any cached files 
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command="cd /usr/local/airflow/dbt_taxi_data_hub && dbt clean --profiles-dir ..",
    )

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

    ## Run and test dbt models

    # run src
    dbt_run_src = BashOperator(
        task_id="dbt_run_src",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt run --select models/src --profiles-dir .. "
            ),
    )

    # run stg
    dbt_run_stg = BashOperator(
        task_id="dbt_run_stg",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt run --select models/stg --profiles-dir .. "
            ),
    )

    # run dim
    dbt_run_dim = BashOperator(
        task_id="dbt_run_dim",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt run --select models/dim --profiles-dir .. "
            ),
    )

    # test dim
    dbt_test_dim = BashOperator(
        task_id="dbt_test_dim",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt test --select models/dim --profiles-dir .. "
            ),
    )

    # run int
    dbt_run_int = BashOperator(
        task_id="dbt_run_int",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt run --select models/int --profiles-dir .. "
            ),
    )

    # test int
    dbt_test_int = BashOperator(
        task_id="dbt_test_int",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt test --select models/int --profiles-dir .. "
            ),
    )

    # run fct
    dbt_run_fct = BashOperator(
        task_id="dbt_run_fct",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt run --select models/fct --profiles-dir .. "
            "{% if params.dbt_full_refresh %}--full-refresh{% endif %}"
            ),
    )

    # test fct
    dbt_test_fct = BashOperator(
        task_id="dbt_test_fct",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt test --select models/fct --profiles-dir .. "
            ),
    )

    # run mart
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            "cd /usr/local/airflow/dbt_taxi_data_hub && "
            "dbt run --select models/mart --profiles-dir .. "
            "{% if params.dbt_full_refresh %}--full-refresh{% endif %}"
            ),
    )

    # Task dependencies
    dbt_clean >> dbt_deps >> dbt_seed >> dbt_run_src >> dbt_run_stg >> dbt_run_dim >> dbt_test_dim >> dbt_run_int >> dbt_test_int >> dbt_run_fct >> dbt_test_fct >> dbt_run_mart


dbt_pipeline()
