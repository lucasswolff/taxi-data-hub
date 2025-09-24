from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from scripts import snf_ingestion_raw, local_ingestion_location

dataset_raw_taxi = Dataset("raw_taxi")
dataset_taxi_location = Dataset("file:///dbt_taxi_data_hub/seeds/seed_taxi_zone.csv")


@dag(
    dag_id="ingestion_taxi_snf",
    description="Run python script to download from url the taxi data",
    schedule="0 10 * * *", #10 AM UTC
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=["python", "analytics", "taxi-data-hub", "snowflake"],
)
def ingestion_taxi_snf():

    #calls the function to ingest parquet files from taxi
    task_download_raw = PythonOperator(
        task_id="download_taxi_raw_data",
        python_callable=snf_ingestion_raw.main,  
        outlets=[dataset_raw_taxi],
    )

    #calls the function to ingest location files from taxi zones
    task_download_location = PythonOperator(
        task_id="download_taxi_location_data",
        python_callable=local_ingestion_location.main, 
        outlets=[dataset_taxi_location],
    )

    task_download_raw
    task_download_location

ingestion_taxi_snf()