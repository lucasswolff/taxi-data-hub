from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from scripts import snf_ingestion_weather

dataset_weather = Dataset("raw_weather")

@dag(
    dag_id="ingestion_weather_snf",
    description="Run python script to download from api weather data",
    schedule="0 10 * * *", #10 AM UTC
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=["python", "analytics", "taxi-data-hub", "snowflake"],
)
def ingestion_weather_snf():

    task_download = PythonOperator(
        task_id="download_weather_data",
        python_callable=snf_ingestion_weather.main,  # call the function
        outlets=[dataset_weather],
    )

    task_download

ingestion_weather_snf()