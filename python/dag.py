from airflow.decorators import dag, task
from datetime import datetime, timedelta
from functions import extract_hourly_data

@dag(
    'hourly_data_extraction',
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG para extração de dados horária dos sensores',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2020, 1, 1),
)
def hourly_data_extraction_dag():
    @task
    def run_extraction(**execution_date):
        extract_hourly_data(execution_date)

    run_extraction()
