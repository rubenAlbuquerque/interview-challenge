
from pyspark.sql import SparkSession

# Utilizacao do schedule do airflow para executar a dag de hora em hora

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
    start_date=datetime(2025, 1, 1),
)
def hourly_data_extraction_dag():
    @task
    def run_extraction(**execution_date):
        extract_hourly_data(execution_date)

    run_extraction()


# 1. Paralelismo com Multiprocessamento ou Multithreading
# Utilizar o paralelismo para executar múltiplas consultas simultaneamente

import multiprocessing
import pyodbc
from datetime import datetime, timedelta

def fetch_data(query):
    
    spark = SparkSession.builder.appName("Data Extraction").getOrCreate()

    conn = pyodbc.connect('DSN=your_dsn_name;UID=your_user;PWD=your_password')
    df = conn.cursor().execute(query).fetchall()

    df_spark = spark.createDataFrame(df, ["timestamp", "sensor_name", "value"])
    return df_spark

# Example
# queries = [
#     "SELECT * FROM table WHERE timestamp >= '2020-01-01 00:00:00' AND timestamp < '2020-01-01 00:10:00'",
#     "SELECT * FROM table WHERE timestamp >= '2020-01-01 00:10:00' AND timestamp < '2020-01-01 00:20:00'",
#     # ...
# ]

def generate_queries(start_time, interval_minutes=10):
    queries = []
    end_time = start_time + timedelta(hours=1)
    
    current_time = start_time
    while current_time < end_time:
        
        next_time = current_time + timedelta(minutes=interval_minutes)
        query = f"SELECT * FROM table WHERE timestamp >= '{current_time.strftime('%Y-%m-%d %H:%M:%S')}' AND timestamp < '{next_time.strftime('%Y-%m-%d %H:%M:%S')}'"
        queries.append(query)
        current_time = next_time
    
    return queries


def extract_hourly_data(execution_date):
    spark = SparkSession.builder.appName("Data Extraction").getOrCreate()
    
    queries = generate_queries(execution_date)

    with multiprocessing.Pool(processes=4) as pool:
        results = pool.map(fetch_data, queries)

    combined_df = results[0]
    for df in results[1:]:
        combined_df = combined_df.union(df)
    combined_df.write.parquet(f"/path/to/data_lake/sensors_data_{execution_date.strftime('%Y-%m-%d_%H-%M-%S')}")




# 2. Configuração do Produtor Kafka

