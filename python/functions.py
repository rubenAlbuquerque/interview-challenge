import pyodbc
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import multiprocessing

spark = SparkSession.builder.appName('Data Extraction').getOrCreate()

def get_sensor_names(start_time, end_time):
    query = f"""
    SELECT DISTINCT sensor_name 
    FROM your_table 
    WHERE timestamp >= '{start_time}' AND timestamp < '{end_time}'
    """
    
    # conn = pyodbc.connect('DSN=your_dsn_name;UID=your_username;PWD=your_password')
    sensor_names_df = spark.read.jdbc(
        url='jdbc:odbc:your_dsn',
        table=f"({query}) as t",
        properties={'user': 'your_user', 
                    'password': 'your_password'}
    )
    
    return [row['sensor_name'] for row in sensor_names_df.collect()]

def fetch_and_save_data(start_time, end_time, sensor_name):
    query = f"""
    SELECT * 
    FROM your_table 
    WHERE timestamp >= '{start_time}' 
      AND timestamp < '{end_time}' 
      AND sensor_name = '{sensor_name}'
    """
    
    df = spark.read.jdbc(
        url='jdbc:odbc:your_dsn',
        table=f"({query}) as t",
        properties={'user': 'your_user', 'password': 'your_password'}
    )
    
    df.write.parquet(f"/path/to/data_lake/raw/{start_time.strftime('%Y-%m-%d_%H:%M:%S')}_{sensor_name}.parquet")


def extract_hourly_data(start_time):
    end_time = start_time + timedelta(hours=1)
    sensor_names = get_sensor_names(start_time, end_time)
    
    pool = multiprocessing.Pool(processes=4)
    for sensor_name in sensor_names:
        pool.apply_async(fetch_and_save_data, args=(start_time, end_time, sensor_name))
    
    pool.close()
    pool.join()



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
