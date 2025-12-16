from airflow import dag, task, task_group
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.mysql.sensors.mysql import MySqlSensor
from functions import extract_transactions_from_mysql, convert_usd_to_eur, save_to_data_lake

db_config = Variable.get("ny_store_db_config", deserialize_json=True) 
raw_bucket = Variable.get("raw_bucket") #, default_var="/data_lake/raw/transactions")
std_bucket = Variable.get("std_bucket") #, default_var="/data_lake/standardized/transactions")

nifi_config = Variable.get("nifi_usd_eur_config", deserialize_json=True) 


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id="ny_store_transactions_ingestion",
    description="Ingest store transactions",
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1
)

def store_transactions_ingestion():


    @task_group(group_id="ny_data_flow")
    def ny_data_flow():

        check_data_availability = MySqlSensor(
            task_id="check_data_availability",
            conn_id="mysql_conn",
            sql="SELECT COUNT(*) FROM transactions WHERE timestamp >= CURDATE() - INTERVAL 1 DAY;",
            poke_interval=300,
            timeout=600,
            mode="reschedule"
        )

        @task(task_id="extract_mysql")
        def extract_task(**kwargs):
            """
            Extract transactions from the MySQL database to the raw_bucket.
            """
            return extract_transactions_from_mysql(
                execution_date= kwargs["execution_date"],
                db_config=db_config,
                raw_base_path=raw_bucket
            )

        @task(task_id="convert_usd_to_eur")
        def convert_task(**kwargs):
            """
            Convert prices from USD to EUR using the NiFi API.
            """
            return convert_usd_to_eur(
                execution_date= kwargs["execution_date"],
                raw_base_path=raw_bucket,
                std_base_path=std_bucket,
                nifi_config=nifi_config
            )

        @task(task_id="save_to_data_lake")
        def load_task(**kwargs):
            """
            Load the transformed data to the data lake.
            """
            save_to_data_lake(
                execution_date=kwargs["execution_date"],
                std_base_path=std_bucket
            )

        check_data_availability >> extract_task() >> convert_task() >> load_task()

    ny_data_flow()

store_transactions_ingestion()
