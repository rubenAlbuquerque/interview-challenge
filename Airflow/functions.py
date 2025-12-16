from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import requests
import logging

# RAW_BASE_PATH = "/data_lake/raw/transactions"
# STD_BASE_PATH = "/data_lake/standardized/transactions"


def get_spark():
    """
    Initialize and return a Spark session.
    """
    spark = SparkSession.builder \
        .appName("NY_Store_Extraction") \
        .getOrCreate()
    
    return spark

def read_from_mysql(jdbc_url, query, db_config):
    """
    Reads data from a MySQL database using Spark JDBC.
    """
    spark = get_spark()

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("user", db_config["user"]) \
        .option("password", db_config["password"]) \
        .load()

    return df




def extract_transactions_from_mysql(execution_date, db_config, raw_base_path):
    """
    Extract transactions from MySQL for the previous day using Spark JDBC
    """
    spark = get_spark()

    date_str = execution_date.strftime("%Y-%m-%d")

    jdbc_url = db_config["jdbc_url"]

    query = f"""SELECT * FROM transactions
                WHERE timestamp >= '{date_str}'
                AND timestamp < DATE_ADD('{date_str}', 1)"""

    df = read_from_mysql(jdbc_url, query, db_config)

    df.write.mode("overwrite").parquet(f"{raw_base_path}/{date_str}")


def convert_usd_to_eur(execution_date, raw_base_path, std_base_path, nifi_config, **kwargs):
    """
    Convert product_price from USD to EUR using exchange rate
    obtained from NiFi REST API
    """

    spark = get_spark()
    date_str = execution_date.strftime("%Y-%m-%d")
    df = spark.read.parquet(f"{raw_base_path}/{date_str}")

    response = requests.get(nifi_config["endpoint"])
    exchange_rate = response.json().get("rate", 1.0)
    
    df_converted = df.withColumn("product_price_eur", F.col("product_price") * F.lit(exchange_rate))

    df_converted.write.mode("overwrite").parquet(f"{std_base_path}/{date_str}")

