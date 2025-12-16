### Airflow

1. **Check data**: Verifies if there are transactions for the previous day in MySQL.
2. **Extract data**: If available, extracts the transactions and saves them to the data lake.
3. **Convert currency**: Converts the prices from USD to EUR using the exchange rate from the NiFi API.
4. **Load data**: Stores the converted data in the data lake in a standardized format.

### SQL

-   check the SQL/sensores.sql file with the queries

### Spark

-   check the Spark/readings.py file with the python code

### Bash

The command find "$DIRECTORY" -type f -mtime +180 -exec rm -f {} \; does the following:

1. Finds files (-type f) in the specified directory ("$DIRECTORY").
2. Filters files modified more than 180 days ago (-mtime +180).
3. Deletes each found file with the rm -f command.

### Data Building Tool - dbt

1. dbt (Data Build Tool) is an open-source tool for transforming data inside a data warehouse, using SQL and software engineering best practices.
2. It allows users to define data models as SQL queries, which dbt compiles and executes to create tables or views in the data warehouse.

### Python (Database extraction)

-   Summary of the Logic:

    1. Airflow runs the DAG every hour.
    2. SQL queries are generated to extract data in 10-minute intervals.
    3. Multiprocessing is used to run queries in parallel.
    4. The results of the queries are combined into a single DataFrame.
    5. The combined DataFrame is saved as a Parquet file for later analysis and consumption.

