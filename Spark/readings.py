import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()

df = spark.read.csv("sensor_readings.csv", header=True, inferSchema=True)


print(f"Total number of rows: {df.count()}")

print(f"Number of distinct sensors: {df.select('name').distinct().count()}")

print(f"Number of rows for PPL340: {df.filter(df.name == 'PPL340').count()}")

# The number of rows by year for the sensor PPL340;
print(df.filter(df.name == 'PPL340').groupBy('year').count().show())


# Average number of readings by year for the sensor PPL340;
yearly_counts = df.filter(df.name == "PPL340").groupBy("year").count()
average_readings_per_year = yearly_counts.select(F.avg("count")).collect()[0][0]
print(average_readings_per_year)


# For PPL340, Identify the years in which the number of readings is less than the average;
years_below_avg = yearly_counts.filter(yearly_counts["count"] < average_readings_per_year).select("year")

print(years_below_avg.show())
