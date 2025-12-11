import sys
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, to_timestamp, hour, avg, desc, count, sum as _sum, lit, date_trunc
from pyspark.sql.types import IntegerType, StringType

# -------------------------------
# Initialize Spark
# -------------------------------
findspark.init()
spark = SparkSession.builder \
    .appName("ServerLogsHourly") \
    .config("spark.driver.memory", "2g") \
    .config("spark.jars", "/home/bigdata/Desktop/project/postgresql-42.6.2.jar") \
    .getOrCreate()

# -------------------------------
# Read Parquet file from argument
# -------------------------------
if len(sys.argv) < 2:
    raise ValueError("Missing argument: parquet file path")
parquet_path = sys.argv[1]
if not os.path.exists(parquet_path):
    raise FileNotFoundError(f"{parquet_path} not found!")

df = spark.read.parquet(parquet_path)

# -------------------------------
# Ensure correct types
# -------------------------------
df = df.withColumn("status_code", col("status_code").cast(IntegerType())) \
       .withColumn("response_time", col("response_time").cast(IntegerType())) \
       .withColumn("timestamp_utc", to_timestamp(col("timestamp_utc"), "yyyy-MM-dd HH:mm:ss"))

# -------------------------------
# Rename columns for consistency
# -------------------------------
df = df.withColumnRenamed("user_id", "user") \
       .withColumnRenamed("request_type", "method") \
       .withColumnRenamed("api", "endpoint") \
       .withColumnRenamed("status_code", "status") \
       .withColumnRenamed("referrer", "referer") \
       .withColumnRenamed("timestamp_utc", "timestamp_clean")

# -------------------------------
# Ensure method column is string
# -------------------------------
df = df.withColumn("method", col("method").cast(StringType()))

# -------------------------------
# Categorize HTTP methods & errors
# -------------------------------
df = df.withColumn(
    "method_category",
    when(upper(col("method")).isin("GET","HEAD"), "Read")
    .when(upper(col("method")).isin("POST","PUT","PATCH"), "Write")
    .when(upper(col("method")) == "DELETE", "Delete")
    .otherwise("Other")
)

df = df.withColumn(
    "error_category",
    when((col("status") >= 400) & (col("status") < 500), "Client Error")
    .when(col("status") >= 500, "Server Error")
    .otherwise("OK")
)

# -------------------------------
# Create hourly aggregated stats
# -------------------------------
df = df.withColumn("hour_timestamp", date_trunc("hour", col("timestamp_clean")))

logs_aggregated = df.groupBy("hour_timestamp").agg(
    count("*").alias("total_requests"),
    avg("response_time").alias("avg_response_time"),
    _sum(when((col("status") >= 400) & (col("status") < 500), 1).otherwise(0)).alias("client_errors"),
    _sum(when(col("status") >= 500, 1).otherwise(0)).alias("server_errors")
)

# -------------------------------
# Extract only errors (4xx & 5xx)
# -------------------------------
logs_errors = df.filter(col("status") >= 400)

# -------------------------------
# Additional aggregations
# -------------------------------
top_endpoints = df.groupBy("endpoint").count().orderBy(desc("count"))
top_ips = df.groupBy("ip").count().orderBy(desc("count"))

# Suspicious IPs: count > 2 * avg_count
ip_counts = df.groupBy("ip").count()
avg_count = ip_counts.agg(avg("count")).first()[0] or 0
suspicious_ips = ip_counts.filter(col("count") > 2 * lit(avg_count)).orderBy(desc("count"))

# -------------------------------
# PostgreSQL connection info
# -------------------------------
pg_url = "jdbc:postgresql://localhost:5432/server_logs"
pg_user = "postgres"
pg_password = "marya"

def write_to_postgres(df_spark, table_name):
    df_spark.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", table_name) \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

# -------------------------------
# Write all tables to PostgreSQL
# -------------------------------
write_to_postgres(df, "logs_raw")
write_to_postgres(logs_aggregated, "logs_aggregated")
write_to_postgres(logs_errors, "logs_errors")
write_to_postgres(top_endpoints, "logs_top_endpoints")
write_to_postgres(top_ips, "logs_top_ips")
write_to_postgres(suspicious_ips, "logs_suspicious_ips")

# -------------------------------
# Stop Spark session
# -------------------------------
spark.stop()
print("All DataFrames written to PostgreSQL and Spark stopped.")

