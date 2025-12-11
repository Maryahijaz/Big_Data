from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timezone
import pandas as pd
import os
import re
import kaggle
import pendulum
import subprocess

# -------------------------------
# Default DAG arguments
# -------------------------------
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1)  # Start date of DAG (yesterday)
}

# -------------------------------
# Define the DAG
# -------------------------------
with DAG(
    dag_id='server_logs',
    default_args=default_args,
    schedule='@hourly',   # Run every hour
    catchup=False          # Do not backfill old runs
) as dag:

    # -------------------------------
    # Task 1: Download logs from Kaggle
    # -------------------------------
    @task()
    def extract_logs_data():
        os.environ['KAGGLE_CONFIG_DIR'] = '/home/bigdata/Desktop/project'
        kaggle.api.authenticate()  # Authenticate with Kaggle API
        kaggle.api.dataset_download_files(
            'vishnu0399/server-logs',  # Dataset name
            path='/home/bigdata/Desktop/project/data',
            unzip=True                  # Automatically unzip
        )
        return '/home/bigdata/Desktop/project/data/logfiles.log'  # Return path to log file

    # -------------------------------
    # Task 2: Check if file exists and is not empty
    # -------------------------------
    @task()
    def check_file(file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        if os.path.getsize(file_path) == 0:
            raise ValueError(f"File is empty: {file_path}")
        return file_path

    # -------------------------------
    # Task 3: Parse log lines into a structured DataFrame
    # -------------------------------
    @task()
    def parse_logs(file_path: str):
        # Regex pattern to extract fields from Apache/Nginx style logs
        pattern = re.compile(
            r'(?P<ip>\S+) '
            r'(?P<identity>\S+) '
            r'(?P<user>\S+) '
            r'\[(?P<timestamp>.*?)\] '
            r'"(?P<method>\S+) '
            r'(?P<path>\S+) '
            r'(?P<protocol>[^"]+)" '
            r'(?P<status>\d{3}) '
            r'(?P<bytes>\S+) '
            r'"(?P<referer>[^"]*)" '
            r'"(?P<user_agent>[^"]*)" '
            r'(?P<response_time>\d+)'
        )

        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = pattern.match(line)
                if match:
                    data = match.groupdict()
                    dt_local = datetime.strptime(data['timestamp'], "%d/%b/%Y:%H:%M:%S %z")
                    timestamp_utc_naive = dt_local.astimezone(timezone.utc).replace(tzinfo=None)
                    records.append({
                        "ip": data["ip"],
                        "remote_log_name": None if data["identity"] == '-' else data["identity"],
                        "user_id": None if data["user"] == '-' else data["user"],
                        "timestamp_utc": timestamp_utc_naive,
                        "request_type": data["method"],
                        "api": data["path"],
                        "protocol": data["protocol"],
                        "status_code": int(data["status"]),
                        "bytes_sent": int(data["bytes"]) if data["bytes"] != '-' else 0,  # ADD THIS LINE
                        "referrer": None if data["referer"] == '-' else data["referer"],
                        "user_agent": data["user_agent"],
                        "response_time": int(data["response_time"]),
                    })

        # Convert list of records to a Pandas DataFrame
        df = pd.DataFrame(records)

        # Save parsed data to Parquet (Spark-compatible)
        output_file = "/home/bigdata/Desktop/project/data/parsed_logs.parquet"
        df.to_parquet(output_file, index=False, engine='pyarrow', coerce_timestamps='ms')

        return output_file

    # -------------------------------
    # Task 4: Filter logs by the current hour
    # -------------------------------
    @task()
    def filter_hour(file_path: str, **context):
        df = pd.read_parquet(file_path)
        start = pd.to_datetime(context['data_interval_start'], utc=True)
        end = pd.to_datetime(context['data_interval_end'], utc=True)
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)

        # Keep only logs for this DAG interval (current hour)
        hourly = df[(df['timestamp_utc'] >= start) & (df['timestamp_utc'] < end)]
        filtered_file = "/tmp/hourly_logs.parquet"
        hourly.to_parquet(filtered_file, index=False)
        return filtered_file

    # -------------------------------
    # Task 5: Run Spark job for hourly processing
    # -------------------------------
    @task()
    def run_spark_processing(hourly_file: str):
        spark_script = "/home/bigdata/Desktop/project/logs_pyspark.py"

        # Check if Spark script exists
        if not os.path.isfile(spark_script):
            raise FileNotFoundError(f"Spark script not found: {spark_script}")

        # Spark submit command
        cmd = [
            "spark-submit",
            "--master", "local[*]",  # Use all cores
            "--driver-memory", "2g",
            "--jars", "/home/bigdata/Desktop/project/postgresql-42.6.2.jar",
            spark_script,
            hourly_file
        ]

        try:
            # Run the Spark job
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("=== Spark STDOUT ===\n", result.stdout)
            print("=== Spark STDERR ===\n", result.stderr)
        except subprocess.CalledProcessError as e:
            # Raise detailed error if Spark fails
            raise Exception(f"Spark job failed with code {e.returncode}\nSTDOUT:\n{e.stdout}\nSTDERR:\n{e.stderr}")

        return "Spark processing completed successfully"

    # -------------------------------
    # DAG task dependencies
    # -------------------------------
    logs_file = extract_logs_data()
    checked_file = check_file(logs_file)
    parsed_file = parse_logs(checked_file)
    hourly_file = filter_hour(parsed_file)
    spark_result = run_spark_processing(hourly_file)
