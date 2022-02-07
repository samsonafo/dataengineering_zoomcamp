import os
from datetime import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq 

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "healthy-fuze-339218")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_healthy-fuze-339218")

dataset_file = "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"  #change this to the fhv datafile
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')

path_to_creds = f"{path_to_local_home}/google_credentials.json"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    #"execution_date": datetime(2020, 12, 31),
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_fhv",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    #schedule_interval="0 6 2 * *",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=10,
    tags=['dtc-de-fhv'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command= f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    remove_temporary_csv = BashOperator(
        task_id = "remove_temp_files_docker_csv",
        bash_command = f"rm {path_to_local_home}/{dataset_file}"
    )

    remove_temporary_parquet = BashOperator(
        task_id = "remove_temp_files_docker_parquet",
        bash_command = f"rm {path_to_local_home}/{parquet_file}"

    )

    download_dataset_task >> format_to_parquet >> local_to_gcs_task >> remove_temporary_csv >> remove_temporary_parquet  