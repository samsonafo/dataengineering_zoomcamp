import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_2_bq_dag_fhv",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    
    #create a folder for all the repos seperately.
    gcs_2_gcs_fhv_task = GCSToGCSOperator(
        task_id="gcs_2_gcs_fhv_task",
        source_bucket=BUCKET,
        source_object='raw/fhv_tripdata*.parquet',
        destination_bucket=BUCKET,
        destination_object="fhv_trip_data/",
        move_object=True,
    )

     #create an External table from the files in the datalake above
    gcs_2_bq_ext_fhv_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_fhv_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_fhv_tripdata",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/fhv_trip_data/*"],
            },
        },
    )

#create a partitioned table from the table created above.
    CREATE_BQ_TBL_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.fhv_tripdata_partitioned_clustered \
        PARTITION BY DATE(dropoff_datetime) \
        CLUSTER BY dispatching_base_num AS \
        SELECT * FROM {BIGQUERY_DATASET}.external_fhv_tripdata;"


    bq_ext_2_part_task = BigQueryInsertJobOperator(
        task_id="bq_ext_2_part_task",
        configuration={
            "query" : {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql" : False,
            }
        },

    )


gcs_2_gcs_fhv_task >> gcs_2_bq_ext_fhv_task


#gcs_2_gcs -- helps re-organise our files to a better folder structure
#airflow already provides google integrated libraries