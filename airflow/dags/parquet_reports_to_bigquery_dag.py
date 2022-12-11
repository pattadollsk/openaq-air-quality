import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

from google.cloud import storage
# Reference: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "openaq") # Added default value to second argument in case failure

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def create_bigquery_tbl(
    dag,
    tableId,
    prefix,

):
    with dag:
    # Reference for "BigQueryCreateExternalTableOperator":
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html
        gcs_to_bq_tbl_task = BigQueryCreateExternalTableOperator(
            task_id="gcs_to_bq_tbl_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{tableId}", # choose our table name
                },
                # Reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externaldataconfiguration
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{prefix}"],
                },
            },
        )
        gcs_to_bq_tbl_task

parquet_report_pm25 = DAG(
    dag_id="parquet_report_pm25_to_bq_dag",
    schedule_interval="@once",
    start_date=datetime(2020, 1, 25),
    end_date=datetime(2020, 1, 25),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['parquet_report_pm25'],
)

create_bigquery_tbl(
    dag=parquet_report_pm25,
    tableId='global_pm25',
    prefix='report/parquet/pm25/*.parquet'   
)

parquet_report_all_pm = DAG(
    dag_id="parquet_report_all_pm_to_bq_dag",
    schedule_interval="@once",
    start_date=datetime(2020, 1, 25),
    end_date=datetime(2020, 1, 25),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['parquet_report_all_pm'],
)

create_bigquery_tbl(
    dag=parquet_report_all_pm,
    tableId='global_all_pm',
    prefix='report/parquet/all_pm/*.parquet'   
)