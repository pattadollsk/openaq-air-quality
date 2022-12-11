import os
import logging
import pathlib
import boto3

from datetime import datetime

from google.cloud import storage

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


# Define default arguments here:
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Reference: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-code-sample
def upload_to_gcs(
    bucket,
    object_name,
    local_file
):

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def boto3_s3_download_file(
    objects,
    source_bucket_name,
    prefix,

):
    source_bucket_key_list = objects

    local_path_folder_template = AIRFLOW_HOME + '/openaq_air_quality/' + prefix
    pathlib.Path(local_path_folder_template).mkdir(parents=True, exist_ok=True)

    for source_bucket_key in source_bucket_key_list:
        print(source_bucket_key, type(source_bucket_key))

        file_name = source_bucket_key.split('/')[-1]

        local_path_template = local_path_folder_template + file_name

        # s3.download_file will not create a directory, so a directory must be created ahead of time 
        s3 = boto3.client('s3')
        s3.download_file(source_bucket_name, source_bucket_key, local_path_template)

        gcs_path_template = 'raw/' + prefix + file_name

        upload_to_gcs(
            bucket=BUCKET,
            object_name=gcs_path_template,
            local_file=local_path_template
        )
    
    return local_path_folder_template


def download_s3_to_gcs(
    dag,
    source_bucket_name,
    prefix,
):
    with dag:
        list_keys_task = S3ListOperator(
            task_id="list_keys_task",
            bucket=source_bucket_name,
            prefix=prefix,
            delimiter='/',
            aws_conn_id='aws_default'
        )

        s3_to_gcs_task=PythonOperator(
            task_id="s3_to_gcs_task",
            python_callable=boto3_s3_download_file,
            op_kwargs={
                "objects": "{{ task_instance.xcom_pull('list_keys_task') }}",
                "source_bucket_name": source_bucket_name,
                "prefix": prefix,
            }           
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command="rm -r {{ task_instance.xcom_pull('s3_to_gcs_task') }}"
        )

        list_keys_task >> s3_to_gcs_task >> rm_task


openaq_s3_to_gcs_dag = DAG(
    dag_id="openaq_s3_to_gcs_dag",
    render_template_as_native_obj=True, # set True, or else s3ListOperator will return a string with `list` like structure
    schedule_interval="@daily",
    start_date=datetime(2020, 1, 19),
    end_date=datetime(2020, 1, 25),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['openaq_s3_to_gcs_dag'],
)

download_s3_to_gcs(
    dag=openaq_s3_to_gcs_dag,
    source_bucket_name='openaq-fetches',
    prefix='realtime-gzipped/{{ execution_date.strftime(\'%Y-%m-%d\') }}/',

)