"""
Ride-Sharing Analytics Pipeline DAG
Orchestrates the complete data pipeline from Bronze to Gold
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# GCP Configuration
PROJECT_ID = Variable.get('GCP_PROJECT_ID')
REGION = Variable.get('GCP_REGION')
CLUSTER_NAME = Variable.get('DATAPROC_CLUSTER_NAME')
BUCKET_NAME = Variable.get('GCS_BUCKET_NAME')  
DATASET = Variable.get('BIGQUERY_DATASET')



with DAG(
    'analytics_pipeline_dag',
    default_args=default_args,
    description='Analytics Pipeline',
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    start_date=days_ago(1),
    catchup=False,
    tags=['analytics', 'gcp'],
) as dag:

    # Task 1: Check if data exists in Bronze folder
    check_bronze_data = GCSListObjectsOperator(
        task_id='check_bronze_data',
        bucket=BUCKET_NAME,
        prefix='bronze/rides/',
        gcp_conn_id='google_cloud_default',
    )

    bronze_to_silver = DataprocSubmitJobOperator(
        task_id='bronze_to_silver',
        job={
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{BUCKET_NAME}/spark-jobs/bronze_to_silver.py',
                'args': [
                    f'gs://{BUCKET_NAME}/bronze',
                    f'gs://{BUCKET_NAME}/silver',
                    '{{ ds }}',  # Processing date from Airflow
                ],
                'python_file_uris': [],
                'jar_file_uris': [],
            }
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    silver_to_gold = DataprocSubmitJobOperator(
        task_id='silver_to_gold',
        job={
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{BUCKET_NAME}/spark-jobs/silver_to_gold.py',
                'args': [
                    f'gs://{BUCKET_NAME}/silver',
                    PROJECT_ID,
                    DATASET,
                    BUCKET_NAME,  # Temp bucket for BigQuery
                    '{{ ds }}',  # Processing date
                ],
                'python_file_uris': [],
                'jar_file_uris': [],
            }
        },
        region=REGION,
        project_id=PROJECT_ID,
    )


    check_bronze_data >> bronze_to_silver >> silver_to_gold

