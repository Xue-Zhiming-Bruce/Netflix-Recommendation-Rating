import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# Add the parent directory to Python path to find utils module
sys.path.append('/opt/airflow')

# Import utility functions
from utils.bronze_processing import process_bronze_data
from utils.silver_processing import process_silver_data
from utils.gold_processing import process_gold_data
from utils.data_extraction import extract_netflix_gold_data
from utils.data_transformation import transform_netflix_data
from utils.feature_engineering import perform_feature_engineering
from utils.model_train import train_netflix_model
from utils.mlflow_setup import setup_mlflow
from utils.monitoring import generate_monitoring_report, cleanup_temp_files
from utils.s3_upload import upload_processed_data_to_s3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'netflix_mlops_pipeline',
    default_args=default_args,
    description='Complete MLOps pipeline for Netflix recommendation model',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['mlops', 'netflix', 'recommendation', 'mlflow', 'evidently'],
)

# Configuration
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
S3_BUCKET_DATA = os.getenv('S3_BUCKET_DATA', 'netflix-recommend-project')
S3_BUCKET_ARTIFACTS = os.getenv('S3_BUCKET_ARTIFACTS', 'netflix-mlops-mlflow-artifacts')
S3_BUCKET_MONITORING = os.getenv('S3_BUCKET_MONITORING', 'netflix-mlops-monitoring-reports')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-west-1')

# Define tasks
# Raw data processing tasks
process_bronze_task = PythonOperator(
    task_id='process_bronze_data',
    python_callable=process_bronze_data,
    dag=dag,
)

process_silver_task = PythonOperator(
    task_id='process_silver_data',
    python_callable=process_silver_data,
    dag=dag,
)

process_gold_task = PythonOperator(
    task_id='process_gold_data',
    python_callable=process_gold_data,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_netflix_gold_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_netflix_data,
    dag=dag,
)

feature_engineering_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=perform_feature_engineering,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_netflix_model,
    dag=dag,
)

monitoring_task = PythonOperator(
    task_id='generate_monitoring_report',
    python_callable=generate_monitoring_report,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_processed_data_to_s3',
    python_callable=upload_processed_data_to_s3,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
    trigger_rule='all_done',  # Run even if upstream tasks fail
)

# Define task dependencies
process_bronze_task >> process_silver_task >> process_gold_task >> extract_task >> transform_task >> feature_engineering_task >> train_model_task >> monitoring_task >> upload_to_s3_task >> cleanup_task