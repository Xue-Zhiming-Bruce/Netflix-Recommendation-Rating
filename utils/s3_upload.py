# s3_upload.py
import boto3
import os
import logging
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logger = logging.getLogger(__name__)

def upload_to_s3(bucket_name=None):
    """Upload processed data to S3"""
    if bucket_name is None:
        bucket_name = os.getenv('S3_BUCKET_DATA', 'netflix-recommend-project')
    
    try:
        s3 = boto3.client('s3')
        logger.info(f"Starting upload to S3 bucket: {bucket_name}")
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        return False
    except Exception as e:
        logger.error(f"Error initializing S3 client: {e}")
        return False
    
    # Upload bronze data (already processed by your script)
    local_bronze = '/opt/airflow/datamart/bronze/'
    s3_bronze = 'data/bronze/'
    
    if os.path.exists(local_bronze):
        for file in os.listdir(local_bronze):
            if file.endswith('.csv'):
                local_path = os.path.join(local_bronze, file)
                s3_key = f'{s3_bronze}{file}'
                try:
                    s3.upload_file(local_path, bucket_name, s3_key)
                    logger.info(f"Uploaded {file} to s3://{bucket_name}/{s3_key}")
                except ClientError as e:
                    logger.error(f"Failed to upload {file}: {e}")
    else:
        logger.warning(f"Bronze directory not found: {local_bronze}")
    
    # Upload silver and gold data
    for layer in ['silver', 'gold']:
        local_path = f'/opt/airflow/datamart/{layer}/'
        s3_path = f'data/{layer}/'
        
        if os.path.exists(local_path):
            for file in os.listdir(local_path):
                if file.endswith('.csv'):
                    local_file = os.path.join(local_path, file)
                    s3_key = f'{s3_path}{file}'
                    try:
                        s3.upload_file(local_file, bucket_name, s3_key)
                        logger.info(f"Uploaded {file} to s3://{bucket_name}/{s3_key}")
                    except ClientError as e:
                        logger.error(f"Failed to upload {file}: {e}")
        else:
            logger.warning(f"{layer.capitalize()} directory not found: {local_path}")
    
    # Upload raw Netflix data (if available locally)
    raw_data_path = '/opt/airflow/netflix-prize-data/'
    s3_raw_path = 'data/raw/'
    
    if os.path.exists(raw_data_path):
        for file in os.listdir(raw_data_path):
            if file.endswith(('.txt', '.csv')):
                local_file = os.path.join(raw_data_path, file)
                s3_key = f'{s3_raw_path}{file}'
                try:
                    s3.upload_file(local_file, bucket_name, s3_key)
                    logger.info(f"Uploaded {file} to s3://{bucket_name}/{s3_key}")
                except ClientError as e:
                    logger.error(f"Failed to upload {file}: {e}")
    
    # Upload any additional data files in root directory
    for file in ['/opt/airflow/data.zip', '/opt/airflow/netflix-prize-data.zip']:
        if os.path.exists(file):
            filename = os.path.basename(file)
            s3_key = f'data/archives/{filename}'
            try:
                s3.upload_file(file, bucket_name, s3_key)
                logger.info(f"Uploaded {filename} to s3://{bucket_name}/{s3_key}")
            except ClientError as e:
                logger.error(f"Failed to upload {filename}: {e}")
    
    logger.info("S3 upload process completed")
    return True

def upload_processed_data_to_s3(**context):
    """Airflow task function to upload processed data to S3"""
    bucket_name = os.getenv('S3_BUCKET_DATA', 'netflix-recommend-project')
    return upload_to_s3(bucket_name)

if __name__ == "__main__":
    upload_to_s3()