# s3_download.py
import boto3
import os
import logging
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logger = logging.getLogger(__name__)

def download_from_s3(bucket_name, s3_key, local_path):
    """
    Download a file from S3 to local path
    
    Args:
        bucket_name (str): S3 bucket name
        s3_key (str): S3 object key
        local_path (str): Local file path to save the downloaded file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3 = boto3.client('s3')
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download file
        s3.download_file(bucket_name, s3_key, local_path)
        logger.info(f"Successfully downloaded s3://{bucket_name}/{s3_key} to {local_path}")
        return True
        
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        return False
    except ClientError as e:
        logger.error(f"Error downloading from S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {e}")
        return False

def download_netflix_raw_data(bucket_name, local_base_path='/opt/airflow/netflix-prize-data'):
    """
    Download all Netflix Prize raw data files from S3
    
    Args:
        bucket_name (str): S3 bucket name
        local_base_path (str): Local base path to save files
    
    Returns:
        bool: True if all files downloaded successfully, False otherwise
    """
    # List of raw data files to download
    raw_files = [
        'combined_data_1.txt',
        'combined_data_2.txt', 
        'combined_data_3.txt',
        'combined_data_4.txt',
        'movie_titles.csv',
        'probe.txt',
        'qualifying.txt',
        'README'
    ]
    
    success_count = 0
    total_files = len(raw_files)
    
    logger.info(f"Starting download of {total_files} Netflix Prize raw data files from S3")
    
    for filename in raw_files:
        s3_key = f'data/raw/{filename}'
        local_path = os.path.join(local_base_path, filename)
        
        if download_from_s3(bucket_name, s3_key, local_path):
            success_count += 1
        else:
            logger.warning(f"Failed to download {filename}")
    
    logger.info(f"Downloaded {success_count}/{total_files} files successfully")
    return success_count == total_files

def check_s3_file_exists(bucket_name, s3_key):
    """
    Check if a file exists in S3
    
    Args:
        bucket_name (str): S3 bucket name
        s3_key (str): S3 object key
    
    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        s3 = boto3.client('s3')
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError:
        return False
    except Exception as e:
        logger.error(f"Error checking S3 file existence: {e}")
        return False

if __name__ == "__main__":
    # Test the download functionality
    import os
    bucket_name = os.getenv('S3_BUCKET_DATA', 'netflix-recommend-project')
    download_netflix_raw_data(bucket_name)