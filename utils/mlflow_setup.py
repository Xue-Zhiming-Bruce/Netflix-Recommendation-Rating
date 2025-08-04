import mlflow
import mlflow.sklearn
import os
import logging
import time
import requests
from requests.exceptions import ConnectionError, RequestException

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MAX_RETRIES = 3
RETRY_DELAY = 2

def wait_for_mlflow_service(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Wait for MLflow service to be available"""
    for attempt in range(max_retries):
        try:
            # Try to reach MLflow health endpoint
            health_url = f"{MLFLOW_TRACKING_URI.rstrip('/')}/health"
            response = requests.get(health_url, timeout=5)
            if response.status_code == 200:
                logger.info(f"MLflow service is available at {MLFLOW_TRACKING_URI}")
                return True
        except (ConnectionError, RequestException) as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: MLflow service not available - {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    
    logger.error(f"MLflow service is not available after {max_retries} attempts")
    return False

def setup_mlflow():
    """Setup MLflow tracking with retry logic
    
    Returns:
        bool: True if MLflow setup was successful, False otherwise
    """
    try:
        # First check if MLflow service is available
        if not wait_for_mlflow_service():
            logger.warning("MLflow service is not available, skipping MLflow setup")
            return False
        
        # Set tracking URI
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        logger.info(f"MLflow tracking URI set to: {MLFLOW_TRACKING_URI}")
        
        # Try to set experiment with retry logic
        for attempt in range(MAX_RETRIES):
            try:
                mlflow.set_experiment('netflix-recommendation-pipeline')
                logger.info("MLflow experiment 'netflix-recommendation-pipeline' set successfully")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to set MLflow experiment - {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        logger.error("Failed to set MLflow experiment after all retries")
        return False
        
    except Exception as e:
        logger.error(f"MLflow setup failed with error: {str(e)}")
        return False