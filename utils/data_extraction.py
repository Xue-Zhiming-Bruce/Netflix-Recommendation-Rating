import pandas as pd
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

def extract_netflix_gold_data(**context):
    """Extract Netflix Prize gold data for processing"""
    logger.info("Starting data extraction...")
    
    try:
        # Use real Netflix Prize gold data
        data_path = '/opt/airflow/datamart/gold/train.csv'
        
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Gold data not found at {data_path}. Please ensure the datamart directory is properly mounted.")
        
        # Load the gold data
        df = pd.read_csv(data_path)
        
        # Rename columns to match expected format
        df = df.rename(columns={
            'customer_id': 'user_id'
        })
        
        # Select all relevant columns for enhanced feature engineering
        feature_columns = ['user_id', 'movie_id', 'rating', 'date', 'YearOfRelease', 'Title', 
                          'year', 'month', 'is_weekend', 'season', 'movie_age']
        df = df[feature_columns]
        
        # Save to temp location for processing
        df.to_parquet('/tmp/gold_ratings.parquet', index=False)
        
        logger.info(f"Netflix Prize gold data extracted successfully: {len(df)} ratings")
        return {'status': 'success', 'files_extracted': 1, 'records_count': len(df)}
        
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise