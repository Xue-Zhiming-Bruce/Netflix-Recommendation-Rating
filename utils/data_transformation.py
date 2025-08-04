import pandas as pd
import logging

# Configure logging
logger = logging.getLogger(__name__)

def transform_netflix_data(**context):
    """Transform and clean the datamart data"""
    logger.info("Starting data transformation...")
    
    try:
        # Load datamart data
        df = pd.read_parquet('/tmp/gold_ratings.parquet')
        
        # Add some data quality checks and transformations
        # Remove any invalid ratings
        df = df[(df['rating'] >= 1) & (df['rating'] <= 5)]
        
        # Ensure date is properly formatted
        df['date'] = pd.to_datetime(df['date'])
        
        # Add some derived features
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day_of_week'] = df['date'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # Save processed data
        df.to_parquet('/tmp/processed_ratings.parquet', index=False)
        
        logger.info(f"Data transformation completed. Processed {len(df)} ratings")
        return {'status': 'success', 'records_processed': len(df)}
        
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise