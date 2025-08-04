import pandas as pd
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

def process_silver_data(**context):
    """Process bronze data into silver layer"""
    logger.info("Starting silver data processing...")
    
    try:
        # Create silver directory
        os.makedirs('/opt/airflow/datamart/silver', exist_ok=True)
        
        # Read bronze data
        user = pd.read_csv('/opt/airflow/datamart/bronze/user.csv')
        movie = pd.read_csv('/opt/airflow/datamart/bronze/movie.csv')
        
        # Merge user and movie data
        user_movie = pd.merge(user, movie, left_on='movie_id', right_on='MovieID')
        user_movie = user_movie.drop('MovieID', axis=1)
        
        # Filter data to only include records before 2005
        user_movie['date'] = pd.to_datetime(user_movie['date'])
        user_movie = user_movie[user_movie['date'] < '2005-01-01']
        
        # Handle null values
        if user_movie.isnull().any().any():
            user_movie = user_movie.dropna()
        
        # Save silver data
        user_movie.to_csv('/opt/airflow/datamart/silver/user_movie.csv', index=False)
        
        logger.info(f"Silver data processing completed. Records: {len(user_movie)}")
        
    except Exception as e:
        logger.error(f"Error in silver data processing: {str(e)}")
        raise