import pandas as pd
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

def process_gold_data(**context):
    """Process silver data into gold layer with feature engineering"""
    logger.info("Starting gold data processing...")
    
    try:
        # Create gold directory
        os.makedirs('/opt/airflow/datamart/gold', exist_ok=True)
        
        # Read silver data
        user_movie = pd.read_csv('/opt/airflow/datamart/silver/user_movie.csv')
        
        # Feature engineering for user rating time
        user_movie['date'] = pd.to_datetime(user_movie['date'])
        user_movie['year_month'] = user_movie['date'].dt.to_period('M')
        user_movie['year'] = user_movie['date'].dt.year
        user_movie['month'] = user_movie['date'].dt.month
        user_movie['is_weekend'] = user_movie['date'].dt.dayofweek >= 5
        user_movie['season'] = user_movie['month'].apply(lambda x: (x % 12 + 3) // 3)
        
        # Feature engineering for movie release time
        user_movie['movie_age'] = user_movie['year'] - user_movie['YearOfRelease']
        
        # Get sorted unique year-months
        months = sorted(user_movie['year_month'].unique())
        
        # Define OOT periods, 6 months for each OOT data
        OOT1_months = months[-18:-12]
        OOT2_months = months[-12:-6]
        OOT3_months = months[-6:]
        train_months = months[:-18]
        
        # Create data splits
        train = user_movie[user_movie['year_month'].isin(train_months)]
        OOT1 = user_movie[user_movie['year_month'].isin(OOT1_months)]
        OOT2 = user_movie[user_movie['year_month'].isin(OOT2_months)]
        OOT3 = user_movie[user_movie['year_month'].isin(OOT3_months)]
        
        # Save splits
        train.to_csv('/opt/airflow/datamart/gold/train.csv', index=False)
        OOT1.to_csv('/opt/airflow/datamart/gold/oot1.csv', index=False)
        OOT2.to_csv('/opt/airflow/datamart/gold/oot2.csv', index=False)
        OOT3.to_csv('/opt/airflow/datamart/gold/oot3.csv', index=False)
        
        logger.info(f"Gold data processing completed.")
        logger.info(f"Train records: {len(train)}")
        logger.info(f"OOT1 records: {len(OOT1)}")
        logger.info(f"OOT2 records: {len(OOT2)}")
        logger.info(f"OOT3 records: {len(OOT3)}")
        
        # Save the processed data path for downstream tasks
        context['task_instance'].xcom_push(key='gold_data_path', value='/opt/airflow/datamart/gold/train.csv')
        
    except Exception as e:
        logger.error(f"Error in gold data processing: {str(e)}")
        raise

