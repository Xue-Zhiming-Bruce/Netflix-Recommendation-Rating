import os
import pandas as pd
import csv
import random
from collections import defaultdict
import logging
from utils.s3_download import download_netflix_raw_data

# Configure logging
logger = logging.getLogger(__name__)

def process_bronze_data(**context):
    """Process raw Netflix Prize data into bronze layer"""
    logger.info("Starting bronze data processing...")
    
    try:
        # Download raw data from S3 first
        bucket_name = os.getenv('S3_BUCKET_DATA', 'netflix-data')
        local_data_path = '/opt/airflow/netflix-prize-data'
        
        logger.info(f"Downloading raw data from S3 bucket: {bucket_name}")
        if not download_netflix_raw_data(bucket_name, local_data_path):
            logger.warning("Some files failed to download from S3, proceeding with available files")
        
        # File paths for raw data (adjusted for Airflow container)
        data_file_path1 = '/opt/airflow/netflix-prize-data/combined_data_1.txt'
        data_file_path2 = '/opt/airflow/netflix-prize-data/combined_data_2.txt'
        data_file_path3 = '/opt/airflow/netflix-prize-data/combined_data_3.txt'
        data_file_path4 = '/opt/airflow/netflix-prize-data/combined_data_4.txt'
        
        # Create bronze directory
        os.makedirs('/opt/airflow/datamart/bronze', exist_ok=True)
        
        # Configuration for sampling
        SAMPLE_PERCENTAGE = 0.01  # Use 1% of data
        MIN_USER_INTERACTIONS = 5  # Minimum interactions per user
        MIN_MOVIE_INTERACTIONS = 10  # Minimum interactions per movie
        RANDOM_SEED = 42
        
        random.seed(RANDOM_SEED)
        
        # Optimized approach: First pass to count user interactions only
        logger.info("First pass: counting user interactions...")
        user_counts = defaultdict(int)
        total_interactions = 0

        for path in [data_file_path1, data_file_path2, data_file_path3, data_file_path4]:
            if os.path.exists(path):
                with open(path, 'r') as file:
                    movie_id = None
                    for line in file:
                        line = line.strip()
                        if line.endswith(':'):
                            movie_id = int(line[:-1])
                        else:
                            customer_id, rating, date = line.split(',')
                            user_counts[int(customer_id)] += 1
                            total_interactions += 1
            else:
                logger.warning(f"File not found: {path}")
        
        logger.info(f"Total interactions: {total_interactions}")
        logger.info(f"Total unique users: {len(user_counts)}")
        
        # Filter and sample users based on interaction counts
        qualified_users = {user_id: count for user_id, count in user_counts.items() 
                          if count >= MIN_USER_INTERACTIONS}
        
        logger.info(f"Users with >= {MIN_USER_INTERACTIONS} interactions: {len(qualified_users)}")
        
        # Sample users strategically
        target_users = max(1, int(len(qualified_users) * SAMPLE_PERCENTAGE))
        sampled_user_ids = set(random.sample(list(qualified_users.keys()), target_users))
        
        logger.info(f"Sampling {target_users} users for {SAMPLE_PERCENTAGE*100}% dataset")
        
        # Second pass: collect only sampled user interactions
        logger.info("Second pass: collecting sampled user data...")
        rows = []
        movie_counts = defaultdict(int)
        
        for path in [data_file_path1, data_file_path2, data_file_path3, data_file_path4]:
            if os.path.exists(path):
                with open(path, 'r') as file:
                    movie_id = None
                    for line in file:
                        line = line.strip()
                        if line.endswith(':'):
                            movie_id = int(line[:-1])
                        else:
                            customer_id, rating, date = line.split(',')
                            if int(customer_id) in sampled_user_ids:
                                row = {
                                    'movie_id': movie_id,
                                    'customer_id': int(customer_id),
                                    'rating': int(rating),
                                    'date': date
                                }
                                rows.append(row)
                                movie_counts[movie_id] += 1
        
        logger.info(f"Collected {len(rows)} interactions from sampled users")
        
        # Filter by movie popularity to ensure sufficient movie interactions
        qualified_movies = {movie_id for movie_id, count in movie_counts.items() 
                           if count >= MIN_MOVIE_INTERACTIONS}
        
        rows = [row for row in rows if row['movie_id'] in qualified_movies]
        
        logger.info(f"Final sampled dataset: {len(rows)} interactions")
        logger.info(f"Users in sample: {len(set(row['customer_id'] for row in rows))}")
        logger.info(f"Movies in sample: {len(set(row['movie_id'] for row in rows))}")
        
        # Save user data
        with open('/opt/airflow/datamart/bronze/user.csv', 'w', newline='', encoding='utf-8') as csvfile:
            if rows:
                fieldnames = rows[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        
        logger.info("user.csv has been created")
        
        # Process movie titles data
        movie_titles_data = []
        movie_titles_path = '/opt/airflow/netflix-prize-data/movie_titles.csv'
        
        if os.path.exists(movie_titles_path):
            with open(movie_titles_path, encoding='ISO-8859-1') as f:
                for line in f:
                    parts = line.strip().split(',')
                    movie_id = parts[0]
                    year = int(parts[1]) if parts[1].isdigit() else None
                    title = ','.join(parts[2:]) if len(parts) > 2 else ""
                    movie_titles_data.append({
                        'MovieID': movie_id,
                        'YearOfRelease': year,
                        'Title': title
                    })
        
        movie_titles = pd.DataFrame(movie_titles_data)
        # Convert YearOfRelease to datetime (use January 1st of the year)
        movie_titles["YearOfRelease"] = pd.to_datetime(movie_titles["YearOfRelease"], format='%Y', errors='coerce')
        movie_titles.to_csv('/opt/airflow/datamart/bronze/movie.csv', index=False, encoding='utf-8', date_format='%Y')
        
        logger.info("movie.csv has been created")
        logger.info("Bronze data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in bronze data processing: {str(e)}")
        raise