import pandas as pd
import numpy as np
import logging

# Configure logging
logger = logging.getLogger(__name__)

def perform_feature_engineering(**context):
    """Perform enhanced feature engineering using gold datamart features"""
    logger.info("Starting enhanced feature engineering...")
    
    try:
        # Load processed data with rich features
        df = pd.read_parquet('/tmp/processed_ratings.parquet')
        
        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Enhanced user features
        user_features = df.groupby('user_id').agg({
            'rating': ['mean', 'std', 'count', 'min', 'max'],
            'is_weekend': 'mean',
            'month': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 1,
            'season': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 1,
            'movie_age': ['mean', 'std'],
            'YearOfRelease': ['mean', 'std']
        }).round(4)
        
        user_features.columns = ['rating_mean', 'rating_std', 'rating_count', 'rating_min', 'rating_max',
                                'weekend_ratio', 'preferred_month', 'preferred_season', 
                                'avg_movie_age', 'std_movie_age', 'avg_movie_year', 'std_movie_year']
        user_features = user_features.fillna(0)
        
        # Enhanced movie features
        movie_features = df.groupby('movie_id').agg({
            'rating': ['mean', 'std', 'count', 'min', 'max'],
            'user_id': 'nunique',
            'YearOfRelease': 'first',
            'movie_age': 'first',
            'Title': 'first'
        }).round(4)
        
        movie_features.columns = ['rating_mean', 'rating_std', 'rating_count', 'rating_min', 'rating_max',
                                 'unique_users', 'year_of_release', 'movie_age', 'title']
        movie_features = movie_features.fillna(0)
        
        # Add movie popularity score (combination of rating and user count)
        movie_features['popularity_score'] = (movie_features['rating_mean'] * 
                                            np.log1p(movie_features['unique_users'])).round(4)
        
        # Save features locally
        user_features.to_parquet('/tmp/user_features.parquet')
        movie_features.to_parquet('/tmp/movie_features.parquet')
        
        logger.info(f"Feature engineering completed. Created features for {len(user_features)} users and {len(movie_features)} movies")
        return {
            'status': 'success', 
            'user_features': len(user_features),
            'movie_features': len(movie_features)
        }
        
    except Exception as e:
        logger.error(f"Feature engineering failed: {str(e)}")
        raise