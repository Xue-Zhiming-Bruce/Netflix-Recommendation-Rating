import pandas as pd
import numpy as np
import logging
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
import joblib
import mlflow
import mlflow.sklearn
from .mlflow_setup import setup_mlflow

# Configure logging
logger = logging.getLogger(__name__)

def train_netflix_model(**context):
    """Train the recommendation model with optional MLflow tracking"""
    logger.info("Starting model training...")
    
    # Try to setup MLflow, but continue if it fails
    mlflow_available = False
    try:
        mlflow_available = setup_mlflow()
        if mlflow_available:
            logger.info("MLflow setup successful - tracking enabled")
        else:
            logger.warning("MLflow setup failed - continuing without tracking")
    except Exception as e:
        logger.warning(f"MLflow setup failed with error: {str(e)} - continuing without tracking")
        mlflow_available = False
    
    # Start MLflow run only if available
    if mlflow_available:
        try:
            mlflow.start_run(run_name=f"netflix-training-{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        except Exception as e:
            logger.warning(f"Failed to start MLflow run: {str(e)} - continuing without tracking")
            mlflow_available = False
    
    try:
        # Load data and features
        df = pd.read_parquet('/tmp/processed_ratings.parquet')
        user_features = pd.read_parquet('/tmp/user_features.parquet')
        movie_features = pd.read_parquet('/tmp/movie_features.parquet')
        
        # Log dataset info to MLflow if available
        if mlflow_available:
            try:
                mlflow.log_param("dataset_size", len(df))
                mlflow.log_param("num_users", len(user_features))
                mlflow.log_param("num_movies", len(movie_features))
            except Exception as e:
                logger.warning(f"Failed to log parameters to MLflow: {str(e)}")
        
        # Simple collaborative filtering approach for demo
        # Prepare training data
        df_merged = df.merge(user_features, left_on='user_id', right_index=True, suffixes=('', '_user'))
        df_merged = df_merged.merge(movie_features, left_on='movie_id', right_index=True, suffixes=('', '_movie'))
        
        # Debug: Print available columns to understand the structure
        logger.info(f"Available columns after merge: {list(df_merged.columns)}")
        
        # Select features based on actual column names after merge
        # User features (with _user suffix after merge)
        user_feature_cols = [col for col in df_merged.columns if col.endswith('_user')]
        # Movie features (with _movie suffix after merge)
        movie_feature_cols = [col for col in df_merged.columns if col.endswith('_movie')]
        # Original rating features that don't have suffixes
        base_feature_cols = [col for col in df_merged.columns if col in ['rating', 'is_weekend', 'month', 'season', 'movie_age', 'YearOfRelease']]
        
        # Combine all available features
        feature_cols = user_feature_cols + movie_feature_cols + base_feature_cols
        
        # Remove 'rating' from features as it's our target
        if 'rating' in feature_cols:
            feature_cols.remove('rating')
        
        logger.info(f"Selected feature columns: {feature_cols}")
        
        if not feature_cols:
            raise ValueError("No valid feature columns found after merge")
        
        X = df_merged[feature_cols].fillna(0)
        y = df_merged['rating']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test)
        
        # Calculate metrics
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        
        # Log metrics to MLflow if available
        if mlflow_available:
            try:
                mlflow.log_metric("mse", mse)
                mlflow.log_metric("mae", mae)
                mlflow.log_metric("rmse", rmse)
                
                # Log model to MLflow
                mlflow.sklearn.log_model(model, "model")
                logger.info("Successfully logged metrics and model to MLflow")
            except Exception as e:
                logger.warning(f"Failed to log metrics/model to MLflow: {str(e)}")
        
        # Always save model locally regardless of MLflow status
        joblib.dump(model, '/tmp/netflix_model.pkl')
        logger.info("Model saved locally to /tmp/netflix_model.pkl")
        
        logger.info(f"Model training completed. RMSE: {rmse:.4f}, MAE: {mae:.4f}")
        
        return {
            'status': 'success',
            'rmse': rmse,
            'mae': mae,
            'mse': mse,
            'mlflow_tracking': mlflow_available
        }
        
    except Exception as e:
        logger.error(f"Model training failed: {str(e)}")
        
        # Log error to MLflow if available
        if mlflow_available:
            try:
                mlflow.log_param("status", "failed")
                mlflow.log_param("error", str(e))
            except Exception as mlflow_error:
                logger.warning(f"Failed to log error to MLflow: {str(mlflow_error)}")
        
        raise
    
    finally:
        # End MLflow run if it was started
        if mlflow_available:
            try:
                mlflow.end_run()
            except Exception as e:
                logger.warning(f"Failed to end MLflow run: {str(e)}")