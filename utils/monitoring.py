import pandas as pd
import logging
from datetime import datetime

try:
    from evidently import ColumnMapping
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset, TargetDriftPreset
    EVIDENTLY_AVAILABLE = True
except ImportError:
    # Mock classes for when evidently is not available
    class ColumnMapping:
        def __init__(self, **kwargs):
            pass
    
    class Report:
        def __init__(self, **kwargs):
            pass
        def run(self, **kwargs):
            pass
        def save_html(self, path):
            with open(path, 'w') as f:
                f.write('<html><body>Evidently not available</body></html>')
    
    class DataDriftPreset:
        pass
    
    class TargetDriftPreset:
        pass
    
    EVIDENTLY_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

def generate_monitoring_report(**context):
    """Generate data drift and model monitoring report using Evidently AI"""
    logger.info("Starting monitoring report generation...")
    
    try:
        # Load current and reference data
        current_data = pd.read_parquet('/tmp/processed_ratings.parquet')
        
        # For demo, use a subset as reference data (in practice, this would be historical data)
        reference_data = current_data.sample(n=min(10000, len(current_data)//2), random_state=42)
        current_data = current_data.sample(n=min(10000, len(current_data)//2), random_state=123)
        
        # Define column mapping
        column_mapping = ColumnMapping(
            target='rating',
            numerical_features=['user_id', 'movie_id'],
            categorical_features=[]
        )
        
        # Create data drift report
        data_drift_report = Report(metrics=[
            DataDriftPreset(),
            TargetDriftPreset()
        ])
        
        data_drift_report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)
        
        # Save report locally
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f'/tmp/data_drift_report_{timestamp}.html'
        data_drift_report.save_html(report_path)
        
        logger.info("Monitoring report generated successfully")
        return {
            'status': 'success',
            'report_path': report_path,
            'reference_data_size': len(reference_data),
            'current_data_size': len(current_data)
        }
        
    except Exception as e:
        logger.error(f"Monitoring report generation failed: {str(e)}")
        raise

def cleanup_temp_files(**context):
    """Clean up temporary files"""
    logger.info("Cleaning up temporary files...")
    
    import os
    temp_files = [
        '/tmp/combined_data_1.txt',
        '/tmp/combined_data_2.txt',
        '/tmp/combined_data_3.txt',
        '/tmp/combined_data_4.txt',
        '/tmp/movie_titles.csv',
        '/tmp/processed_ratings.parquet',
        '/tmp/user_features.parquet',
        '/tmp/movie_features.parquet',
        '/tmp/netflix_model.pkl',
        '/tmp/data_drift_report.html'
    ]
    
    for file_path in temp_files:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Removed {file_path}")
        except Exception as e:
            logger.warning(f"Could not remove {file_path}: {str(e)}")
    
    logger.info("Cleanup completed")
    return {'status': 'success'}