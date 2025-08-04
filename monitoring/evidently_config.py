# Evidently AI Configuration for Netflix MLOps Pipeline

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from evidently import ColumnMapping
from evidently.report import Report
from evidently.test_suite import TestSuite
from evidently.metric_preset import (
    DataDriftPreset,
    TargetDriftPreset,
    DataQualityPreset,
    RegressionPreset
)
from evidently.test_preset import (
    DataStabilityTestPreset,
    DataQualityTestPreset,
    RegressionTestPreset
)
from evidently.metrics import (
    ColumnDriftMetric,
    DatasetDriftMetric,
    ColumnSummaryMetric,
    ColumnQuantileMetric,
    ConflictTargetMetric,
    ConflictPredictionMetric
)
from evidently.tests import (
    TestColumnDrift,
    TestShareOfMissingValues,
    TestMeanInNSigmas,
    TestValueRange
)

class NetflixMonitoringConfig:
    """Configuration class for Netflix recommendation model monitoring"""
    
    def __init__(self):
        self.column_mapping = self._setup_column_mapping()
        self.drift_thresholds = self._setup_drift_thresholds()
        self.quality_thresholds = self._setup_quality_thresholds()
    
    def _setup_column_mapping(self) -> ColumnMapping:
        """Setup column mapping for Netflix dataset"""
        return ColumnMapping(
            target='rating',
            prediction='predicted_rating',
            numerical_features=[
                'user_id',
                'movie_id',
                'rating_mean_user',
                'rating_std_user',
                'rating_count_user',
                'weekend_ratio',
                'rating_mean_movie',
                'rating_std_movie',
                'rating_count_movie',
                'unique_users',
                'movie_age',
                'year',
                'month'
            ],
            categorical_features=[
                'day_of_week',
                'is_weekend',
                'season',
                'user_activity_level',
                'movie_popularity_tier'
            ],
            datetime_features=['date']
        )
    
    def _setup_drift_thresholds(self) -> Dict[str, float]:
        """Setup drift detection thresholds"""
        return {
            'rating': 0.1,
            'user_id': 0.2,
            'movie_id': 0.2,
            'rating_mean_user': 0.15,
            'rating_std_user': 0.15,
            'weekend_ratio': 0.1,
            'rating_mean_movie': 0.15,
            'movie_age': 0.2
        }
    
    def _setup_quality_thresholds(self) -> Dict[str, Dict[str, float]]:
        """Setup data quality thresholds"""
        return {
            'missing_values': {
                'rating': 0.01,
                'user_id': 0.0,
                'movie_id': 0.0,
                'date': 0.01
            },
            'value_ranges': {
                'rating': {'min': 1, 'max': 5},
                'rating_mean_user': {'min': 1, 'max': 5},
                'rating_mean_movie': {'min': 1, 'max': 5},
                'weekend_ratio': {'min': 0, 'max': 1}
            },
            'statistical_bounds': {
                'rating_std_user': {'n_sigmas': 3},
                'rating_count_user': {'n_sigmas': 3},
                'unique_users': {'n_sigmas': 3}
            }
        }

class NetflixReportGenerator:
    """Generate monitoring reports for Netflix recommendation model"""
    
    def __init__(self, config: NetflixMonitoringConfig):
        self.config = config
        self.reports_dir = '/app/workspace/reports'
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def generate_data_drift_report(self, reference_data, current_data, save_path: Optional[str] = None) -> Report:
        """Generate comprehensive data drift report"""
        report = Report(metrics=[
            DataDriftPreset(),
            TargetDriftPreset(),
            DataQualityPreset(),
            ColumnDriftMetric(column_name='rating'),
            ColumnDriftMetric(column_name='rating_mean_user'),
            ColumnDriftMetric(column_name='rating_mean_movie'),
            ColumnSummaryMetric(column_name='rating'),
            ColumnQuantileMetric(column_name='rating', quantile=0.95),
            DatasetDriftMetric()
        ])
        
        report.run(
            reference_data=reference_data,
            current_data=current_data,
            column_mapping=self.config.column_mapping
        )
        
        if save_path:
            report.save_html(save_path)
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            default_path = os.path.join(self.reports_dir, f'data_drift_report_{timestamp}.html')
            report.save_html(default_path)
        
        return report
    
    def generate_model_performance_report(self, reference_data, current_data, save_path: Optional[str] = None) -> Report:
        """Generate model performance monitoring report"""
        report = Report(metrics=[
            RegressionPreset(),
            ConflictTargetMetric(),
            ConflictPredictionMetric()
        ])
        
        report.run(
            reference_data=reference_data,
            current_data=current_data,
            column_mapping=self.config.column_mapping
        )
        
        if save_path:
            report.save_html(save_path)
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            default_path = os.path.join(self.reports_dir, f'model_performance_report_{timestamp}.html')
            report.save_html(default_path)
        
        return report
    
    def generate_data_quality_test_suite(self, data, save_path: Optional[str] = None) -> TestSuite:
        """Generate data quality test suite"""
        tests = [
            DataQualityTestPreset(),
            DataStabilityTestPreset()
        ]
        
        # Add custom tests based on thresholds
        for column, threshold in self.config.quality_thresholds['missing_values'].items():
            tests.append(TestShareOfMissingValues(column_name=column, lt=threshold))
        
        for column, bounds in self.config.quality_thresholds['value_ranges'].items():
            tests.append(TestValueRange(column_name=column, left=bounds['min'], right=bounds['max']))
        
        for column, params in self.config.quality_thresholds['statistical_bounds'].items():
            tests.append(TestMeanInNSigmas(column_name=column, n_sigmas=params['n_sigmas']))
        
        test_suite = TestSuite(tests=tests)
        test_suite.run(reference_data=data, current_data=None, column_mapping=self.config.column_mapping)
        
        if save_path:
            test_suite.save_html(save_path)
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            default_path = os.path.join(self.reports_dir, f'data_quality_tests_{timestamp}.html')
            test_suite.save_html(default_path)
        
        return test_suite
    
    def generate_drift_test_suite(self, reference_data, current_data, save_path: Optional[str] = None) -> TestSuite:
        """Generate drift detection test suite"""
        tests = []
        
        # Add drift tests for key columns
        for column, threshold in self.config.drift_thresholds.items():
            tests.append(TestColumnDrift(column_name=column, stattest_threshold=threshold))
        
        test_suite = TestSuite(tests=tests)
        test_suite.run(
            reference_data=reference_data,
            current_data=current_data,
            column_mapping=self.config.column_mapping
        )
        
        if save_path:
            test_suite.save_html(save_path)
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            default_path = os.path.join(self.reports_dir, f'drift_tests_{timestamp}.html')
            test_suite.save_html(default_path)
        
        return test_suite
    
    def generate_regression_test_suite(self, reference_data, current_data, save_path: Optional[str] = None) -> TestSuite:
        """Generate regression model test suite"""
        test_suite = TestSuite(tests=[
            RegressionTestPreset()
        ])
        
        test_suite.run(
            reference_data=reference_data,
            current_data=current_data,
            column_mapping=self.config.column_mapping
        )
        
        if save_path:
            test_suite.save_html(save_path)
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            default_path = os.path.join(self.reports_dir, f'regression_tests_{timestamp}.html')
            test_suite.save_html(default_path)
        
        return test_suite

class MonitoringScheduler:
    """Schedule and manage monitoring tasks"""
    
    def __init__(self, config: NetflixMonitoringConfig, report_generator: NetflixReportGenerator):
        self.config = config
        self.report_generator = report_generator
    
    def daily_monitoring_pipeline(self, reference_data, current_data) -> Dict[str, str]:
        """Run daily monitoring pipeline"""
        results = {}
        
        try:
            # Generate data drift report
            drift_report = self.report_generator.generate_data_drift_report(reference_data, current_data)
            results['data_drift'] = 'success'
            
            # Generate data quality tests
            quality_tests = self.report_generator.generate_data_quality_test_suite(current_data)
            results['data_quality'] = 'success'
            
            # Generate drift tests
            drift_tests = self.report_generator.generate_drift_test_suite(reference_data, current_data)
            results['drift_tests'] = 'success'
            
            # Check if predictions are available for model performance monitoring
            if 'predicted_rating' in current_data.columns:
                performance_report = self.report_generator.generate_model_performance_report(reference_data, current_data)
                regression_tests = self.report_generator.generate_regression_test_suite(reference_data, current_data)
                results['model_performance'] = 'success'
                results['regression_tests'] = 'success'
            
        except Exception as e:
            results['error'] = str(e)
        
        return results
    
    def weekly_monitoring_pipeline(self, reference_data, current_data) -> Dict[str, str]:
        """Run weekly comprehensive monitoring pipeline"""
        # Run daily pipeline
        results = self.daily_monitoring_pipeline(reference_data, current_data)
        
        # Add weekly-specific monitoring tasks
        try:
            # Generate comprehensive reports with more detailed analysis
            # This could include trend analysis, seasonal pattern detection, etc.
            results['weekly_analysis'] = 'success'
            
        except Exception as e:
            results['weekly_error'] = str(e)
        
        return results

# Example usage and configuration
def get_monitoring_config() -> NetflixMonitoringConfig:
    """Get the monitoring configuration for Netflix recommendation model"""
    return NetflixMonitoringConfig()

def get_report_generator(config: NetflixMonitoringConfig) -> NetflixReportGenerator:
    """Get the report generator with configuration"""
    return NetflixReportGenerator(config)

def get_monitoring_scheduler(config: NetflixMonitoringConfig, report_generator: NetflixReportGenerator) -> MonitoringScheduler:
    """Get the monitoring scheduler"""
    return MonitoringScheduler(config, report_generator)

# Configuration constants
MONITORING_CONFIG = {
    'report_retention_days': 30,
    'alert_thresholds': {
        'data_drift_score': 0.5,
        'model_performance_degradation': 0.1,
        'data_quality_score': 0.8
    },
    'notification_channels': {
        'email': ['mlops-team@company.com'],
        'slack': ['#mlops-alerts'],
        'pagerduty': ['mlops-oncall']
    }
}