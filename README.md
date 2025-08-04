# Netflix MLOps Pipeline

A complete MLOps pipeline for Netflix recommendation system using Airflow, MLflow, and Evidently.

## Architecture

- **Data Processing**: Bronze, Silver, Gold layer architecture
- **Orchestration**: Apache Airflow for workflow management
- **ML Tracking**: MLflow for experiment tracking and model registry
- **Monitoring**: Evidently for data and model drift detection
- **Containerization**: Docker for consistent deployment

## Project Structure

```
├── dags/                    # Airflow DAG definitions
│   └── netflix_mlops_pipeline.py
├── utils/                   # Data processing and ML utilities
│   ├── bronze_processing.py # Raw data ingestion
│   ├── silver_processing.py # Data cleaning and transformation
│   ├── gold_processing.py   # Feature engineering
│   ├── model_train.py       # Model training and evaluation
│   ├── s3_download.py       # S3 data download
│   ├── s3_upload.py         # S3 data upload
│   └── monitoring.py        # Data quality monitoring
├── monitoring/              # Data quality and drift monitoring
│   └── evidently_config.py
├── docker-compose.mlops.yml # MLOps services configuration
├── Dockerfile              # Container definition
├── deploy.sh               # Deployment script
└── nginx.conf              # Nginx configuration
```

![Netflix MLOps Pipeline Dashboard](Webpage.png)

## Services

- **Airflow**: http://localhost:8080 (admin/admin)
- **MLflow**: http://localhost:5000
- **Monitoring Dashboard**: Integrated with Evidently

## Data Flow

1. **Bronze Layer**: Raw data ingestion from S3
2. **Silver Layer**: Data cleaning and transformation
3. **Gold Layer**: Feature engineering and model training
4. **Monitoring**: Continuous data quality checks with Evidently

## Features

- ✅ Automated data pipeline with medallion architecture
- ✅ ML experiment tracking with MLflow
- ✅ Data drift detection and monitoring
- ✅ Containerized deployment with Docker
- ✅ Scalable orchestration with Airflow
- ✅ S3 integration for data storage

## Requirements

- Docker and Docker Compose
- AWS S3 access credentials
- Python 3.8+

## Contributing

This project follows MLOps best practices for reproducible and scalable machine learning workflows.
