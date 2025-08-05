# Netflix MLOps Pipeline

A complete MLOps pipeline for Netflix recommendation system using Airflow, MLflow, and Evidently.

## Quick Start

1. Set up environment variables in `.env` file:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
```

2. Deploy the pipeline:
```bash
./deploy.sh
```

## Architecture

- **Data Processing**: Bronze, Silver, Gold layer architecture
- **Orchestration**: Apache Airflow for workflow management
- **ML Tracking**: MLflow for experiment tracking and model registry
- **Monitoring**: Evidently for data and model drift detection
- **Containerization**: Docker for consistent deployment

## Project Structure

```
├── dags/                    # Airflow DAG definitions
├── utils/                   # Data processing and ML utilities
├── monitoring/              # Data quality and drift monitoring
├── docker-compose.mlops.yml # MLOps services configuration
├── Dockerfile              # Container definition
└── deploy.sh               # Deployment script
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
