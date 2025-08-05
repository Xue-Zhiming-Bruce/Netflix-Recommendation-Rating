# Netflix MLOps Pipeline

A complete MLOps pipeline for Netflix recommendation system using Airflow, MLflow, and Evidently. This pipeline processes the Netflix Prize dataset to build and deploy machine learning models for movie recommendations.

## Dataset

This project uses the **Netflix Prize Dataset** from Kaggle:
- **Source**: [Netflix Prize Data](https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data)
- **Description**: Contains over 100 million ratings from 480,000 users on 17,770 movies
- **Format**: User-movie rating pairs with timestamps
- **Goal**: Predict user ratings for movies to improve recommendation accuracy

## Quick Start

1. **Download the dataset** from [Kaggle Netflix Prize Data](https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data)

2. **Set up environment variables** in `.env` file:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
MLFLOW_TRACKING_URI=http://localhost:5000
AIRFLOW_UID=50000
```

3. **Upload dataset to S3** (or place in local data directory)

4. **Deploy the pipeline**:
```bash
chmod +x deploy.sh
./deploy.sh
```

5. **Access the services**:
   - Airflow UI: http://localhost:8080 (admin/admin)
   - MLflow UI: http://localhost:5000
   - Monitor logs and pipeline execution

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

1. **Bronze Layer**: 
   - Raw Netflix dataset ingestion from S3
   - Data validation and schema enforcement
   - Stores original CSV files in bronze directory

2. **Silver Layer**: 
   - Data cleaning and preprocessing
   - Handle missing values and outliers
   - User and movie feature extraction
   - Normalized rating data

3. **Gold Layer**: 
   - Feature engineering for ML models
   - User-item interaction matrices
   - Model training (collaborative filtering, matrix factorization)
   - Model evaluation and validation

4. **Monitoring**: 
   - Data quality checks with Evidently
   - Model performance monitoring
   - Drift detection for user behavior changes

## Features

- ✅ Automated data pipeline with medallion architecture
- ✅ ML experiment tracking with MLflow
- ✅ Data drift detection and monitoring
- ✅ Containerized deployment with Docker
- ✅ Scalable orchestration with Airflow
- ✅ S3 integration for data storage

## Requirements

- **Docker** and **Docker Compose** (latest versions)
- **AWS S3** access credentials for data storage
- **Python 3.8+** for local development
- **8GB+ RAM** recommended for processing large datasets
- **Kaggle account** to download the Netflix Prize dataset
- **Git** for version control

## Contributing

This project follows MLOps best practices for reproducible and scalable machine learning workflows.
