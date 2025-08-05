# 🎬 Netflix MLOps Pipeline

A comprehensive MLOps pipeline for Netflix recommendation system using Apache Airflow, MLflow, and Evidently AI for end-to-end machine learning workflow automation.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Monitoring & Observability](#monitoring--observability)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This MLOps pipeline implements a complete machine learning workflow for Netflix recommendation systems, featuring:

- **Medallion Architecture**: Bronze, Silver, Gold data layers for progressive data refinement
- **Automated Orchestration**: Apache Airflow for workflow management and scheduling
- **Experiment Tracking**: MLflow for model versioning, metrics tracking, and artifact storage
- **Data Quality Monitoring**: Evidently AI for data drift detection and model performance monitoring
- **Containerized Deployment**: Docker and Docker Compose for consistent environments
- **Cloud Integration**: AWS S3 for scalable data storage and retrieval

## 🏗️ Architecture

### System Components

- **Data Processing**: Bronze, Silver, Gold layer architecture following medallion pattern
- **Orchestration**: Apache Airflow for workflow management and task scheduling
- **ML Tracking**: MLflow for experiment tracking, model registry, and artifact storage
- **Monitoring**: Evidently AI for data drift detection and model performance monitoring
- **Containerization**: Docker for consistent deployment across environments
- **Load Balancing**: Nginx for traffic distribution and reverse proxy
- **Storage**: AWS S3 for data lake and model artifact storage

### Data Flow Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Raw Data  │───▶│ Bronze Layer│───▶│Silver Layer │───▶│ Gold Layer  │
│   (S3)      │    │ (Ingestion) │    │(Cleaning)   │    │(Features)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                   │
                                                                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Monitoring  │◀───│   MLflow    │◀───│Model Training│◀───│Feature Store│
│ (Evidently) │    │ (Tracking)  │    │& Evaluation │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## 📁 Project Structure

```
├── dags/                           # Airflow DAG definitions
│   ├── __init__.py
│   └── netflix_mlops_pipeline.py   # Main pipeline DAG
├── utils/                          # Core processing utilities
│   ├── __init__.py
│   ├── bronze_processing.py        # Raw data ingestion from S3
│   ├── silver_processing.py        # Data cleaning and validation
│   ├── gold_processing.py          # Feature engineering and aggregation
│   ├── model_train.py              # Model training and evaluation
│   ├── s3_download.py              # S3 data download utilities
│   ├── s3_upload.py                # S3 data upload utilities
│   ├── data_extraction.py          # Data extraction utilities
│   ├── data_transformation.py      # Data transformation logic
│   ├── feature_engineering.py     # Feature engineering pipeline
│   ├── mlflow_setup.py             # MLflow configuration and setup
│   └── monitoring.py               # Data quality monitoring
├── monitoring/                     # Monitoring and observability
│   └── evidently_config.py         # Evidently AI configuration
├── docker-compose.mlops.yml        # MLOps services orchestration
├── Dockerfile                      # Container definition
├── deploy.sh                       # Deployment automation script
├── nginx.conf                      # Nginx reverse proxy configuration
├── .gitignore                      # Git ignore patterns
└── README.md                       # This documentation
```

![Netflix MLOps Pipeline Dashboard](Webpage.png)

## 🔧 Prerequisites

### System Requirements

- **Operating System**: Linux, macOS, or Windows with WSL2
- **Memory**: Minimum 8GB RAM (16GB recommended)
- **Storage**: At least 10GB free disk space
- **Network**: Internet connection for downloading dependencies

### Software Dependencies

- **Docker**: Version 20.10+ ([Installation Guide](https://docs.docker.com/get-docker/))
- **Docker Compose**: Version 2.0+ ([Installation Guide](https://docs.docker.com/compose/install/))
- **Python**: Version 3.8+ (for local development)
- **Git**: For version control

### Cloud Services

- **AWS Account**: For S3 storage access
- **AWS CLI**: Configured with appropriate credentials

## 🚀 Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Xue-Zhiming-Bruce/Netflix-Recommendation-Rating.git
cd Netflix-Recommendation-Rating
```

### 2. Environment Configuration

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration:

```env
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-netflix-data-bucket

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# MLflow Configuration
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_S3_ENDPOINT_URL=https://s3.amazonaws.com

# Evidently Configuration
EVIDENTLY_UI_PORT=8085
```

### 3. Build and Deploy

```bash
# Make deployment script executable
chmod +x deploy.sh

# Deploy the entire MLOps pipeline
./deploy.sh
```

### 4. Verify Installation

Check that all services are running:

```bash
docker-compose -f docker-compose.mlops.yml ps
```

Expected output:
```
NAME                    IMAGE                   STATUS
airflow-scheduler       apache/airflow:2.7.0   Up
airflow-webserver       apache/airflow:2.7.0   Up
mlflow-server           mlflow/mlflow:latest    Up
evidently-service       evidently/evidently     Up
postgres                postgres:13             Up
redis                   redis:7                 Up
```

## ⚙️ Configuration

### Airflow Configuration

#### DAG Configuration

Edit `dags/netflix_mlops_pipeline.py` to customize:

```python
# DAG default arguments
default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG schedule
dag = DAG(
    'netflix_mlops_pipeline',
    default_args=default_args,
    description='Netflix recommendation MLOps pipeline',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['netflix', 'mlops', 'recommendation'],
)
```

#### Connection Setup

1. Access Airflow UI at http://localhost:8080
2. Navigate to Admin → Connections
3. Add AWS connection:
   - Connection Id: `aws_default`
   - Connection Type: `Amazon Web Services`
   - Extra: `{"aws_access_key_id": "your_key", "aws_secret_access_key": "your_secret"}`

### MLflow Configuration

#### Experiment Setup

```python
# In utils/mlflow_setup.py
import mlflow

# Set tracking URI
mlflow.set_tracking_uri("http://localhost:5000")

# Create experiment
experiment_name = "netflix-recommendation"
try:
    experiment_id = mlflow.create_experiment(experiment_name)
except mlflow.exceptions.MlflowException:
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id

mlflow.set_experiment(experiment_name)
```

### Evidently Configuration

#### Monitoring Setup

```python
# In monitoring/evidently_config.py
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset

# Column mapping for Netflix data
column_mapping = ColumnMapping(
    target='rating',
    prediction='predicted_rating',
    numerical_features=['duration', 'release_year'],
    categorical_features=['genre', 'country', 'type']
)

# Data drift report
data_drift_report = Report(metrics=[
    DataDriftPreset(),
    TargetDriftPreset()
])
```

## 🎮 Usage

### Starting the Pipeline

1. **Access Airflow UI**: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

2. **Enable the DAG**: Toggle the `netflix_mlops_pipeline` DAG to "On"

3. **Trigger Manual Run**: Click "Trigger DAG" for immediate execution

### Monitoring Progress

- **Airflow**: Monitor task execution and logs
- **MLflow**: Track experiments at http://localhost:5000
- **Evidently**: View data quality reports at http://localhost:8085

### Data Pipeline Stages

#### Bronze Layer (Raw Data Ingestion)
```bash
# Manual execution
python utils/bronze_processing.py --date 2024-01-01
```

#### Silver Layer (Data Cleaning)
```bash
# Manual execution
python utils/silver_processing.py --input-path datamart/bronze --output-path datamart/silver
```

#### Gold Layer (Feature Engineering)
```bash
# Manual execution
python utils/gold_processing.py --input-path datamart/silver --output-path datamart/gold
```

## 📚 API Documentation

### Airflow REST API

#### Trigger DAG Run
```bash
curl -X POST "http://localhost:8080/api/v1/dags/netflix_mlops_pipeline/dagRuns" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic YWRtaW46YWRtaW4=" \
  -d '{"conf": {"date": "2024-01-01"}}'
```

#### Get DAG Status
```bash
curl "http://localhost:8080/api/v1/dags/netflix_mlops_pipeline" \
  -H "Authorization: Basic YWRtaW46YWRtaW4="
```

### MLflow REST API

#### List Experiments
```bash
curl "http://localhost:5000/api/2.0/mlflow/experiments/list"
```

#### Get Latest Model Version
```bash
curl "http://localhost:5000/api/2.0/mlflow/model-versions/get-latest" \
  -G -d "name=netflix-recommendation-model" -d "stage=Production"
```

### Custom API Endpoints

#### Health Check
```bash
curl "http://localhost:8080/health"
```

#### Pipeline Status
```bash
curl "http://localhost:8080/api/v1/pipeline/status"
```

## 📊 Monitoring & Observability

### Service Endpoints

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|----------|
| **Airflow** | http://localhost:8080 | admin/admin | Workflow orchestration |
| **MLflow** | http://localhost:5000 | None | Experiment tracking |
| **Evidently** | http://localhost:8085 | None | Data monitoring |
| **Nginx** | http://localhost:80 | None | Load balancer |

### Key Metrics to Monitor

#### Data Quality Metrics
- **Data Drift**: Statistical changes in feature distributions
- **Target Drift**: Changes in target variable distribution
- **Missing Values**: Percentage of missing data per feature
- **Data Volume**: Number of records processed per batch

#### Model Performance Metrics
- **RMSE**: Root Mean Square Error for rating predictions
- **MAE**: Mean Absolute Error
- **R²**: Coefficient of determination
- **Prediction Drift**: Changes in model output distribution

#### System Metrics
- **Pipeline Success Rate**: Percentage of successful DAG runs
- **Processing Time**: Time taken for each pipeline stage
- **Resource Usage**: CPU, memory, and storage utilization
- **Error Rate**: Frequency of task failures

### Setting Up Alerts

#### Airflow Email Alerts
```python
# In DAG configuration
default_args = {
    'email': ['your-email@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
```

#### Custom Monitoring Script
```python
# monitoring/custom_alerts.py
import requests
import smtplib
from email.mime.text import MIMEText

def check_pipeline_health():
    """Check pipeline health and send alerts if needed"""
    try:
        response = requests.get('http://localhost:8080/health')
        if response.status_code != 200:
            send_alert(f"Pipeline health check failed: {response.status_code}")
    except Exception as e:
        send_alert(f"Pipeline health check error: {str(e)}")

def send_alert(message):
    """Send email alert"""
    # Email configuration
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "alerts@company.com"
    sender_password = "your-password"
    recipient_email = "team@company.com"
    
    # Create message
    msg = MIMEText(message)
    msg['Subject'] = "MLOps Pipeline Alert"
    msg['From'] = sender_email
    msg['To'] = recipient_email
    
    # Send email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
```

## 🚀 Deployment

### Local Development

```bash
# Start services
docker-compose -f docker-compose.mlops.yml up -d

# View logs
docker-compose -f docker-compose.mlops.yml logs -f

# Stop services
docker-compose -f docker-compose.mlops.yml down
```

### Production Deployment

#### AWS ECS Deployment

1. **Build and push Docker image**:
```bash
# Build image
docker build -t netflix-mlops:latest .

# Tag for ECR
docker tag netflix-mlops:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/netflix-mlops:latest

# Push to ECR
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/netflix-mlops:latest
```

2. **Create ECS task definition**:
```json
{
  "family": "netflix-mlops",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "2048",
  "memory": "4096",
  "containerDefinitions": [
    {
      "name": "airflow-webserver",
      "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/netflix-mlops:latest",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "AIRFLOW__CORE__EXECUTOR",
          "value": "CeleryExecutor"
        }
      ]
    }
  ]
}
```

#### Kubernetes Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: netflix-mlops
spec:
  replicas: 3
  selector:
    matchLabels:
      app: netflix-mlops
  template:
    metadata:
      labels:
        app: netflix-mlops
    spec:
      containers:
      - name: airflow-webserver
        image: netflix-mlops:latest
        ports:
        - containerPort: 8080
        env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "KubernetesExecutor"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
```

### Environment-Specific Configurations

#### Development
```env
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG
AIRFLOW__CORE__LOAD_EXAMPLES=true
```

#### Staging
```env
ENVIRONMENT=staging
DEBUG=false
LOG_LEVEL=INFO
AIRFLOW__CORE__LOAD_EXAMPLES=false
```

#### Production
```env
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=WARNING
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Services Not Starting

**Problem**: Docker containers fail to start

**Solution**:
```bash
# Check Docker daemon
sudo systemctl status docker

# Check available resources
docker system df
docker system prune -f

# Restart Docker
sudo systemctl restart docker
```

#### 2. Airflow Database Connection Error

**Problem**: `sqlalchemy.exc.OperationalError: (psycopg2.OperationalError)`

**Solution**:
```bash
# Reset Airflow database
docker-compose -f docker-compose.mlops.yml exec airflow-webserver airflow db reset

# Initialize database
docker-compose -f docker-compose.mlops.yml exec airflow-webserver airflow db init

# Create admin user
docker-compose -f docker-compose.mlops.yml exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### 3. MLflow Tracking Server Not Accessible

**Problem**: MLflow UI returns 502 Bad Gateway

**Solution**:
```bash
# Check MLflow container logs
docker-compose -f docker-compose.mlops.yml logs mlflow-server

# Restart MLflow service
docker-compose -f docker-compose.mlops.yml restart mlflow-server

# Check port binding
netstat -tulpn | grep 5000
```

#### 4. S3 Access Denied Error

**Problem**: `botocore.exceptions.ClientError: An error occurred (AccessDenied)`

**Solution**:
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check S3 bucket permissions
aws s3api get-bucket-policy --bucket your-bucket-name

# Test S3 access
aws s3 ls s3://your-bucket-name/
```

#### 5. Memory Issues

**Problem**: Containers getting killed due to OOM

**Solution**:
```bash
# Increase Docker memory limit
# Docker Desktop: Settings → Resources → Memory

# Monitor memory usage
docker stats

# Reduce worker processes in docker-compose.yml
AIRFLOW__CELERY__WORKER_CONCURRENCY: 2
```

### Debugging Commands

#### Check Service Health
```bash
# All services status
docker-compose -f docker-compose.mlops.yml ps

# Service logs
docker-compose -f docker-compose.mlops.yml logs -f [service-name]

# Execute commands in container
docker-compose -f docker-compose.mlops.yml exec airflow-webserver bash
```

#### Database Debugging
```bash
# Connect to PostgreSQL
docker-compose -f docker-compose.mlops.yml exec postgres psql -U airflow

# Check Airflow tables
\dt

# Check DAG runs
SELECT dag_id, state, execution_date FROM dag_run ORDER BY execution_date DESC LIMIT 10;
```

#### Network Debugging
```bash
# Check network connectivity
docker network ls
docker network inspect mlops_default

# Test service connectivity
docker-compose -f docker-compose.mlops.yml exec airflow-webserver curl http://mlflow-server:5000/health
```

### Performance Optimization

#### Airflow Optimization
```python
# In airflow.cfg or environment variables
AIRFLOW__CORE__PARALLELISM = 32
AIRFLOW__CORE__DAG_CONCURRENCY = 16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG = 16
AIRFLOW__CELERY__WORKER_CONCURRENCY = 16
```

#### Database Optimization
```sql
-- PostgreSQL optimization
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
SELECT pg_reload_conf();
```

## 🤝 Contributing

### Development Setup

1. **Fork the repository**
2. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Set up development environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements-dev.txt
   ```
4. **Run tests**:
   ```bash
   pytest tests/
   ```
5. **Submit a pull request**

### Code Standards

- **Python**: Follow PEP 8 style guide
- **Documentation**: Use Google-style docstrings
- **Testing**: Maintain >80% code coverage
- **Linting**: Use `black`, `flake8`, and `isort`

### Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_bronze_processing.py -v

# Run with coverage
pytest tests/ --cov=utils --cov-report=html
```

### Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run hooks manually
pre-commit run --all-files
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:

- **Issues**: [GitHub Issues](https://github.com/Xue-Zhiming-Bruce/Netflix-Recommendation-Rating/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Xue-Zhiming-Bruce/Netflix-Recommendation-Rating/discussions)
- **Email**: support@mlops-team.com

## 🙏 Acknowledgments

- Apache Airflow community for the robust orchestration platform
- MLflow team for the excellent ML lifecycle management tools
- Evidently AI for the comprehensive monitoring capabilities
- Netflix for inspiring the recommendation system use case

---

**Built with ❤️ by the MLOps Team**

This project follows MLOps best practices for reproducible and scalable machine learning workflows.
