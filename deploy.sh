#!/bin/bash

# Netflix MLOps Pipeline Local Deployment Script
# This script helps deploy the complete MLOps pipeline locally

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="netflix-mlops"
DOCKER_COMPOSE_FILE="docker-compose.mlops.yml"
REQUIREMENTS_FILE="requirements-mlops.txt"

# Functions
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  Netflix MLOps Pipeline - Local Only${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

check_dependencies() {
    print_info "Checking dependencies..."
    
    # Add Docker to PATH if needed (for macOS)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Compose first."
        exit 1
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    
    print_success "All dependencies are installed"
}

setup_local_environment() {
    print_info "Setting up local environment..."
    
    # Create necessary directories
    mkdir -p logs
    mkdir -p plugins
    mkdir -p notebooks
    mkdir -p monitoring/reports
    mkdir -p data/raw
    mkdir -p data/processed
    mkdir -p data/features
    
    # Create .env file if it doesn't exist
    if [ ! -f .env ]; then
        print_info "Creating .env file..."
        cat > .env << EOF
# AWS S3 Environment Variables
# Replace with your actual AWS credentials
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_DEFAULT_REGION=us-west-2
MLFLOW_TRACKING_URI=http://mlflow:5000
S3_BUCKET_DATA=netflix-data
S3_BUCKET_ARTIFACTS=mlflow-artifacts
S3_BUCKET_MONITORING=monitoring-reports
EOF
        print_success "Created .env file"
    fi
    
    print_success "Local environment setup complete"
}

# Sample data creation removed - using real Netflix Prize datamart data

deploy_local() {
    print_info "Deploying MLOps pipeline locally..."
    
    # Add Docker to PATH if needed (for macOS)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
    fi
    
    # Pull latest images
    print_info "Pulling Docker images..."
    docker compose -f $DOCKER_COMPOSE_FILE pull
    
    # Start services
    print_info "Starting services..."
    docker compose -f $DOCKER_COMPOSE_FILE up -d
    
    # Wait for services to be ready
    print_info "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    check_local_services
    
    print_success "Local deployment complete!"
    print_service_urls
}

check_local_services() {
    print_info "Checking service health..."
    
    services=("mlflow" "evidently" "nginx")
    
    for service in "${services[@]}"; do
        if docker compose -f $DOCKER_COMPOSE_FILE ps $service | grep -q "healthy\|Up"; then
            print_success "$service is running"
        else
            print_warning "$service might not be ready yet"
        fi
    done
}

print_service_urls() {
    echo ""
    print_info "Service URLs:"
    echo "  🏠 Main Dashboard: http://localhost"
    echo "  📊 MLflow UI: http://localhost/mlflow/"
    echo "  🔄 Airflow UI: http://localhost/airflow/ (admin/admin)"
    echo "  📈 Evidently AI: http://localhost/evidently/"
    echo "  📓 Jupyter Lab: http://localhost/jupyter/ (token: mlops)"
    echo ""
    print_info "Note: Make sure to include trailing slashes for proper routing"
}

start_airflow() {
    print_info "Starting Airflow services..."
    docker compose -f $DOCKER_COMPOSE_FILE up -d airflow-init
    sleep 10
    docker compose -f $DOCKER_COMPOSE_FILE up -d airflow-webserver airflow-scheduler
    print_success "Airflow services started"
}

stop_local() {
    print_info "Stopping local MLOps pipeline..."
    docker compose -f $DOCKER_COMPOSE_FILE down
    print_success "Local pipeline stopped"
}

cleanup_local() {
    print_info "Cleaning up local MLOps pipeline..."
    docker compose -f $DOCKER_COMPOSE_FILE down -v
    docker system prune -f
    print_success "Local cleanup complete"
}

show_logs() {
    print_info "Showing service logs..."
    docker compose -f $DOCKER_COMPOSE_FILE logs -f
}

show_status() {
    print_info "Service status:"
    docker compose -f $DOCKER_COMPOSE_FILE ps
}

# Sample notebook creation removed - notebooks can be created manually as needed

show_help() {
    echo "Netflix MLOps Pipeline Local Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  deploy          Deploy the complete pipeline locally"
    echo "  start-airflow   Start Airflow services"
    echo "  stop            Stop the local pipeline"
    echo "  cleanup         Stop and remove all local containers and volumes"
    echo "  logs            Show service logs"
    echo "  status          Show service status"
    echo "  help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 deploy          # Deploy complete pipeline"
    echo "  $0 start-airflow   # Start Airflow after deployment"
    echo "  $0 status          # Check service status"
    echo ""
}

# Main script logic
print_header

case "${1:-help}" in
    "deploy")
        check_dependencies
        setup_local_environment
        deploy_local
        ;;
    "start-airflow")
        start_airflow
        ;;
    "stop")
        stop_local
        ;;
    "cleanup")
        cleanup_local
        ;;
    "logs")
        show_logs
        ;;
    "status")
        show_status
        ;;

    "help")
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac