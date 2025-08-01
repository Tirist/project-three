#!/bin/bash

# Docker run script for Stock Evaluation Pipeline
# Usage: ./scripts/docker-run.sh [command] [options]

set -e

# Default values
IMAGE_NAME="stock-pipeline"
TAG="latest"
CONTAINER_NAME="stock-pipeline-run"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to check if image exists
check_image() {
    if ! docker images | grep -q "${IMAGE_NAME}.*${TAG}"; then
        print_warning "Image ${IMAGE_NAME}:${TAG} not found. Building..."
        ./scripts/docker-build.sh production $TAG
    fi
}

# Function to run pipeline
run_pipeline() {
    local mode=${1:-help}
    
    print_status "Running pipeline in mode: $mode"
    
    case $mode in
        "test")
            docker run --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                python pipeline/run_pipeline.py --test
            ;;
        "full")
            docker run --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                python pipeline/run_pipeline.py --full
            ;;
        "daily")
            docker run --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                python pipeline/run_pipeline.py --daily-integrity
            ;;
        "weekly")
            docker run --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                python pipeline/run_pipeline.py --weekly-integrity
            ;;
        "shell")
            docker run -it --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                /bin/bash
            ;;
        "python")
            docker run -it --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                python
            ;;
        "tests")
            docker run --rm \
                --name $CONTAINER_NAME \
                -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
                -e TEST_MODE=true \
                -v "$(pwd)/data:/app/data" \
                -v "$(pwd)/logs:/app/logs" \
                -v "$(pwd)/reports:/app/reports" \
                ${IMAGE_NAME}:${TAG} \
                python -m pytest tests/ -v
            ;;
        "help"|*)
            print_status "Available pipeline modes:"
            echo "  test     - Run pipeline in test mode"
            echo "  full     - Run full pipeline"
            echo "  daily    - Run daily integrity check"
            echo "  weekly   - Run weekly integrity check"
            echo "  shell    - Open shell in container"
            echo "  python   - Open Python REPL in container"
            echo "  tests    - Run test suite"
            echo "  help     - Show this help"
            ;;
    esac
}

# Function to run development environment
run_dev() {
    print_status "Starting development environment..."
    
    docker run -it --rm \
        --name "${CONTAINER_NAME}-dev" \
        -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
        -e DEBUG=true \
        -e LOG_LEVEL=DEBUG \
        -p 8888:8888 \
        -v "$(pwd):/app" \
        -v "$(pwd)/data:/app/data" \
        -v "$(pwd)/logs:/app/logs" \
        -v "$(pwd)/reports:/app/reports" \
        ${IMAGE_NAME}:development \
        python -m ipython
}

# Function to run with docker-compose
run_compose() {
    local service=${1:-pipeline}
    
    print_status "Running with docker-compose: $service"
    
    case $service in
        "pipeline")
            docker-compose up pipeline
            ;;
        "dev")
            docker-compose --profile dev up pipeline-dev
            ;;
        "test")
            docker-compose --profile test up pipeline-test
            ;;
        "monitoring")
            docker-compose --profile monitoring up monitoring
            ;;
        "all")
            docker-compose up
            ;;
        *)
            print_error "Unknown service: $service"
            print_status "Available services: pipeline, dev, test, monitoring, all"
            exit 1
            ;;
    esac
}

# Function to show container status
show_status() {
    print_status "Container status:"
    docker ps -a --filter "name=stock-pipeline" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    print_status "Image information:"
    docker images ${IMAGE_NAME} --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"
}

# Function to clean up containers
cleanup() {
    print_status "Cleaning up containers..."
    
    # Stop and remove containers
    docker stop $(docker ps -q --filter "name=stock-pipeline") 2>/dev/null || true
    docker rm $(docker ps -aq --filter "name=stock-pipeline") 2>/dev/null || true
    
    print_success "Cleanup completed"
}

# Main execution
main() {
    local command=${1:-help}
    
    # Check if Docker is running
    check_docker
    
    case $command in
        "pipeline")
            check_image
            run_pipeline ${2:-help}
            ;;
        "dev")
            check_image
            run_dev
            ;;
        "compose")
            run_compose ${2:-pipeline}
            ;;
        "status")
            show_status
            ;;
        "cleanup")
            cleanup
            ;;
        "help"|"-h"|"--help"|*)
            echo "Usage: $0 [command] [options]"
            echo ""
            echo "Commands:"
            echo "  pipeline [mode]  - Run pipeline (test|full|daily|weekly|shell|python|tests)"
            echo "  dev             - Run development environment"
            echo "  compose [service] - Run with docker-compose (pipeline|dev|test|monitoring|all)"
            echo "  status          - Show container and image status"
            echo "  cleanup         - Clean up containers"
            echo "  help            - Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 pipeline test"
            echo "  $0 pipeline full"
            echo "  $0 dev"
            echo "  $0 compose dev"
            echo "  $0 status"
            ;;
    esac
}

# Execute main function
main "$@" 