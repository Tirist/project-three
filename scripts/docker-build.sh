#!/bin/bash

# Docker build script for Stock Evaluation Pipeline
# Usage: ./scripts/docker-build.sh [target] [tag]

set -e

# Default values
TARGET=${1:-production}
TAG=${2:-latest}
IMAGE_NAME="stock-pipeline"

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

# Function to build image
build_image() {
    local target=$1
    local tag=$2
    
    print_status "Building Docker image: ${IMAGE_NAME}:${tag} (target: ${target})"
    
    # Build arguments
    BUILD_ARGS=""
    
    # Add build arguments if they exist
    if [ ! -z "$ALPHA_VANTAGE_API_KEY" ]; then
        BUILD_ARGS="$BUILD_ARGS --build-arg ALPHA_VANTAGE_API_KEY=$ALPHA_VANTAGE_API_KEY"
    fi
    
    # Build the image
    docker build \
        --target $target \
        --tag ${IMAGE_NAME}:${tag} \
        $BUILD_ARGS \
        .
    
    if [ $? -eq 0 ]; then
        print_success "Successfully built ${IMAGE_NAME}:${tag}"
    else
        print_error "Failed to build ${IMAGE_NAME}:${tag}"
        exit 1
    fi
}

# Function to run tests
run_tests() {
    print_status "Running tests in container..."
    
    docker run --rm \
        -e ALPHA_VANTAGE_API_KEY="$ALPHA_VANTAGE_API_KEY" \
        -e TEST_MODE=true \
        ${IMAGE_NAME}:${TAG} \
        python -m pytest tests/ -v
    
    if [ $? -eq 0 ]; then
        print_success "All tests passed!"
    else
        print_error "Tests failed!"
        exit 1
    fi
}

# Function to show image info
show_image_info() {
    print_status "Image information:"
    docker images ${IMAGE_NAME}:${TAG}
    
    print_status "Image size:"
    docker images ${IMAGE_NAME}:${TAG} --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
}

# Function to clean up old images
cleanup_old_images() {
    print_status "Cleaning up old images..."
    
    # Remove dangling images
    docker image prune -f
    
    # Remove old versions of this image (keep last 3)
    docker images ${IMAGE_NAME} --format "{{.ID}}" | tail -n +4 | xargs -r docker rmi -f
    
    print_success "Cleanup completed"
}

# Main execution
main() {
    print_status "Starting Docker build process..."
    
    # Check if Docker is running
    check_docker
    
    # Validate target
    case $TARGET in
        "production"|"development"|"dependencies"|"base")
            ;;
        *)
            print_error "Invalid target: $TARGET"
            print_status "Valid targets: production, development, dependencies, base"
            exit 1
            ;;
    esac
    
    # Build the image
    build_image $TARGET $TAG
    
    # Show image information
    show_image_info
    
    # Run tests if building production image
    if [ "$TARGET" = "production" ]; then
        read -p "Run tests? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            run_tests
        fi
    fi
    
    # Cleanup old images
    read -p "Clean up old images? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup_old_images
    fi
    
    print_success "Build process completed successfully!"
    print_status "To run the container:"
    echo "  docker run -it --rm ${IMAGE_NAME}:${TAG}"
    echo "  docker-compose up pipeline"
}

# Handle script arguments
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [target] [tag]"
        echo ""
        echo "Targets:"
        echo "  production    - Production image (default)"
        echo "  development   - Development image with additional tools"
        echo "  dependencies  - Dependencies only"
        echo "  base          - Base image with system dependencies"
        echo ""
        echo "Examples:"
        echo "  $0 production latest"
        echo "  $0 development dev"
        echo "  $0"
        exit 0
        ;;
    *)
        main
        ;;
esac 