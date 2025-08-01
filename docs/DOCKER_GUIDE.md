# Docker Guide for Stock Evaluation Pipeline

This guide covers containerization setup, usage, and deployment for the Stock Evaluation Pipeline.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Docker Images](#docker-images)
- [Docker Compose](#docker-compose)
- [Development Workflow](#development-workflow)
- [Production Deployment](#production-deployment)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Overview

The project includes a comprehensive Docker setup with:

- **Multi-stage Dockerfile** with optimized layers
- **Docker Compose** for easy local development
- **Build and run scripts** for simplified operations
- **CI/CD workflows** for automated testing and deployment
- **Monitoring setup** with Prometheus

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Git

## Quick Start

### 1. Build the Image

```bash
# Build production image
./scripts/docker-build.sh production latest

# Build development image
./scripts/docker-build.sh development dev
```

### 2. Run the Pipeline

```bash
# Run in test mode
./scripts/docker-run.sh pipeline test

# Run full pipeline
./scripts/docker-run.sh pipeline full

# Run with docker-compose
docker-compose up pipeline
```

### 3. Development Environment

```bash
# Start development environment
./scripts/docker-run.sh dev

# Or with docker-compose
docker-compose --profile dev up pipeline-dev
```

## Docker Images

### Image Targets

The Dockerfile includes multiple build targets:

- **base**: Base image with Python and system dependencies
- **dependencies**: Python dependencies installation
- **production**: Optimized production image
- **development**: Development image with additional tools

### Building Images

```bash
# Build specific target
docker build --target production -t stock-pipeline:latest .

# Build with build script
./scripts/docker-build.sh production v1.0.0
./scripts/docker-build.sh development dev
```

### Image Optimization

- Multi-stage builds reduce final image size
- Layer caching for faster rebuilds
- Non-root user for security
- Health checks for monitoring

## Docker Compose

### Services

The `docker-compose.yml` includes:

- **pipeline**: Production pipeline service
- **pipeline-dev**: Development service with additional tools
- **pipeline-test**: Testing service
- **monitoring**: Prometheus monitoring (optional)

### Usage

```bash
# Start production pipeline
docker-compose up pipeline

# Start development environment
docker-compose --profile dev up pipeline-dev

# Start with monitoring
docker-compose --profile monitoring up

# Run tests
docker-compose --profile test up pipeline-test
```

### Environment Variables

Create a `.env` file with:

```bash
# Required
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Optional - Cloud Storage
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1

# Optional - Performance
MAX_WORKERS=4
CHUNK_SIZE=1000
```

## Development Workflow

### Local Development

1. **Start development container**:
   ```bash
   ./scripts/docker-run.sh dev
   ```

2. **Mount source code**:
   ```bash
   docker run -it --rm \
     -v $(pwd):/app \
     -v $(pwd)/data:/app/data \
     stock-pipeline:development
   ```

3. **Run tests**:
   ```bash
   ./scripts/docker-run.sh pipeline tests
   ```

### Code Changes

The development setup includes:
- Live code mounting
- Debug mode enabled
- Additional development tools (IPython, Jupyter)
- Hot reloading for development

## Production Deployment

### Cloud Deployment

#### AWS ECS

```bash
# Build and push to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin your-account.dkr.ecr.us-east-1.amazonaws.com
docker tag stock-pipeline:latest your-account.dkr.ecr.us-east-1.amazonaws.com/stock-pipeline:latest
docker push your-account.dkr.ecr.us-east-1.amazonaws.com/stock-pipeline:latest
```

#### Google Cloud Run

```bash
# Build and push to GCR
docker tag stock-pipeline:latest gcr.io/your-project/stock-pipeline:latest
docker push gcr.io/your-project/stock-pipeline:latest

# Deploy to Cloud Run
gcloud run deploy stock-pipeline \
  --image gcr.io/your-project/stock-pipeline:latest \
  --platform managed \
  --region us-central1
```

#### Azure Container Instances

```bash
# Build and push to ACR
az acr build --registry your-registry --image stock-pipeline:latest .

# Deploy to Container Instances
az container create \
  --resource-group your-rg \
  --name stock-pipeline \
  --image your-registry.azurecr.io/stock-pipeline:latest \
  --environment-variables ALPHA_VANTAGE_API_KEY=your_key
```

### Kubernetes Deployment

Create a `k8s-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stock-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stock-pipeline
  template:
    metadata:
      labels:
        app: stock-pipeline
    spec:
      containers:
      - name: pipeline
        image: stock-pipeline:latest
        env:
        - name: ALPHA_VANTAGE_API_KEY
          valueFrom:
            secretKeyRef:
              name: pipeline-secrets
              key: alpha-vantage-key
        volumeMounts:
        - name: data-volume
          mountPath: /app/data
        - name: logs-volume
          mountPath: /app/logs
      volumes:
      - name: data-volume
        persistentVolumeClaim:
          claimName: pipeline-data-pvc
      - name: logs-volume
        persistentVolumeClaim:
          claimName: pipeline-logs-pvc
```

## Monitoring

### Prometheus Setup

The monitoring service includes:

- **Prometheus** for metrics collection
- **Health checks** for container monitoring
- **Custom metrics** for pipeline performance

### Accessing Metrics

```bash
# Start monitoring
docker-compose --profile monitoring up monitoring

# Access Prometheus UI
open http://localhost:9090
```

### Custom Metrics

Add metrics to your pipeline:

```python
import prometheus_client

# Define metrics
pipeline_duration = prometheus_client.Histogram(
    'pipeline_duration_seconds',
    'Time spent processing pipeline'
)

# Use in pipeline
with pipeline_duration.time():
    # Your pipeline code
    pass
```

## Troubleshooting

### Common Issues

#### Build Failures

```bash
# Clean build cache
docker builder prune

# Rebuild without cache
docker build --no-cache -t stock-pipeline:latest .
```

#### Permission Issues

```bash
# Fix volume permissions
sudo chown -R $USER:$USER data/ logs/ reports/
```

#### Memory Issues

```bash
# Increase Docker memory limit
# In Docker Desktop: Settings > Resources > Memory > 4GB
```

#### Network Issues

```bash
# Check container networking
docker network ls
docker network inspect stock-pipeline-network
```

### Debug Commands

```bash
# Check container logs
docker logs stock-pipeline

# Execute commands in running container
docker exec -it stock-pipeline /bin/bash

# Check container status
./scripts/docker-run.sh status

# Clean up containers
./scripts/docker-run.sh cleanup
```

### Performance Optimization

1. **Use multi-stage builds** to reduce image size
2. **Mount volumes** for data persistence
3. **Use .dockerignore** to exclude unnecessary files
4. **Optimize layer caching** by copying requirements first
5. **Use health checks** for better monitoring

## Security Best Practices

1. **Non-root user**: Container runs as non-root user
2. **Secrets management**: Use environment variables for sensitive data
3. **Image scanning**: Regular vulnerability scans in CI/CD
4. **Minimal base image**: Using slim Python image
5. **Regular updates**: Keep base images updated

## Next Steps

1. **Set up CI/CD**: Configure GitHub Actions for automated builds
2. **Add monitoring**: Implement custom metrics and alerts
3. **Scale deployment**: Set up Kubernetes or cloud-native deployment
4. **Security hardening**: Implement additional security measures
5. **Performance tuning**: Optimize for your specific workload

For more information, see the [API Documentation](api/) and [Troubleshooting Guide](troubleshooting/). 