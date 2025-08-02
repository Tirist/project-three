# Cloud Deployment Guide

## Overview
This guide covers the complete deployment of the Stock Evaluation Pipeline to Google Cloud Run with Google Cloud Storage integration.

## Prerequisites

### 1. Google Cloud Setup
- Google Cloud account with billing enabled
- Google Cloud CLI installed and authenticated
- Docker installed locally
- Python 3.8+ with virtual environment

### 2. Required APIs
Enable the following Google Cloud APIs:
```bash
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com
gcloud services enable storage.googleapis.com
```

## Service Account Configuration

### 1. Create Service Account
```bash
# Create service account
gcloud iam service-accounts create stock-pipeline-sa \
    --display-name="Stock Pipeline Service Account"

# Get the service account email
SA_EMAIL=$(gcloud iam service-accounts list \
    --filter="displayName:Stock Pipeline Service Account" \
    --format="value(email)")
```

### 2. Grant Required Permissions
```bash
# Grant Cloud Storage permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectCreator"

# Grant Cloud Run permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/run.invoker"
```

### 3. Create and Download Key
```bash
# Create service account key
gcloud iam service-accounts keys create service-account-key.json \
    --iam-account=$SA_EMAIL

# Store key securely (never commit to version control)
mv service-account-key.json ~/.config/gcloud/
```

## Docker Configuration

### 1. Service Account Key Setup
The project supports multiple ways to configure Google Cloud credentials:

**Option A: Environment Variable (Recommended for Docker)**
```bash
# Set the environment variable to point to the mounted service account key
export GOOGLE_APPLICATION_CREDENTIALS=/app/config/service-account-key.json
```

**Option B: Configuration File**
```yaml
# In config/cloud_settings.yaml
gcs:
  credentials_file: "./service-account-key.json"
```

The system automatically handles credential configuration with the following priority:
1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable (highest priority)
2. `credentials_file` in `config/cloud_settings.yaml`
3. Default Google Cloud SDK authentication

### 2. Docker Compose Configuration
The Docker Compose configuration automatically:
- Mounts the service account key file: `./service-account-key.json:/app/config/service-account-key.json:ro`
- Sets the environment variable: `GOOGLE_APPLICATION_CREDENTIALS=/app/config/service-account-key.json`

### 3. Running with Docker
```bash
# Start the pipeline with Google Cloud Storage
docker-compose up pipeline

# For development
docker-compose --profile dev up pipeline-dev

# For testing
docker-compose --profile test up pipeline-test
```

### 4. Using Cloud Storage in Docker
```bash
# Run the pipeline with GCS storage
docker-compose run --rm pipeline python pipeline/process_features.py \
    --storage-provider gcs \
    --storage-config config/cloud_settings.yaml \
    --test-mode
```

### 5. Enhanced Configuration Loading
The system now automatically handles Google Cloud credentials with intelligent fallback:

1. **Environment Variable Priority**: If `GOOGLE_APPLICATION_CREDENTIALS` is set, it takes precedence
2. **Configuration File Fallback**: If not set via environment, the system checks `config/cloud_settings.yaml`
3. **Automatic Environment Setting**: If credentials are found in config, the system automatically sets the environment variable
4. **Google Cloud SDK**: Falls back to default Google Cloud SDK authentication if neither is configured

This provides flexibility for different deployment scenarios while maintaining security best practices.
```

## Environment Configuration

### 1. Local Development (.env file)
```bash
# Alpha Vantage API Configuration
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Google Cloud Storage Configuration (Optional - can also use config file)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Performance Configuration
MAX_WORKERS=4
CHUNK_SIZE=500
```

**Note**: For Google Cloud credentials, you can either:
- Set `GOOGLE_APPLICATION_CREDENTIALS` in your `.env` file, or
- Configure `credentials_file` in `config/cloud_settings.yaml`

### 2. Cloud Run Environment Variables
```bash
# Set environment variables for Cloud Run
gcloud run services update stock-pipeline \
    --region us-central1 \
    --set-env-vars \
    ALPHA_VANTAGE_API_KEY=your_api_key_here,\
    STORAGE_PROVIDER=gcs,\
    GOOGLE_CLOUD_PROJECT=your-project-id

# For Google Cloud credentials, you have two options:

# Option A: Use environment variable (if you have the key file)
gcloud run services update stock-pipeline \
    --region us-central1 \
    --set-env-vars \
    GOOGLE_APPLICATION_CREDENTIALS=/app/config/service-account-key.json

# Option B: Use service account attached to Cloud Run (recommended)
# The service account will be automatically used for authentication
```

## Persistent Storage in Cloud Run

### Important: Cloud Run Storage Limitations
Cloud Run containers are **stateless** and have **ephemeral filesystems**. This means:

1. **No persistent local storage**: Files written to the container filesystem are lost when the container stops
2. **Temporary storage only**: Use `/tmp` for temporary files during processing
3. **External storage required**: All persistent data must be stored in Google Cloud Storage

### Storage Strategy
```python
# In your application code
import os

# Use /tmp for temporary files
TEMP_DIR = "/tmp"
os.makedirs(TEMP_DIR, exist_ok=True)

# All persistent data goes to GCS
STORAGE_PROVIDER = "gcs"
GCS_BUCKET = "your-bucket-name"
```

### Cloud Storage Bucket Setup
```bash
# Create GCS bucket
gsutil mb gs://your-bucket-name

# Set bucket permissions
gsutil iam ch serviceAccount:$SA_EMAIL:objectViewer gs://your-bucket-name
gsutil iam ch serviceAccount:$SA_EMAIL:objectCreator gs://your-bucket-name
```

## Docker Configuration

### 1. Dockerfile for Cloud Run
```dockerfile
# Use production stage for Cloud Run
FROM python:3.9-slim as production

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port for Cloud Run
EXPOSE 8080

# Set environment variables
ENV PYTHONPATH=/app
ENV PORT=8080

# Run Flask app
CMD ["python", "app.py"]
```

### 2. Build and Push
```bash
# Build Docker image
docker build --target production -t stock-pipeline:latest .

# Tag for Google Container Registry
docker tag stock-pipeline:latest \
    gcr.io/$PROJECT_ID/stock-pipeline:latest

# Push to registry
docker push gcr.io/$PROJECT_ID/stock-pipeline:latest
```

## Deployment

### 1. Deploy to Cloud Run
```bash
gcloud run deploy stock-pipeline \
    --image gcr.io/$PROJECT_ID/stock-pipeline:latest \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 2Gi \
    --cpu 2 \
    --timeout 3600 \
    --max-instances 10
```

### 2. Configure Environment Variables
```bash
gcloud run services update stock-pipeline \
    --region us-central1 \
    --set-env-vars \
    ALPHA_VANTAGE_API_KEY=$ALPHA_VANTAGE_API_KEY,\
    STORAGE_PROVIDER=gcs,\
    GOOGLE_CLOUD_PROJECT=$PROJECT_ID
```

## Testing the Deployment

### 1. Health Check
```bash
curl https://stock-pipeline-xxxxx.us-central1.run.app/
```

### 2. Run Pipeline
```bash
# Test mode
curl -X POST https://stock-pipeline-xxxxx.us-central1.run.app/run \
    -H "Content-Type: application/json" \
    -d '{"mode": "test", "storage_provider": "gcs"}'

# Production mode
curl -X POST https://stock-pipeline-xxxxx.us-central1.run.app/run \
    -H "Content-Type: application/json" \
    -d '{"mode": "prod", "storage_provider": "gcs"}'
```

## Monitoring and Logging

### 1. View Logs
```bash
# View Cloud Run logs
gcloud logs read "resource.type=cloud_run_revision" \
    --limit 50 \
    --format "table(timestamp,severity,textPayload)"

# View specific service logs
gcloud logs read "resource.labels.service_name=stock-pipeline" \
    --limit 50
```

### 2. Monitor Storage Usage
```bash
# Check GCS bucket usage
gsutil du -sh gs://your-bucket-name

# List recent files
gsutil ls -l gs://your-bucket-name/data/ | tail -20
```

## Security Best Practices

### 1. Service Account Security
- Use least privilege principle
- Rotate service account keys regularly
- Never commit keys to version control
- Use Workload Identity when possible

### 2. Credential Management
- **Local Development**: Use environment variables or configuration files
- **Docker Deployments**: Mount service account keys as read-only volumes
- **Cloud Run**: Use service account attached to Cloud Run (recommended)
- **Configuration Priority**: Environment variables > config files > default auth

### 3. Environment Variables
- Store sensitive data in Secret Manager
- Use environment variables for configuration
- Validate all inputs

### 3. Network Security
- Use VPC connectors if needed
- Configure proper IAM roles
- Enable audit logging

## Troubleshooting

### Common Issues

#### 1. Permission Denied
```bash
# Check service account permissions
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:$SA_EMAIL"
```

#### 2. Storage Backend Issues
```bash
# Test GCS connectivity
gsutil ls gs://your-bucket-name/

# Check credentials
gcloud auth application-default print-access-token
```

#### 3. Container Build Issues
```bash
# Build with verbose output
docker build --target production -t stock-pipeline:latest . --progress=plain

# Check image layers
docker history stock-pipeline:latest
```

### Debug Mode
```bash
# Deploy with debug logging
gcloud run services update stock-pipeline \
    --region us-central1 \
    --set-env-vars DEBUG=true
```

## Cost Optimization

### 1. Resource Allocation
- Start with minimal resources (1 CPU, 512MB RAM)
- Scale based on actual usage
- Use autoscaling appropriately

### 2. Storage Costs
- Implement data lifecycle policies
- Use appropriate storage classes
- Monitor and clean up old data

### 3. Compute Costs
- Use concurrency settings to optimize
- Monitor cold start frequency
- Consider using Cloud Functions for simple tasks

## Maintenance

### 1. Regular Updates
```bash
# Update dependencies
pip install --upgrade -r requirements.txt

# Rebuild and redeploy
docker build --target production -t stock-pipeline:latest .
docker tag stock-pipeline:latest gcr.io/$PROJECT_ID/stock-pipeline:latest
docker push gcr.io/$PROJECT_ID/stock-pipeline:latest
gcloud run deploy stock-pipeline --image gcr.io/$PROJECT_ID/stock-pipeline:latest
```

### 2. Backup Strategy
- Regular GCS bucket backups
- Export configuration
- Document deployment procedures

### 3. Monitoring
- Set up Cloud Monitoring alerts
- Monitor API quotas
- Track storage usage trends 