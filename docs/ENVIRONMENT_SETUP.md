# Environment Setup Guide

This guide explains how to set up your environment for the Stock Evaluation Pipeline, including API keys and configuration management.

## Quick Start

### 1. Automatic Setup (Recommended)

Run the interactive setup script:

```bash
python scripts/setup_environment.py
```

This script will:
- Guide you through creating a `.env` file
- Help you configure API keys and cloud storage
- Validate your setup
- Provide next steps

### 2. Manual Setup

If you prefer to set up manually:

```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file with your actual values
nano .env  # or use your preferred editor
```

## Required Configuration

### Alpha Vantage API Key

The pipeline requires an Alpha Vantage API key for fetching stock data.

1. **Get a free API key:**
   - Visit: https://www.alphavantage.co/support/#api-key
   - Sign up for a free account
   - Copy your API key

2. **Add to your .env file:**
   ```bash
   ALPHA_VANTAGE_API_KEY=your_actual_api_key_here
   ```

## Optional Configuration

### Cloud Storage (Optional)

If you want to use cloud storage instead of local filesystem:

#### AWS S3
```bash
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_DEFAULT_REGION=us-east-1
```

#### Google Cloud Storage
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

#### Azure Blob Storage
```bash
AZURE_STORAGE_CONNECTION_STRING=your_azure_connection_string
```

### Performance Configuration (Optional)

```bash
MAX_WORKERS=4
CHUNK_SIZE=1000
```

### Logging Configuration (Optional)

```bash
LOG_LEVEL=INFO
LOG_FILE=logs/pipeline.log
```

## Environment File Structure

Your `.env` file should look like this:

```bash
# Alpha Vantage API Configuration
ALPHA_VANTAGE_API_KEY=your_actual_api_key_here

# Optional: Cloud Storage Configuration
# AWS_ACCESS_KEY_ID=your_aws_access_key_id
# AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
# AWS_DEFAULT_REGION=us-east-1

# Optional: Performance Configuration
# MAX_WORKERS=4
# CHUNK_SIZE=1000
```

## Security Best Practices

### 1. Never Commit Sensitive Data

- ✅ `.env` file is in `.gitignore`
- ✅ API keys are loaded from environment variables
- ✅ Configuration files contain no sensitive data

### 2. Use Environment Variables

Always use environment variables for sensitive data:

```python
# Good - uses environment variable
api_key = os.environ.get('ALPHA_VANTAGE_API_KEY')

# Bad - hardcoded in source
api_key = "6CVZ0Z8NSWD386W0"
```

### 3. Validate Configuration

Run validation to ensure your setup is correct:

```bash
python pipeline/utils/config_validator.py
```

## Configuration Validation

The pipeline includes comprehensive configuration validation:

### API Key Validation
- Checks that Alpha Vantage API key is set
- Validates key format and length
- Warns about placeholder values

### Cloud Storage Validation
- Validates AWS credentials if S3 is configured
- Checks Google Cloud credentials file exists
- Validates Azure connection strings

### Path Validation
- Ensures data and log directories exist
- Creates missing directories automatically
- Validates write permissions

### Performance Validation
- Validates batch sizes and worker counts
- Checks retry settings
- Warns about potentially problematic values

## Troubleshooting

### Common Issues

#### 1. "ALPHA_VANTAGE_API_KEY is not set"
```bash
# Solution: Add to your .env file
echo "ALPHA_VANTAGE_API_KEY=your_key_here" >> .env
```

#### 2. "Configuration file not found"
```bash
# Solution: Ensure you're in the project root directory
pwd  # Should show project root
ls config/settings.yaml  # Should exist
```

#### 3. "Cannot create base data path"
```bash
# Solution: Check permissions
ls -la data/  # Check if directory exists and is writable
mkdir -p data/  # Create if missing
```

#### 4. "ImportError: dotenv"
```bash
# Solution: Install python-dotenv
pip install python-dotenv
```

### Validation Commands

Check your environment setup:
```bash
python scripts/setup_environment.py
```

Validate configuration:
```bash
python pipeline/utils/config_validator.py
```

Test API connectivity:
```bash
python -c "
import os
from alpha_vantage.timeseries import TimeSeries
ts = TimeSeries(key=os.environ['ALPHA_VANTAGE_API_KEY'])
print('API key is valid!')
"
```

## Development vs Production

### Development Environment
- Use local filesystem storage
- Enable debug logging
- Use smaller batch sizes for testing

### Production Environment
- Use cloud storage for scalability
- Configure proper logging levels
- Optimize performance settings
- Use environment-specific API keys

## Environment Variables Reference

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `ALPHA_VANTAGE_API_KEY` | Yes | Alpha Vantage API key | `ABC123DEF456` |
| `AWS_ACCESS_KEY_ID` | No | AWS access key for S3 | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | No | AWS secret key for S3 | `wJalr...` |
| `AWS_DEFAULT_REGION` | No | AWS region | `us-east-1` |
| `GOOGLE_APPLICATION_CREDENTIALS` | No | GCS service account file | `/path/to/key.json` |
| `AZURE_STORAGE_CONNECTION_STRING` | No | Azure connection string | `DefaultEndpoints...` |
| `MAX_WORKERS` | No | Maximum parallel workers | `4` |
| `CHUNK_SIZE` | No | Data processing chunk size | `1000` |
| `LOG_LEVEL` | No | Logging level | `INFO` |
| `LOG_FILE` | No | Log file path | `logs/pipeline.log` |

## Next Steps

After setting up your environment:

1. **Run the pipeline:**
   ```bash
   python pipeline/run_pipeline.py
   ```

2. **Check the logs:**
   ```bash
   tail -f logs/fetch.log
   ```

3. **View the data:**
   ```bash
   ls data/raw/
   ```

4. **Monitor performance:**
   ```bash
   python tools/monitoring/performance_monitor.py
   ```

## Support

If you encounter issues:

1. Check the troubleshooting section above
2. Run the validation script: `python scripts/setup_environment.py`
3. Check the logs in the `logs/` directory
4. Review the configuration documentation 