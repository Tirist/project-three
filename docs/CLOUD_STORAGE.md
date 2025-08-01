# Cloud Storage Support

The `DataManager` class has been extended to support multiple storage backends, including cloud storage providers. This allows for seamless migration from local filesystem storage to cloud storage while maintaining the same API.

## Overview

The new architecture uses a pluggable storage backend system with the following components:

- **StorageBackend**: Abstract base class defining the interface for all storage backends
- **LocalStorageBackend**: Implementation for local filesystem storage (default)
- **S3StorageBackend**: Implementation for AWS S3 storage
- **GCSStorageBackend**: Implementation for Google Cloud Storage
- **DataManager**: High-level interface that works with any storage backend

## Supported Storage Backends

### 1. Local Filesystem (Default)
```python
from pipeline.utils.common import DataManager

# Default behavior - uses local filesystem
data_manager = DataManager(base_dir="data", test_mode=False)
```

### 2. AWS S3
```python
from pipeline.utils.common import DataManager, create_storage_backend

# Create S3 backend
s3_backend = create_storage_backend(
    storage_type="s3",
    bucket_name="your-bucket-name",
    aws_access_key_id="your-access-key",  # Optional if using AWS credentials
    aws_secret_access_key="your-secret-key",  # Optional if using AWS credentials
    region_name="us-east-1"  # Optional
)

# Create DataManager with S3 backend
data_manager = DataManager(
    base_dir="project-three-data",
    test_mode=False,
    storage_backend=s3_backend
)
```

### 3. Google Cloud Storage
```python
from pipeline.utils.common import DataManager, create_storage_backend

# Create GCS backend
gcs_backend = create_storage_backend(
    storage_type="gcs",
    bucket_name="your-gcs-bucket-name",
    project_id="your-project-id"  # Optional if using default project
)

# Create DataManager with GCS backend
data_manager = DataManager(
    base_dir="project-three-data",
    test_mode=False,
    storage_backend=gcs_backend
)
```

## Installation

### Required Dependencies

The core functionality works with the existing dependencies. For cloud storage, install the optional dependencies:

```bash
# For AWS S3
pip install boto3

# For Google Cloud Storage
pip install google-cloud-storage

# For Azure Blob Storage (future support)
pip install azure-storage-blob
```

### Configuration

Add the cloud storage dependencies to your `requirements.txt`:

```txt
# Optional cloud storage backends (uncomment as needed)
# AWS S3 support
boto3>=1.26.0

# Google Cloud Storage support
google-cloud-storage>=2.8.0

# Azure Blob Storage support
azure-storage-blob>=12.17.0
```

## Usage Examples

### Basic Usage (Backward Compatible)

The existing code continues to work without changes:

```python
from pipeline.utils.common import DataManager

# This works exactly as before
data_manager = DataManager(base_dir="data", test_mode=False)

# All existing methods work the same way
data_manager.save_dataframe(df, "data/raw/dt=2024-01-01/data.parquet")
data_manager.load_dataframe("data/raw/dt=2024-01-01/data.parquet")
```

### Cloud Storage Usage

```python
from pipeline.utils.common import DataManager, create_storage_backend

# Create cloud storage backend
cloud_backend = create_storage_backend(
    storage_type="s3",
    bucket_name="my-stock-data-bucket"
)

# Create DataManager with cloud backend
data_manager = DataManager(
    base_dir="stock-data",
    test_mode=False,
    storage_backend=cloud_backend
)

# Use the same API - no code changes needed
data_manager.save_dataframe(df, "stock-data/raw/dt=2024-01-01/data.parquet")
data_manager.load_dataframe("stock-data/raw/dt=2024-01-01/data.parquet")
```

### Data Migration

Easily migrate data between storage backends:

```python
from pipeline.utils.common import DataManager, create_storage_backend

# Load from local storage
local_manager = DataManager(base_dir="data", test_mode=False)
data = local_manager.load_dataframe("data/raw/dt=2024-01-01/data.parquet")

# Save to cloud storage
cloud_backend = create_storage_backend(storage_type="s3", bucket_name="my-bucket")
cloud_manager = DataManager(base_dir="stock-data", storage_backend=cloud_backend)
cloud_manager.save_dataframe(data, "stock-data/raw/dt=2024-01-01/data.parquet")
```

## API Reference

### DataManager Methods

All existing `DataManager` methods work with any storage backend:

- `get_partition_path(date, data_type)` - Get partition path
- `partition_exists(date, data_type)` - Check if partition exists
- `list_partitions(data_type)` - List all partitions
- `cleanup_old_partitions(retention_days, data_type)` - Clean up old data
- `save_dataframe(df, path, format='parquet')` - Save DataFrame
- `load_dataframe(path, format='parquet')` - Load DataFrame
- `save_json(data, path)` - Save JSON data
- `load_json(path)` - Load JSON data
- `get_storage_info()` - Get storage backend information

### Storage Backend Interface

All storage backends implement the same interface:

- `exists(path)` - Check if path exists
- `mkdir(path, parents=True, exist_ok=True)` - Create directory
- `listdir(path)` - List directory contents
- `read_file(path, mode='r')` - Read file
- `write_file(path, content, mode='w')` - Write file
- `delete_file(path)` - Delete file
- `delete_directory(path)` - Delete directory
- `get_file_size(path)` - Get file size
- `get_last_modified(path)` - Get last modified time

## Configuration

### Environment Variables

For cloud storage, you can use environment variables for credentials:

**AWS S3:**
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

**Google Cloud Storage:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

### Configuration Files

You can also configure storage backends in your configuration files:

```yaml
# config/settings.yaml
storage:
  type: "s3"  # or "gcs", "local"
  bucket_name: "my-stock-data-bucket"
  region: "us-east-1"
  # Other provider-specific settings
```

## Best Practices

### 1. Error Handling

Always handle storage-specific errors:

```python
try:
    data_manager.save_dataframe(df, path)
except FileNotFoundError:
    print("File not found")
except PermissionError:
    print("Permission denied")
except Exception as e:
    print(f"Storage error: {e}")
```

### 2. Performance Considerations

- **Local Storage**: Fastest for small to medium datasets
- **Cloud Storage**: Better for large datasets and distributed access
- **Caching**: Consider implementing caching for frequently accessed data
- **Batch Operations**: Use batch operations when possible for cloud storage

### 3. Cost Optimization

- **S3 Lifecycle Policies**: Configure automatic deletion of old data
- **GCS Object Lifecycle**: Use lifecycle management for cost control
- **Storage Classes**: Use appropriate storage classes (e.g., S3 Standard-IA for infrequently accessed data)

### 4. Security

- **IAM Roles**: Use IAM roles instead of access keys when possible
- **Bucket Policies**: Configure appropriate bucket policies
- **Encryption**: Enable server-side encryption for sensitive data

## Migration Guide

### From Local to Cloud Storage

1. **Install Dependencies:**
   ```bash
   pip install boto3  # for S3
   # or
   pip install google-cloud-storage  # for GCS
   ```

2. **Configure Credentials:**
   ```bash
   # For AWS
   aws configure
   
   # For GCS
   gcloud auth application-default login
   ```

3. **Update Code:**
   ```python
   # Before (local only)
   data_manager = DataManager(base_dir="data")
   
   # After (cloud capable)
   cloud_backend = create_storage_backend(storage_type="s3", bucket_name="my-bucket")
   data_manager = DataManager(base_dir="data", storage_backend=cloud_backend)
   ```

4. **Migrate Data:**
   ```python
   # Load from local
   local_data = local_manager.load_dataframe("data/raw/dt=2024-01-01/data.parquet")
   
   # Save to cloud
   cloud_manager.save_dataframe(local_data, "data/raw/dt=2024-01-01/data.parquet")
   ```

## Troubleshooting

### Common Issues

1. **Import Errors:**
   ```
   ImportError: boto3 is required for S3 storage
   ```
   Solution: Install the required package: `pip install boto3`

2. **Authentication Errors:**
   ```
   NoCredentialsError: Unable to locate credentials
   ```
   Solution: Configure AWS credentials using `aws configure` or environment variables

3. **Permission Errors:**
   ```
   AccessDenied: Access Denied
   ```
   Solution: Check IAM permissions and bucket policies

4. **Bucket Not Found:**
   ```
   NoSuchBucket: The specified bucket does not exist
   ```
   Solution: Create the bucket or check the bucket name

### Debug Mode

Enable debug logging to troubleshoot storage issues:

```python
import logging
logging.getLogger('boto3').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.DEBUG)
```

## Future Enhancements

- **Azure Blob Storage**: Full implementation of Azure storage backend
- **Multi-Region Support**: Automatic failover between regions
- **Compression**: Automatic compression for cost optimization
- **Caching Layer**: In-memory caching for frequently accessed data
- **Parallel Operations**: Concurrent uploads/downloads for better performance 