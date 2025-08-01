#!/usr/bin/env python3
"""
common.py

Common utilities and shared functions for the stock evaluation pipeline.
Consolidates duplicate logic from fetch_tickers.py, fetch_data.py, and process_features.py.
"""

import json
import logging
import os
import sys
import time
import shutil
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, BinaryIO
import io

import pandas as pd
import yaml

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, continue without it
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Storage backend imports (optional dependencies)
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import ResourceNotFoundError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        pass
    
    @abstractmethod
    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = True) -> None:
        """Create a directory."""
        pass
    
    @abstractmethod
    def listdir(self, path: str) -> List[str]:
        """List contents of a directory."""
        pass
    
    @abstractmethod
    def read_file(self, path: str, mode: str = 'r') -> Union[str, bytes]:
        """Read a file."""
        pass
    
    @abstractmethod
    def write_file(self, path: str, content: Union[str, bytes], mode: str = 'w') -> None:
        """Write content to a file."""
        pass
    
    @abstractmethod
    def delete_file(self, path: str) -> None:
        """Delete a file."""
        pass
    
    @abstractmethod
    def delete_directory(self, path: str) -> None:
        """Delete a directory and its contents."""
        pass
    
    @abstractmethod
    def get_file_size(self, path: str) -> int:
        """Get file size in bytes."""
        pass
    
    @abstractmethod
    def get_last_modified(self, path: str) -> datetime:
        """Get last modified time of a file."""
        pass

class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend."""
    
    def exists(self, path: str) -> bool:
        return Path(path).exists()
    
    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = True) -> None:
        Path(path).mkdir(parents=parents, exist_ok=exist_ok)
    
    def listdir(self, path: str) -> List[str]:
        path_obj = Path(path)
        if not path_obj.exists():
            return []
        return [item.name for item in path_obj.iterdir()]
    
    def read_file(self, path: str, mode: str = 'r') -> Union[str, bytes]:
        with open(path, mode) as f:
            return f.read()
    
    def write_file(self, path: str, content: Union[str, bytes], mode: str = 'w') -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, mode) as f:
            f.write(content)
    
    def delete_file(self, path: str) -> None:
        Path(path).unlink(missing_ok=True)
    
    def delete_directory(self, path: str) -> None:
        shutil.rmtree(path, ignore_errors=True)
    
    def get_file_size(self, path: str) -> int:
        return Path(path).stat().st_size
    
    def get_last_modified(self, path: str) -> datetime:
        return datetime.fromtimestamp(Path(path).stat().st_mtime)

class S3StorageBackend(StorageBackend):
    """AWS S3 storage backend."""
    
    def __init__(self, bucket_name: str, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None, region_name: Optional[str] = None):
        if not S3_AVAILABLE:
            raise ImportError("boto3 is required for S3 storage. Install with: pip install boto3")
        
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
    
    def _normalize_path(self, path: str) -> str:
        """Normalize path for S3 (remove leading slash, handle empty path)."""
        return path.lstrip('/') if path != '/' else ''
    
    def exists(self, path: str) -> bool:
        try:
            normalized_path = self._normalize_path(path)
            if not normalized_path:  # Root bucket
                return True
            
            # Check if it's a file
            self.s3_client.head_object(Bucket=self.bucket_name, Key=normalized_path)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Check if it's a directory (prefix)
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=normalized_path.rstrip('/') + '/',
                    MaxKeys=1
                )
                return 'Contents' in response
            return False
        except Exception:
            return False
    
    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = True) -> None:
        # S3 doesn't have real directories, but we can create a placeholder object
        normalized_path = self._normalize_path(path)
        if normalized_path and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        if normalized_path:
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=normalized_path + '.keep',
                    Body=''
                )
            except Exception as e:
                if not exist_ok:
                    raise e
    
    def listdir(self, path: str) -> List[str]:
        normalized_path = self._normalize_path(path)
        if normalized_path and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=normalized_path,
                Delimiter='/'
            )
            
            items = []
            
            # Add common prefixes (directories)
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    prefix_name = prefix['Prefix'][len(normalized_path):].rstrip('/')
                    if prefix_name:
                        items.append(prefix_name)
            
            # Add objects (files)
            if 'Contents' in response:
                for obj in response['Contents']:
                    obj_name = obj['Key'][len(normalized_path):]
                    if obj_name and not obj_name.endswith('.keep'):
                        items.append(obj_name)
            
            return items
        except Exception:
            return []
    
    def read_file(self, path: str, mode: str = 'r') -> Union[str, bytes]:
        normalized_path = self._normalize_path(path)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=normalized_path)
            content = response['Body'].read()
            
            if mode == 'r':
                return content.decode('utf-8')
            return content
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"File not found: {path}")
            raise
    
    def write_file(self, path: str, content: Union[str, bytes], mode: str = 'w') -> None:
        normalized_path = self._normalize_path(path)
        
        if mode == 'w' and isinstance(content, str):
            content = content.encode('utf-8')
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=normalized_path,
                Body=content
            )
        except Exception as e:
            raise IOError(f"Failed to write file {path}: {e}")
    
    def delete_file(self, path: str) -> None:
        normalized_path = self._normalize_path(path)
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=normalized_path)
        except Exception:
            pass  # Ignore errors for missing files
    
    def delete_directory(self, path: str) -> None:
        normalized_path = self._normalize_path(path)
        if normalized_path and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        try:
            # List all objects with the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=normalized_path
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    self.s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
        except Exception:
            pass
    
    def get_file_size(self, path: str) -> int:
        normalized_path = self._normalize_path(path)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=normalized_path)
            return response['ContentLength']
        except ClientError:
            return 0
    
    def get_last_modified(self, path: str) -> datetime:
        normalized_path = self._normalize_path(path)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=normalized_path)
            return response['LastModified'].replace(tzinfo=None)
        except ClientError:
            return datetime.now()

class GCSStorageBackend(StorageBackend):
    """Google Cloud Storage backend."""
    
    def __init__(self, bucket_name: str, project_id: Optional[str] = None):
        if not GCS_AVAILABLE:
            raise ImportError("google-cloud-storage is required for GCS storage. Install with: pip install google-cloud-storage")
        
        self.bucket_name = bucket_name
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)
    
    def _normalize_path(self, path: str) -> str:
        """Normalize path for GCS."""
        return path.lstrip('/') if path != '/' else ''
    
    def exists(self, path: str) -> bool:
        normalized_path = self._normalize_path(path)
        if not normalized_path:  # Root bucket
            return True
        
        blob = self.bucket.blob(normalized_path)
        return blob.exists()
    
    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = True) -> None:
        # GCS doesn't have real directories, but we can create a placeholder object
        normalized_path = self._normalize_path(path)
        if normalized_path and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        if normalized_path:
            try:
                blob = self.bucket.blob(normalized_path + '.keep')
                blob.upload_from_string('')
            except Exception as e:
                if not exist_ok:
                    raise e
    
    def listdir(self, path: str) -> List[str]:
        normalized_path = self._normalize_path(path)
        if normalized_path and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=normalized_path, delimiter='/')
        
        items = []
        prefixes = set()
        
        for blob in blobs:
            if blob.name.startswith(normalized_path):
                relative_name = blob.name[len(normalized_path):]
                if relative_name and not relative_name.endswith('.keep'):
                    # Remove trailing slash for directories
                    if relative_name.endswith('/'):
                        relative_name = relative_name[:-1]
                    items.append(relative_name)
        
        return list(set(items))
    
    def read_file(self, path: str, mode: str = 'r') -> Union[str, bytes]:
        normalized_path = self._normalize_path(path)
        blob = self.bucket.blob(normalized_path)
        
        if not blob.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        content = blob.download_as_bytes()
        
        if mode == 'r':
            return content.decode('utf-8')
        return content
    
    def write_file(self, path: str, content: Union[str, bytes], mode: str = 'w') -> None:
        normalized_path = self._normalize_path(path)
        blob = self.bucket.blob(normalized_path)
        
        if mode == 'w' and isinstance(content, str):
            content = content.encode('utf-8')
        
        blob.upload_from_string(content)
    
    def delete_file(self, path: str) -> None:
        normalized_path = self._normalize_path(path)
        blob = self.bucket.blob(normalized_path)
        blob.delete(ignore_errors=True)
    
    def delete_directory(self, path: str) -> None:
        normalized_path = self._normalize_path(path)
        if normalized_path and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=normalized_path)
        for blob in blobs:
            blob.delete(ignore_errors=True)
    
    def get_file_size(self, path: str) -> int:
        normalized_path = self._normalize_path(path)
        blob = self.bucket.blob(normalized_path)
        if blob.exists():
            blob.reload()
            return blob.size
        return 0
    
    def get_last_modified(self, path: str) -> datetime:
        normalized_path = self._normalize_path(path)
        blob = self.bucket.blob(normalized_path)
        if blob.exists():
            blob.reload()
            return blob.updated.replace(tzinfo=None)
        return datetime.now()

class DataManager:
    """Manages data directory structure and file operations with support for multiple storage backends."""
    
    def __init__(self, base_dir: str = "data", test_mode: bool = False, 
                 storage_backend: Optional[StorageBackend] = None):
        self.base_dir = base_dir
        self.test_mode = test_mode
        self.storage = storage_backend or LocalStorageBackend()
        
        # Set up directory paths
        if test_mode:
            self.data_dir = f"{base_dir}/test"
        else:
            self.data_dir = base_dir
        
        self.raw_dir = f"{self.data_dir}/raw"
        self.processed_dir = f"{self.data_dir}/processed"
        self.tickers_dir = f"{self.data_dir}/tickers"
        
        # Create directories if they don't exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories."""
        for directory in [self.raw_dir, self.processed_dir, self.tickers_dir]:
            self.storage.mkdir(directory, parents=True, exist_ok=True)
    
    def get_partition_path(self, date: Union[str, datetime], data_type: str) -> str:
        """Get partition path for a specific date and data type."""
        if isinstance(date, str):
            date_str = date
        else:
            date_str = date.strftime("%Y-%m-%d")
        
        partition_name = f"dt={date_str}"
        
        if data_type == "raw":
            return f"{self.raw_dir}/{partition_name}"
        elif data_type == "processed":
            return f"{self.processed_dir}/{partition_name}"
        elif data_type == "tickers":
            return f"{self.tickers_dir}/{partition_name}"
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    def partition_exists(self, date: Union[str, datetime], data_type: str) -> bool:
        """Check if a partition exists."""
        partition_path = self.get_partition_path(date, data_type)
        return self.storage.exists(partition_path)
    
    def list_partitions(self, data_type: str) -> List[str]:
        """List all partitions for a data type."""
        base_path = self.get_partition_path("", data_type).rsplit('/', 1)[0]
        
        if not self.storage.exists(base_path):
            return []
        
        partitions = []
        for item in self.storage.listdir(base_path):
            if item.startswith("dt="):
                partitions.append(item[3:])  # Remove "dt=" prefix
        
        return sorted(partitions)
    
    def cleanup_old_partitions(self, retention_days: int, data_type: str) -> int:
        """Clean up old partitions based on retention policy."""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        deleted_count = 0
        
        for partition_date_str in self.list_partitions(data_type):
            try:
                partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
                if partition_date < cutoff_date:
                    partition_path = self.get_partition_path(partition_date_str, data_type)
                    self.storage.delete_directory(partition_path)
                    deleted_count += 1
            except ValueError:
                continue
        
        return deleted_count
    
    def save_dataframe(self, df: pd.DataFrame, path: str, format: str = 'parquet') -> None:
        """Save DataFrame to storage."""
        if format == 'parquet':
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            self.storage.write_file(path, buffer.getvalue(), mode='wb')
        elif format == 'csv':
            csv_content = df.to_csv(index=False)
            self.storage.write_file(path, csv_content, mode='w')
        elif format == 'json':
            json_content = df.to_json(orient='records', indent=2)
            self.storage.write_file(path, json_content, mode='w')
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def load_dataframe(self, path: str, format: str = 'parquet') -> pd.DataFrame:
        """Load DataFrame from storage."""
        if format == 'parquet':
            content = self.storage.read_file(path, mode='rb')
            buffer = io.BytesIO(content)
            return pd.read_parquet(buffer)
        elif format == 'csv':
            content = self.storage.read_file(path, mode='r')
            buffer = io.StringIO(content)
            return pd.read_csv(buffer)
        elif format == 'json':
            content = self.storage.read_file(path, mode='r')
            buffer = io.StringIO(content)
            return pd.read_json(buffer, orient='records')
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def save_json(self, data: Dict[str, Any], path: str) -> None:
        """Save JSON data to storage."""
        json_content = json.dumps(data, indent=2, default=str)
        self.storage.write_file(path, json_content, mode='w')
    
    def load_json(self, path: str) -> Dict[str, Any]:
        """Load JSON data from storage."""
        content = self.storage.read_file(path, mode='r')
        return json.loads(content)
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about the storage backend."""
        return {
            "backend_type": type(self.storage).__name__,
            "base_dir": self.base_dir,
            "test_mode": self.test_mode,
            "data_dir": self.data_dir
        }

# Factory function for creating storage backends
def create_storage_backend(storage_type: str = "local", **kwargs) -> StorageBackend:
    """Create a storage backend based on type."""
    if storage_type == "local":
        return LocalStorageBackend()
    elif storage_type == "s3":
        return S3StorageBackend(**kwargs)
    elif storage_type == "gcs":
        return GCSStorageBackend(**kwargs)
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

def load_config(config_path: str, config_type: str = "general") -> Dict[str, Any]:
    """
    Load configuration from YAML file with fallback defaults.
    Supports environment variables for sensitive data.
    
    Args:
        config_path: Path to configuration file
        config_type: Type of configuration ('tickers', 'ohlcv', 'general')
        
    Returns:
        Dictionary containing configuration settings
    """
    # Define default configurations for different types
    default_configs = {
        "tickers": {
            "ticker_source": "sp500",
            "data_source": "wikipedia",
            "base_data_path": "data/",
            "base_log_path": "logs/",
            "ticker_data_path": "tickers",
            "ticker_log_path": "tickers",
            "min_tickers_expected": 500,
            "max_tickers_expected": 510,
            "api_retry_attempts": 3,
            "api_retry_delay": 1,
            "retention_days": 30,
            "cleanup_enabled": True,
            "cleanup_log_path": "cleanup",
            "rate_limit_enabled": True,
            "rate_limit_strategy": "exponential_backoff",
            "max_rate_limit_hits": 10,
            "base_cooldown_seconds": 1,
            "max_cooldown_seconds": 60,
            "batch_size": 10,
            "performance_logging": True
        },
        "ohlcv": {
            "base_data_path": "data/",
            "base_log_path": "logs/",
            "ohlcv_data_path": "raw",
            "ohlcv_log_path": "fetch",
            "historical_data_path": "raw/historical",
            "retention_days": 3,
            "api_retry_attempts": 3,
            "api_retry_delay": 1,
            "alpha_vantage_api_key": "",
            "cleanup_enabled": True,
            "cleanup_log_path": "cleanup",
            "rate_limit_enabled": True,
            "rate_limit_strategy": "exponential_backoff",
            "max_rate_limit_hits": 10,
            "base_cooldown_seconds": 1,
            "max_cooldown_seconds": 60,
            "batch_size": 10,
            "performance_logging": True,
            "progress": True,
            "parallel_workers": None,
            "adaptive_reduce_every": 3,
            "incremental_mode": True,
            "min_historical_days": 730
        },
        "general": {
            "base_data_path": "data/",
            "base_log_path": "logs/",
            "api_retry_attempts": 3,
            "api_retry_delay": 1,
            "retention_days": 30,
            "cleanup_enabled": True,
            "rate_limit_enabled": True,
            "rate_limit_strategy": "exponential_backoff",
            "max_rate_limit_hits": 10,
            "base_cooldown_seconds": 1,
            "max_cooldown_seconds": 60,
            "batch_size": 10,
            "performance_logging": True
        }
    }
    
    # Get the appropriate default config
    default_config = default_configs.get(config_type, default_configs["general"])
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            # Merge with defaults for missing keys
            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
    except FileNotFoundError:
        logging.warning(f"Config file {config_path} not found, using defaults")
        config = default_config
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file: {e}")
        config = default_config
    
    # Override sensitive values with environment variables
    config = _override_with_env_vars(config)
    
    return config

def _override_with_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Override configuration values with environment variables for sensitive data.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Updated configuration dictionary
    """
    # API Keys
    if os.environ.get('ALPHA_VANTAGE_API_KEY'):
        config['alpha_vantage_api_key'] = os.environ['ALPHA_VANTAGE_API_KEY']
    
    # Cloud Storage Configuration
    if os.environ.get('AWS_ACCESS_KEY_ID'):
        config['aws_access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']
    if os.environ.get('AWS_SECRET_ACCESS_KEY'):
        config['aws_secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']
    if os.environ.get('AWS_DEFAULT_REGION'):
        config['aws_default_region'] = os.environ['AWS_DEFAULT_REGION']
    
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        config['google_application_credentials'] = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    
    if os.environ.get('AZURE_STORAGE_CONNECTION_STRING'):
        config['azure_storage_connection_string'] = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    
    # Database Configuration
    if os.environ.get('DATABASE_URL'):
        config['database_url'] = os.environ['DATABASE_URL']
    
    # Logging Configuration
    if os.environ.get('LOG_LEVEL'):
        config['log_level'] = os.environ['LOG_LEVEL']
    if os.environ.get('LOG_FILE'):
        config['log_file'] = os.environ['LOG_FILE']
    
    # Performance Configuration
    if os.environ.get('MAX_WORKERS'):
        try:
            config['max_workers'] = int(os.environ['MAX_WORKERS'])
        except ValueError:
            logging.warning(f"Invalid MAX_WORKERS value: {os.environ['MAX_WORKERS']}")
    
    if os.environ.get('CHUNK_SIZE'):
        try:
            config['chunk_size'] = int(os.environ['CHUNK_SIZE'])
        except ValueError:
            logging.warning(f"Invalid CHUNK_SIZE value: {os.environ['CHUNK_SIZE']}")
    
    return config

class PipelineConfig:
    """Centralized configuration management for the pipeline."""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.settings = self._load_settings()
        self.test_schedules = self._load_test_schedules()
    
    def _load_settings(self) -> Dict[str, Any]:
        """Load main settings from config/settings.yaml."""
        settings_path = self.config_dir / "settings.yaml"
        if settings_path.exists():
            with open(settings_path, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def _load_test_schedules(self) -> Dict[str, Any]:
        """Load test schedules from config/test_schedules.yaml."""
        schedules_path = self.config_dir / "test_schedules.yaml"
        if schedules_path.exists():
            with open(schedules_path, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with fallback to default."""
        return self.settings.get(key, default)
    
    def get_test_config(self, key: str, default: Any = None) -> Any:
        """Get test configuration value with fallback to default."""
        return self.test_schedules.get(key, default)

class LogManager:
    """Manages logging and metadata for the pipeline."""
    
    def __init__(self, base_dir: str = "logs", test_mode: bool = False):
        self.base_dir = Path(base_dir)
        self.test_mode = test_mode
        
        # Set up log directories
        if test_mode:
            self.log_dir = self.base_dir / "test"
        else:
            self.log_dir = self.base_dir
        
        self.fetch_dir = self.log_dir / "fetch"
        self.features_dir = self.log_dir / "features"
        self.tickers_dir = self.log_dir / "tickers"
        self.cleanup_dir = self.log_dir / "cleanup"
        self.integrity_dir = self.log_dir / "integrity"
        
        # Create directories
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary log directories."""
        for directory in [self.fetch_dir, self.features_dir, self.tickers_dir, 
                         self.cleanup_dir, self.integrity_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get a logger instance with file and console handlers."""
        logger = logging.getLogger(name)
        
        if not logger.handlers:
            # File handler
            log_file = self.log_dir / f"{name}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            
            # Formatter
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.setLevel(logging.INFO)
        
        return logger
    
    def save_metadata(self, stage: str, metadata: Dict[str, Any], date: Union[str, datetime] = None):
        """Save metadata for a pipeline stage."""
        if date is None:
            date = datetime.now()
        
        if isinstance(date, datetime):
            date_str = date.strftime("%Y-%m-%d")
        else:
            date_str = date
        
        # Create stage directory
        stage_dir = self.log_dir / stage
        stage_dir.mkdir(exist_ok=True)
        
        # Create date partition
        partition_dir = stage_dir / f"dt={date_str}"
        partition_dir.mkdir(exist_ok=True)
        
        # Save metadata
        metadata_file = partition_dir / "metadata.json"
        metadata["timestamp"] = datetime.now().isoformat()
        metadata["stage"] = stage
        metadata["test_mode"] = self.test_mode
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
    def load_metadata(self, stage: str, date: Union[str, datetime]) -> Optional[Dict[str, Any]]:
        """Load metadata for a pipeline stage."""
        if isinstance(date, datetime):
            date_str = date.strftime("%Y-%m-%d")
        else:
            date_str = date
        
        metadata_file = self.log_dir / stage / f"dt={date_str}" / "metadata.json"
        
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                return json.load(f)
        
        return None

# Reusable utility functions

def create_partition_paths(date_str: str, config: Dict[str, Any], data_type: str, test_mode: bool = False) -> Tuple[Path, Path]:
    """
    Create partitioned folder paths for data and logs.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        config: Configuration dictionary
        data_type: Type of data ('tickers', 'raw', 'processed')
        test_mode: If True, use test directories instead of production
        
    Returns:
        Tuple of (data_path, log_path) Path objects
    """
    if test_mode:
        # Use test directories for test mode
        if data_type == "tickers":
            data_path = Path("data/test/tickers") / f"dt={date_str}"
            log_path = Path("logs/test/tickers") / f"dt={date_str}"
        elif data_type == "raw":
            data_path = Path("data/test/raw") / f"dt={date_str}"
            log_path = Path("logs/test/fetch") / f"dt={date_str}"
        elif data_type == "processed":
            data_path = Path("data/test/processed") / f"dt={date_str}"
            log_path = Path("logs/test/features") / f"dt={date_str}"
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    else:
        # Use production directories
        base_data_path = config.get("base_data_path", "data/")
        base_log_path = config.get("base_log_path", "logs/")
        
        if data_type == "tickers":
            data_path = Path(base_data_path) / config.get("ticker_data_path", "tickers") / f"dt={date_str}"
            log_path = Path(base_log_path) / config.get("ticker_log_path", "tickers") / f"dt={date_str}"
        elif data_type == "raw":
            data_path = Path(base_data_path) / config.get("ohlcv_data_path", "raw") / f"dt={date_str}"
            log_path = Path(base_log_path) / config.get("ohlcv_log_path", "fetch") / f"dt={date_str}"
        elif data_type == "processed":
            data_path = Path(base_data_path) / config.get("processed_data_path", "processed") / f"dt={date_str}"
            log_path = Path(base_log_path) / config.get("features_log_path", "features") / f"dt={date_str}"
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    # Ensure directories exist
    data_path.mkdir(parents=True, exist_ok=True)
    log_path.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Created partition paths: {data_path}, {log_path}")
    return data_path, log_path

def save_metadata_to_file(metadata: Dict[str, Any], log_path: Path, dry_run: bool = False) -> str:
    """
    Save metadata to JSON file.
    
    Args:
        metadata: Dictionary containing metadata
        log_path: Path to save the metadata file
        dry_run: If True, don't actually save files
        
    Returns:
        Path to the saved metadata file
    """
    metadata_path = log_path / "metadata.json"
    
    if dry_run:
        logging.info(f"[DRY RUN] Would save metadata to {metadata_path}")
        return str(metadata_path)
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    logging.info(f"Saved metadata to {metadata_path}")
    return str(metadata_path)

def cleanup_old_partitions(config: Dict[str, Any], data_type: str, dry_run: bool = False, test_mode: bool = False) -> Dict[str, Any]:
    """
    Clean up old partitions based on retention policy.
    
    Args:
        config: Configuration dictionary
        data_type: Type of data ('tickers', 'raw', 'processed')
        dry_run: If True, don't actually delete files
        test_mode: If True, clean test directories
        
    Returns:
        Dictionary containing cleanup results
    """
    retention_days = config.get("retention_days", 30)
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    if test_mode:
        if data_type == "tickers":
            base_data_path = Path("data/test/tickers")
            base_log_path = Path("logs/test/tickers")
        elif data_type == "raw":
            base_data_path = Path("data/test/raw")
            base_log_path = Path("logs/test/fetch")
        elif data_type == "processed":
            base_data_path = Path("data/test/processed")
            base_log_path = Path("logs/test/features")
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    else:
        base_data_path = Path(config.get("base_data_path", "data/"))
        base_log_path = Path(config.get("base_log_path", "logs/"))
        
        if data_type == "tickers":
            base_data_path = base_data_path / config.get("ticker_data_path", "tickers")
            base_log_path = base_log_path / config.get("ticker_log_path", "tickers")
        elif data_type == "raw":
            base_data_path = base_data_path / config.get("ohlcv_data_path", "raw")
            base_log_path = base_log_path / config.get("ohlcv_log_path", "fetch")
        elif data_type == "processed":
            base_data_path = base_data_path / config.get("processed_data_path", "processed")
            base_log_path = base_log_path / config.get("features_log_path", "features")
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    deleted_partitions = []
    total_deleted = 0
    
    # Clean up data partitions
    if base_data_path.exists():
        for partition_dir in base_data_path.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                try:
                    partition_date_str = partition_dir.name[3:]  # Remove "dt=" prefix
                    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
                    
                    if partition_date < cutoff_date:
                        if dry_run:
                            logging.info(f"[DRY RUN] Would delete old partition: {partition_dir}")
                        else:
                            shutil.rmtree(partition_dir)
                            logging.info(f"Deleted old partition: {partition_dir}")
                        deleted_partitions.append(str(partition_dir))
                        total_deleted += 1
                except ValueError:
                    logging.warning(f"Could not parse date from partition name: {partition_dir.name}")
    
    # Clean up log partitions
    if base_log_path.exists():
        for partition_dir in base_log_path.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                try:
                    partition_date_str = partition_dir.name[3:]  # Remove "dt=" prefix
                    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
                    
                    if partition_date < cutoff_date:
                        if dry_run:
                            logging.info(f"[DRY RUN] Would delete old log partition: {partition_dir}")
                        else:
                            shutil.rmtree(partition_dir)
                            logging.info(f"Deleted old log partition: {partition_dir}")
                        deleted_partitions.append(str(partition_dir))
                        total_deleted += 1
                except ValueError:
                    logging.warning(f"Could not parse date from log partition name: {partition_dir.name}")
    
    # Save cleanup log
    cleanup_log = {
        "cleanup_date": datetime.now().isoformat(),
        "retention_days": retention_days,
        "cutoff_date": cutoff_date.isoformat(),
        "deleted_partitions": deleted_partitions,
        "total_deleted": total_deleted,
        "dry_run": dry_run,
        "test_mode": test_mode,
        "data_type": data_type
    }
    
    if test_mode:
        cleanup_log_path = Path("logs/test/cleanup")
    else:
        cleanup_log_path = Path(config.get("base_log_path", "logs/")) / config.get("cleanup_log_path", "cleanup")
    
    cleanup_log_path.mkdir(parents=True, exist_ok=True)
    cleanup_file = cleanup_log_path / f"cleanup_{datetime.now().strftime('%Y-%m-%d')}.json"
    
    if not dry_run:
        with open(cleanup_file, 'w') as f:
            json.dump(cleanup_log, f, indent=2)
        logging.info(f"Saved cleanup log to {cleanup_file}")
    
    return cleanup_log

def handle_rate_limit(attempt: int, config: Dict[str, Any]) -> None:
    """
    Handle rate limiting with exponential backoff.
    
    Args:
        attempt: Current attempt number
        config: Configuration dictionary
    """
    max_hits = config.get("max_rate_limit_hits", 10)
    base_cooldown = config.get("base_cooldown_seconds", 1)
    max_cooldown = config.get("max_cooldown_seconds", 60)
    
    if attempt >= max_hits:
        cooldown = max_cooldown
    else:
        cooldown = min(base_cooldown * (2 ** attempt), max_cooldown)
    
    debug = config.get("debug_rate_limit", False)
    if debug:
        print(f"[DEBUG-RATE-LIMIT] Simulating rate limit hit. Sleeping for {cooldown} seconds (attempt {attempt})")
    logging.info(f"Rate limit cooldown: {cooldown} seconds (attempt {attempt})")
    time.sleep(cooldown)



def validate_dataframe(df: pd.DataFrame, required_columns: List[str], 
                      min_rows: int = 0) -> Tuple[bool, List[str]]:
    """Validate DataFrame structure and content."""
    errors = []
    
    # Check required columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {missing_columns}")
    
    # Check minimum rows
    if len(df) < min_rows:
        errors.append(f"DataFrame has {len(df)} rows, minimum required: {min_rows}")
    
    # Check for null values in required columns
    for col in required_columns:
        if col in df.columns and df[col].isnull().any():
            null_count = df[col].isnull().sum()
            errors.append(f"Column '{col}' has {null_count} null values")
    
    return len(errors) == 0, errors

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero."""
    if denominator == 0:
        return default
    return numerator / denominator 