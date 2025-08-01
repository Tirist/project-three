#!/usr/bin/env python3
"""
cloud_storage_example.py

Example demonstrating how to use the extended DataManager with different storage backends.
"""

import os
import sys
from datetime import datetime
import pandas as pd

# Add the pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'pipeline'))

from utils.common import DataManager, create_storage_backend

def example_local_storage():
    """Example using local filesystem storage (default behavior)."""
    print("=== Local Storage Example ===")
    
    # Create DataManager with local storage (default)
    data_manager = DataManager(base_dir="data", test_mode=True)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'ticker': ['AAPL', 'GOOGL', 'MSFT'],
        'price': [150.0, 2800.0, 300.0],
        'volume': [1000000, 500000, 750000]
    })
    
    # Save data
    data_manager.save_dataframe(sample_data, "data/test/raw/dt=2024-01-01/sample.parquet")
    
    # Load data
    loaded_data = data_manager.load_dataframe("data/test/raw/dt=2024-01-01/sample.parquet")
    print(f"Loaded data shape: {loaded_data.shape}")
    
    # Check storage info
    print(f"Storage info: {data_manager.get_storage_info()}")
    print()

def example_s3_storage():
    """Example using AWS S3 storage (requires boto3 and AWS credentials)."""
    print("=== S3 Storage Example ===")
    
    try:
        # Create S3 storage backend
        s3_backend = create_storage_backend(
            storage_type="s3",
            bucket_name="your-bucket-name",
            # aws_access_key_id="your-access-key",  # Optional if using AWS credentials
            # aws_secret_access_key="your-secret-key",  # Optional if using AWS credentials
            # region_name="us-east-1"  # Optional
        )
        
        # Create DataManager with S3 backend
        data_manager = DataManager(
            base_dir="project-three-data",
            test_mode=True,
            storage_backend=s3_backend
        )
        
        # Create sample data
        sample_data = pd.DataFrame({
            'ticker': ['TSLA', 'AMZN', 'NVDA'],
            'price': [250.0, 3300.0, 450.0],
            'volume': [2000000, 800000, 1200000]
        })
        
        # Save data to S3
        data_manager.save_dataframe(sample_data, "project-three-data/test/raw/dt=2024-01-01/sample.parquet")
        
        # Load data from S3
        loaded_data = data_manager.load_dataframe("project-three-data/test/raw/dt=2024-01-01/sample.parquet")
        print(f"Loaded data from S3 shape: {loaded_data.shape}")
        
        # Check storage info
        print(f"Storage info: {data_manager.get_storage_info()}")
        
    except ImportError as e:
        print(f"S3 storage not available: {e}")
        print("Install boto3: pip install boto3")
    except Exception as e:
        print(f"Error with S3 storage: {e}")
    print()

def example_gcs_storage():
    """Example using Google Cloud Storage (requires google-cloud-storage and GCP credentials)."""
    print("=== Google Cloud Storage Example ===")
    
    try:
        # Create GCS storage backend
        gcs_backend = create_storage_backend(
            storage_type="gcs",
            bucket_name="your-gcs-bucket-name",
            # project_id="your-project-id"  # Optional if using default project
        )
        
        # Create DataManager with GCS backend
        data_manager = DataManager(
            base_dir="project-three-data",
            test_mode=True,
            storage_backend=gcs_backend
        )
        
        # Create sample data
        sample_data = pd.DataFrame({
            'ticker': ['META', 'NFLX', 'ADBE'],
            'price': [350.0, 450.0, 550.0],
            'volume': [1500000, 900000, 600000]
        })
        
        # Save data to GCS
        data_manager.save_dataframe(sample_data, "project-three-data/test/raw/dt=2024-01-01/sample.parquet")
        
        # Load data from GCS
        loaded_data = data_manager.load_dataframe("project-three-data/test/raw/dt=2024-01-01/sample.parquet")
        print(f"Loaded data from GCS shape: {loaded_data.shape}")
        
        # Check storage info
        print(f"Storage info: {data_manager.get_storage_info()}")
        
    except ImportError as e:
        print(f"GCS storage not available: {e}")
        print("Install google-cloud-storage: pip install google-cloud-storage")
    except Exception as e:
        print(f"Error with GCS storage: {e}")
    print()

def example_migration():
    """Example of migrating data between storage backends."""
    print("=== Storage Migration Example ===")
    
    # Create local DataManager
    local_manager = DataManager(base_dir="data", test_mode=True)
    
    # Create sample data locally
    sample_data = pd.DataFrame({
        'ticker': ['AAPL', 'GOOGL', 'MSFT', 'TSLA'],
        'price': [150.0, 2800.0, 300.0, 250.0],
        'volume': [1000000, 500000, 750000, 2000000],
        'timestamp': [datetime.now()] * 4
    })
    
    # Save to local storage
    local_path = "data/test/raw/dt=2024-01-01/migration_sample.parquet"
    local_manager.save_dataframe(sample_data, local_path)
    print(f"Saved data locally: {local_path}")
    
    # Load from local storage
    loaded_data = local_manager.load_dataframe(local_path)
    print(f"Loaded data shape: {loaded_data.shape}")
    
    # Example: Migrate to cloud storage (commented out as it requires credentials)
    """
    # Create cloud storage backend
    cloud_backend = create_storage_backend(
        storage_type="s3",
        bucket_name="your-bucket-name"
    )
    
    # Create cloud DataManager
    cloud_manager = DataManager(
        base_dir="project-three-data",
        test_mode=True,
        storage_backend=cloud_backend
    )
    
    # Save to cloud storage
    cloud_path = "project-three-data/test/raw/dt=2024-01-01/migration_sample.parquet"
    cloud_manager.save_dataframe(loaded_data, cloud_path)
    print(f"Migrated data to cloud: {cloud_path}")
    """
    
    print("Migration example completed (cloud migration commented out)")
    print()

def main():
    """Run all examples."""
    print("DataManager Cloud Storage Examples")
    print("=" * 50)
    
    # Run examples
    example_local_storage()
    example_s3_storage()
    example_gcs_storage()
    example_migration()
    
    print("All examples completed!")
    print("\nTo use cloud storage:")
    print("1. Install required packages: pip install boto3 google-cloud-storage")
    print("2. Configure credentials for your cloud provider")
    print("3. Update bucket names and paths in the examples")
    print("4. Uncomment the cloud storage sections in the code")

if __name__ == "__main__":
    main() 