#!/usr/bin/env python3
"""
Example: Using Cloud Configuration File

This example demonstrates how to load and use the cloud configuration
from config/cloud_settings.yaml to set up cloud storage backends.
"""

import os
import sys
import yaml
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.common import create_storage_backend, DataManager

def load_cloud_config():
    """Load cloud configuration from config/cloud_settings.yaml."""
    config_path = Path("config/cloud_settings.yaml")
    
    if not config_path.exists():
        print("❌ config/cloud_settings.yaml not found")
        print("Please create the cloud configuration file first.")
        return None
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("✅ Cloud configuration loaded successfully")
        return config
    except Exception as e:
        print(f"❌ Error loading cloud configuration: {e}")
        return None

def create_storage_backend_from_config(config):
    """Create storage backend based on configuration."""
    storage_provider = config.get('storage_provider', 'local')
    
    print(f"\n🔧 Creating {storage_provider.upper()} storage backend...")
    
    if storage_provider == 'local':
        print("✅ Using local filesystem storage (default)")
        return None  # DataManager will use local storage by default
    
    elif storage_provider == 's3':
        aws_config = config.get('aws', {})
        
        # Check for environment variables first
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID') or aws_config.get('access_key_id')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY') or aws_config.get('secret_access_key')
        aws_region = os.environ.get('AWS_DEFAULT_REGION') or aws_config.get('region', 'us-east-1')
        
        if not aws_access_key or not aws_secret_key:
            print("❌ AWS credentials not found in environment or config")
            print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            return None
        
        bucket_name = aws_config.get('bucket_name')
        if not bucket_name or bucket_name == 'your-s3-bucket-name':
            print("❌ Please configure a valid S3 bucket name in config/cloud_settings.yaml")
            return None
        
        try:
            backend = create_storage_backend(
                storage_type="s3",
                bucket_name=bucket_name,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
            print(f"✅ S3 storage backend created for bucket: {bucket_name}")
            return backend
        except Exception as e:
            print(f"❌ Error creating S3 backend: {e}")
            return None
    
    elif storage_provider == 'gcs':
        gcs_config = config.get('gcs', {})
        
        # Check for environment variable first
        credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or gcs_config.get('credentials_file')
        
        if not credentials_file:
            print("❌ Google Cloud credentials not found")
            print("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable or configure credentials_file in config")
            return None
        
        bucket_name = gcs_config.get('bucket_name')
        if not bucket_name or bucket_name == 'your-gcs-bucket-name':
            print("❌ Please configure a valid GCS bucket name in config/cloud_settings.yaml")
            return None
        
        try:
            backend = create_storage_backend(
                storage_type="gcs",
                bucket_name=bucket_name
            )
            print(f"✅ GCS storage backend created for bucket: {bucket_name}")
            return backend
        except Exception as e:
            print(f"❌ Error creating GCS backend: {e}")
            return None
    
    elif storage_provider == 'azure':
        azure_config = config.get('azure', {})
        
        # Check for environment variable first
        connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING') or azure_config.get('connection_string')
        
        if not connection_string:
            print("❌ Azure connection string not found")
            print("Please set AZURE_STORAGE_CONNECTION_STRING environment variable or configure connection_string in config")
            return None
        
        account_name = azure_config.get('account_name')
        container_name = azure_config.get('container_name')
        
        if not account_name or not container_name:
            print("❌ Please configure account_name and container_name in config/cloud_settings.yaml")
            return None
        
        try:
            backend = create_storage_backend(
                storage_type="azure",
                account_name=account_name,
                container_name=container_name,
                connection_string=connection_string
            )
            print(f"✅ Azure storage backend created for container: {container_name}")
            return backend
        except Exception as e:
            print(f"❌ Error creating Azure backend: {e}")
            return None
    
    else:
        print(f"❌ Unsupported storage provider: {storage_provider}")
        return None

def test_data_manager_with_cloud_storage():
    """Test DataManager with cloud storage backend."""
    print("\n" + "="*60)
    print("CLOUD STORAGE CONFIGURATION EXAMPLE")
    print("="*60)
    
    # Load cloud configuration
    config = load_cloud_config()
    if not config:
        return
    
    # Create storage backend
    storage_backend = create_storage_backend_from_config(config)
    
    # Create DataManager with cloud storage
    try:
        data_manager = DataManager(
            base_dir="cloud-storage-test",
            test_mode=True,  # Use test mode for this example
            storage_backend=storage_backend
        )
        
        print(f"\n✅ DataManager created successfully")
        print(f"   Storage type: {config.get('storage_provider', 'local')}")
        print(f"   Base directory: cloud-storage-test")
        print(f"   Test mode: True")
        
        # Test basic operations
        print("\n🧪 Testing basic operations...")
        
        # Test directory creation using storage backend
        test_dir = "test-directory"
        data_manager.storage.mkdir(test_dir, parents=True, exist_ok=True)
        print(f"   ✅ Created directory: {test_dir}")
        
        # Test file operations
        test_file = f"{test_dir}/test-data.json"
        test_data = {"message": "Hello from cloud storage!", "timestamp": "2025-08-01"}
        
        data_manager.save_json(test_data, test_file)
        print(f"   ✅ Saved test data to: {test_file}")
        
        loaded_data = data_manager.load_json(test_file)
        print(f"   ✅ Loaded test data: {loaded_data}")
        
        # Test file existence
        exists = data_manager.storage.exists(test_file)
        print(f"   ✅ File exists check: {exists}")
        
        # Cleanup
        data_manager.storage.delete_file(test_file)
        print(f"   ✅ Deleted test file: {test_file}")
        
        # Clean up test directory
        data_manager.storage.delete_directory(test_dir)
        print(f"   ✅ Deleted test directory: {test_dir}")
        
        print("\n🎉 Cloud storage test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error testing DataManager: {e}")

def show_configuration_help():
    """Show help for configuring cloud storage."""
    print("\n" + "="*60)
    print("CLOUD STORAGE CONFIGURATION HELP")
    print("="*60)
    
    print("\n📋 To configure cloud storage:")
    print("1. Edit config/cloud_settings.yaml")
    print("2. Set storage_provider to 's3', 'gcs', or 'azure'")
    print("3. Configure the appropriate section (aws, gcs, or azure)")
    print("4. Set environment variables for credentials")
    
    print("\n🔑 Required Environment Variables:")
    print("AWS S3:")
    print("  export AWS_ACCESS_KEY_ID='your-access-key'")
    print("  export AWS_SECRET_ACCESS_KEY='your-secret-key'")
    print("  export AWS_DEFAULT_REGION='us-east-1'")
    
    print("\nGoogle Cloud Storage:")
    print("  export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account-key.json'")
    
    print("\nAzure Blob Storage:")
    print("  export AZURE_STORAGE_CONNECTION_STRING='your-connection-string'")
    
    print("\n📖 For detailed documentation, see docs/CLOUD_STORAGE.md")

if __name__ == "__main__":
    # Show configuration help
    show_configuration_help()
    
    # Test cloud storage configuration
    test_data_manager_with_cloud_storage() 