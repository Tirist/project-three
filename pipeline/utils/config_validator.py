#!/usr/bin/env python3
"""
config_validator.py

Configuration validation utilities for the stock evaluation pipeline.
Validates required environment variables and configuration settings.
"""

import os
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigValidator:
    """Validates configuration and environment variables."""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_api_keys(self, config: Dict) -> bool:
        """Validate that required API keys are present."""
        is_valid = True
        
        # Check Alpha Vantage API key
        api_key = config.get('alpha_vantage_api_key')
        if not api_key:
            self.errors.append("ALPHA_VANTAGE_API_KEY environment variable is required")
            is_valid = False
        elif api_key == "your_alpha_vantage_api_key_here":
            self.errors.append("ALPHA_VANTAGE_API_KEY is set to placeholder value")
            is_valid = False
        elif len(api_key) < 10:
            self.warnings.append("ALPHA_VANTAGE_API_KEY appears to be too short")
        
        return is_valid
    
    def validate_cloud_storage(self, config: Dict) -> bool:
        """Validate cloud storage configuration if enabled."""
        is_valid = True
        
        # Check if any cloud storage is configured
        has_aws = any(key in config for key in ['aws_access_key_id', 'aws_secret_access_key'])
        has_gcs = 'google_application_credentials' in config
        has_azure = 'azure_storage_connection_string' in config
        
        if has_aws:
            if not config.get('aws_access_key_id'):
                self.errors.append("AWS_ACCESS_KEY_ID is required when using AWS S3")
                is_valid = False
            if not config.get('aws_secret_access_key'):
                self.errors.append("AWS_SECRET_ACCESS_KEY is required when using AWS S3")
                is_valid = False
        
        if has_gcs:
            creds_path = config.get('google_application_credentials')
            if creds_path and not Path(creds_path).exists():
                self.errors.append(f"Google Cloud credentials file not found: {creds_path}")
                is_valid = False
        
        return is_valid
    
    def validate_paths(self, config: Dict) -> bool:
        """Validate that required paths exist and are writable."""
        is_valid = True
        
        # Check base data path
        base_data_path = config.get('base_data_path', 'data/')
        if not os.path.exists(base_data_path):
            try:
                os.makedirs(base_data_path, exist_ok=True)
                logger.info(f"Created base data path: {base_data_path}")
            except Exception as e:
                self.errors.append(f"Cannot create base data path {base_data_path}: {e}")
                is_valid = False
        
        # Check base log path
        base_log_path = config.get('base_log_path', 'logs/')
        if not os.path.exists(base_log_path):
            try:
                os.makedirs(base_log_path, exist_ok=True)
                logger.info(f"Created base log path: {base_log_path}")
            except Exception as e:
                self.errors.append(f"Cannot create base log path {base_log_path}: {e}")
                is_valid = False
        
        return is_valid
    
    def validate_performance_settings(self, config: Dict) -> bool:
        """Validate performance-related configuration."""
        is_valid = True
        
        # Check batch size
        batch_size = config.get('batch_size', 10)
        if batch_size <= 0:
            self.errors.append("batch_size must be greater than 0")
            is_valid = False
        elif batch_size > 1000:
            self.warnings.append("Large batch_size may cause memory issues")
        
        # Check retry settings
        retry_attempts = config.get('api_retry_attempts', 3)
        if retry_attempts < 0:
            self.errors.append("api_retry_attempts must be non-negative")
            is_valid = False
        
        retry_delay = config.get('api_retry_delay', 1)
        if retry_delay < 0:
            self.errors.append("api_retry_delay must be non-negative")
            is_valid = False
        
        return is_valid
    
    def validate_all(self, config: Dict) -> Tuple[bool, List[str], List[str]]:
        """Validate all configuration aspects."""
        self.errors = []
        self.warnings = []
        
        # Run all validations
        validations = [
            self.validate_api_keys(config),
            self.validate_cloud_storage(config),
            self.validate_paths(config),
            self.validate_performance_settings(config)
        ]
        
        is_valid = all(validations)
        
        return is_valid, self.errors, self.warnings
    
    def print_validation_report(self, is_valid: bool, errors: List[str], warnings: List[str]):
        """Print a formatted validation report."""
        print("\n" + "="*60)
        print("CONFIGURATION VALIDATION REPORT")
        print("="*60)
        
        if is_valid:
            print("✅ Configuration is valid!")
        else:
            print("❌ Configuration has errors:")
            for error in errors:
                print(f"   • {error}")
        
        if warnings:
            print("\n⚠️  Warnings:")
            for warning in warnings:
                print(f"   • {warning}")
        
        if not is_valid:
            print("\n🔧 To fix these issues:")
            print("   1. Copy .env.example to .env")
            print("   2. Fill in your actual API keys and credentials")
            print("   3. Ensure all required paths are writable")
            print("   4. Run validation again")
        
        print("="*60 + "\n")

def validate_config(config: Dict) -> bool:
    """
    Convenience function to validate configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if configuration is valid, False otherwise
    """
    validator = ConfigValidator()
    is_valid, errors, warnings = validator.validate_all(config)
    validator.print_validation_report(is_valid, errors, warnings)
    return is_valid

def check_environment_setup() -> bool:
    """
    Check if the environment is properly set up.
    
    Returns:
        True if environment is ready, False otherwise
    """
    print("\n" + "="*60)
    print("ENVIRONMENT SETUP CHECK")
    print("="*60)
    
    # Load environment variables from .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv not available
    
    checks = []
    
    # Check for .env file
    env_file = Path('.env')
    if env_file.exists():
        print("✅ .env file exists")
        checks.append(True)
    else:
        print("❌ .env file not found")
        print("   Create .env file from .env.example")
        checks.append(False)
    
    # Check for required environment variables
    required_vars = ['ALPHA_VANTAGE_API_KEY']
    for var in required_vars:
        if os.environ.get(var):
            if var == 'ALPHA_VANTAGE_API_KEY' and os.environ[var] == 'your_alpha_vantage_api_key_here':
                print(f"❌ {var} is set to placeholder value")
                checks.append(False)
            else:
                print(f"✅ {var} is set")
                checks.append(True)
        else:
            print(f"❌ {var} is not set")
            checks.append(False)
    
    # Check optional environment variables
    optional_vars = [
        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION',
        'GOOGLE_APPLICATION_CREDENTIALS', 'AZURE_STORAGE_CONNECTION_STRING'
    ]
    
    print("\nOptional environment variables:")
    for var in optional_vars:
        if os.environ.get(var):
            print(f"✅ {var} is set")
        else:
            print(f"   {var} is not set (optional)")
    
    is_ready = all(checks)
    
    if is_ready:
        print("\n✅ Environment is properly configured!")
    else:
        print("\n❌ Environment needs configuration:")
        print("   1. Copy .env.example to .env")
        print("   2. Add your Alpha Vantage API key to .env")
        print("   3. Add any other required credentials")
        print("   4. Run this check again")
    
    print("="*60 + "\n")
    return is_ready

if __name__ == "__main__":
    # Example usage
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    from pipeline.utils.common import load_config
    
    # Load configuration
    config = load_config("config/settings.yaml", "ohlcv")
    
    # Validate configuration
    is_valid = validate_config(config)
    
    # Check environment setup
    env_ready = check_environment_setup()
    
    if is_valid and env_ready:
        print("🎉 Everything is ready to go!")
    else:
        print("⚠️  Please fix the issues above before running the pipeline") 