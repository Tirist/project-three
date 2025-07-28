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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

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

class DataManager:
    """Manages data directory structure and file operations."""
    
    def __init__(self, base_dir: str = "data", test_mode: bool = False):
        self.base_dir = Path(base_dir)
        self.test_mode = test_mode
        
        # Set up directory paths
        if test_mode:
            self.data_dir = self.base_dir / "test"
        else:
            self.data_dir = self.base_dir
        
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.tickers_dir = self.data_dir / "tickers"
        
        # Create directories if they don't exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories."""
        for directory in [self.raw_dir, self.processed_dir, self.tickers_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_partition_path(self, date: Union[str, datetime], data_type: str) -> Path:
        """Get partition path for a specific date and data type."""
        if isinstance(date, str):
            date_str = date
        else:
            date_str = date.strftime("%Y-%m-%d")
        
        partition_name = f"dt={date_str}"
        
        if data_type == "raw":
            return self.raw_dir / partition_name
        elif data_type == "processed":
            return self.processed_dir / partition_name
        elif data_type == "tickers":
            return self.tickers_dir / partition_name
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    def partition_exists(self, date: Union[str, datetime], data_type: str) -> bool:
        """Check if a partition exists."""
        partition_path = self.get_partition_path(date, data_type)
        return partition_path.exists()
    
    def list_partitions(self, data_type: str) -> List[str]:
        """List all partitions for a data type."""
        base_path = self.get_partition_path("", data_type).parent
        if not base_path.exists():
            return []
        
        partitions = []
        for item in base_path.iterdir():
            if item.is_dir() and item.name.startswith("dt="):
                partitions.append(item.name[3:])  # Remove "dt=" prefix
        
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
                    import shutil
                    shutil.rmtree(partition_path)
                    deleted_count += 1
            except ValueError:
                continue
        
        return deleted_count

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

def format_time(seconds: float) -> str:
    """Format time in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def format_progress(current: int, total: int, elapsed: float) -> str:
    """Format progress information."""
    if total == 0:
        return "0/0 (0%)"
    
    percentage = (current / total) * 100
    if current > 0:
        eta = (elapsed / current) * (total - current)
        eta_str = f" ETA: {format_time(eta)}"
    else:
        eta_str = ""
    
    return f"{current}/{total} ({percentage:.1f}%){eta_str}"

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