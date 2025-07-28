#!/usr/bin/env python3
"""
Stock OHLCV Data Fetching Script for MVP Pipeline

This script reads the latest ticker list and fetches OHLCV data for each ticker,
storing it in partitioned folders with comprehensive logging and error handling.

Author: Stock Evaluation Pipeline Team
Date: 2025-07-18
"""

import os
import json
import time
import logging
import argparse
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd
import yaml
import sys
from pathlib import Path

# Add utils directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from progress import get_progress_tracker
import concurrent.futures

# Try to import yfinance, fallback to alpha_vantage if not available
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("Warning: yfinance not available, will use Alpha Vantage fallback")

# Try to import alpha_vantage
try:
    from alpha_vantage.timeseries import TimeSeries
    ALPHA_VANTAGE_AVAILABLE = True
except ImportError:
    ALPHA_VANTAGE_AVAILABLE = False
    print("Warning: alpha_vantage not available")


class OHLCVFetcher:
    """Handles fetching and processing of OHLCV stock data."""
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Initialize the OHLCVFetcher with configuration.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.setup_logging()
        # Determine mode from environment variable or config
        self.mode = os.environ.get('PIPELINE_MODE', None)
        if self.mode is None:
            if self.config.get('test_mode', False):
                self.mode = 'test'
            else:
                self.mode = 'prod'
        logging.info(f"[PIPELINE MODE] Running in {self.mode.upper()} mode.")
        
    def _load_config(self, config_path: str) -> Dict:
        """
        Load configuration from YAML file with fallback defaults.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Dictionary containing configuration settings
        """
        default_config = {
            "base_data_path": "data/",
            "base_log_path": "logs/",
            "ohlcv_data_path": "raw",
            "ohlcv_log_path": "fetch",
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
            "progress": True,  # Enable progress by default
            "parallel_workers": None,
            "adaptive_reduce_every": 3
        }
        
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                # Merge with defaults for missing keys
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                return config
        except FileNotFoundError:
            logging.warning(f"Config file {config_path} not found, using defaults")
            return default_config
        except yaml.YAMLError as e:
            logging.error(f"Error parsing config file: {e}")
            return default_config
    
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def get_latest_ticker_file(self, test_mode: bool = False) -> Optional[Path]:
        """
        Find the most recent ticker file.
        
        Args:
            test_mode: If True, look in test directories
            
        Returns:
            Path to the latest ticker file, or None if not found
        """
        if test_mode:
            ticker_path = Path("data/test/tickers")
        else:
            ticker_path = Path(self.config["base_data_path"]) / self.config.get("ticker_data_path", "tickers")
        
        if not ticker_path.exists():
            logging.error(f"Ticker directory not found: {ticker_path}")
            return None
        
        # Find all dt=* directories and get the latest one
        partitions = [d for d in ticker_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
        if not partitions:
            logging.error(f"No ticker partitions found in {ticker_path}")
            return None
        
        latest_partition = max(partitions, key=lambda x: x.name)
        ticker_file = latest_partition / "tickers.csv"
        
        if ticker_file.exists():
            logging.info(f"Found latest ticker file: {ticker_file}")
            return ticker_file
        else:
            logging.error(f"Ticker file not found: {ticker_file}")
            return None
    
    def load_tickers(self, ticker_file: Path) -> List[str]:
        """
        Load ticker symbols from CSV file.
        
        Args:
            ticker_file: Path to the ticker CSV file
            
        Returns:
            List of ticker symbols
        """
        try:
            df = pd.read_csv(ticker_file)
            tickers = df['symbol'].tolist()
            logging.info(f"Loaded {len(tickers)} tickers from {ticker_file}")
            return tickers
        except Exception as e:
            logging.error(f"Failed to load tickers from {ticker_file}: {e}")
            return []
    
    def fetch_ohlcv_yfinance(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data using yfinance.
        
        Args:
            ticker: Ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        try:
            ticker_obj = yf.Ticker(ticker)
            data = ticker_obj.history(period=f"{days}d")
            
            if data.empty:
                logging.warning(f"No data returned for {ticker}")
                return None
            
            # Reset index to make date a column
            data = data.reset_index()
            
            # Rename columns to match expected format
            data.columns = [col.lower() for col in data.columns]
            
            # Add ticker column
            data['ticker'] = ticker
            
            logging.info(f"Successfully fetched {len(data)} days of data for {ticker} via yfinance")
            return data
            
        except Exception as e:
            logging.error(f"Failed to fetch data for {ticker} via yfinance: {e}")
            return None
    
    def fetch_ohlcv_alpha_vantage(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data using Alpha Vantage (fallback).
        
        Args:
            ticker: Ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        if not ALPHA_VANTAGE_AVAILABLE:
            logging.error("Alpha Vantage not available")
            return None
        
        api_key = self.config.get("alpha_vantage_api_key")
        if not api_key:
            logging.error("Alpha Vantage API key not configured")
            return None
        
        try:
            ts = TimeSeries(key=api_key, output_format='pandas')
            result = ts.get_daily(symbol=ticker, outputsize='compact')
            
            # Handle different return formats from Alpha Vantage
            if isinstance(result, tuple) and len(result) == 2:
                data, meta_data = result
            else:
                data = result
            
            if data is None or data.empty:
                logging.warning(f"No data returned for {ticker}")
                return None
            
            # Reset index to make date a column
            data = data.reset_index()
            
            # Rename columns to match expected format
            data.columns = [col.lower() for col in data.columns]
            
            # Add ticker column
            data['ticker'] = ticker
            
            # Limit to requested number of days
            data = data.head(days)
            
            logging.info(f"Successfully fetched {len(data)} days of data for {ticker} via Alpha Vantage")
            return data
            
        except Exception as e:
            logging.error(f"Failed to fetch data for {ticker} via Alpha Vantage: {e}")
            return None
    
    def fetch_ohlcv_data(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data using available data sources.
        
        Args:
            ticker: Ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        # Try yfinance first
        if YFINANCE_AVAILABLE:
            data = self.fetch_ohlcv_yfinance(ticker, days)
            if data is not None:
                return data
        
        # Fallback to Alpha Vantage
        if ALPHA_VANTAGE_AVAILABLE:
            data = self.fetch_ohlcv_alpha_vantage(ticker, days)
            if data is not None:
                return data
        
        logging.error(f"Failed to fetch data for {ticker} from all available sources")
        return None
    
    def create_partition_paths(self, date_str: str, test_mode: bool = False) -> Tuple[Path, Path]:
        """
        Create partition paths for data and logs.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            test_mode: If True, use test directories instead of production
            
        Returns:
            Tuple of (data_path, log_path)
        """
        if test_mode:
            # Use test directories for test mode
            data_path = Path("data/test/raw") / f"dt={date_str}"
            log_path = Path("logs/test/fetch") / f"dt={date_str}"
        else:
            # Use production directories
            data_path = Path(self.config["base_data_path"]) / self.config["ohlcv_data_path"] / f"dt={date_str}"
            log_path = Path(self.config["base_log_path"]) / self.config["ohlcv_log_path"] / f"dt={date_str}"
        
        # Ensure directories exist
        data_path.mkdir(parents=True, exist_ok=True)
        log_path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Created partition paths: {data_path}, {log_path}")
        return data_path, log_path
    
    def save_ticker_data(self, ticker: str, data: pd.DataFrame, data_path: Path, dry_run: bool = False) -> bool:
        """
        Save ticker data to CSV file.
        
        Args:
            ticker: Ticker symbol
            data: DataFrame with OHLCV data
            data_path: Path to save the data
            dry_run: If True, don't actually save files
            
        Returns:
            True if saved successfully, False otherwise
        """
        csv_path = data_path / f"{ticker}.csv"
        
        if dry_run:
            logging.info(f"[DRY RUN] Would save {len(data)} rows for {ticker} to {csv_path}")
            return True
        
        try:
            data.to_csv(csv_path, index=False)
            logging.info(f"Saved {len(data)} rows for {ticker} to {csv_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to save data for {ticker}: {e}")
            return False
    
    def save_metadata(self, metadata: Dict, log_path: Path, dry_run: bool = False) -> str:
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
            json.dump(metadata, f, indent=2)
        
        logging.info(f"Saved metadata to {metadata_path}")
        return str(metadata_path)
    
    def save_errors(self, errors: List[Dict], log_path: Path, dry_run: bool = False) -> str:
        """
        Save error log to JSON file.
        
        Args:
            errors: List of error dictionaries
            log_path: Path to save the error log
            dry_run: If True, don't actually save files
            
        Returns:
            Path to the saved error log file
        """
        errors_path = log_path / "errors.json"
        
        if dry_run:
            logging.info(f"[DRY RUN] Would save error log to {errors_path}")
            return str(errors_path)
        
        with open(errors_path, 'w') as f:
            json.dump(errors, f, indent=2)
        
        logging.info(f"Saved error log to {errors_path}")
        return str(errors_path)
    
    def check_existing_partition(self, date_str: str, test_mode: bool = False) -> bool:
        """
        Check if today's partition already exists.
    
        Args:
            date_str: Date string in YYYY-MM-DD format
            test_mode: If True, check test directories
    
        Returns:
            True if partition exists, False otherwise
        """
        data_path, _ = self.create_partition_paths(date_str, test_mode)
        
        if data_path.exists():
            # Check if there are any CSV files in the partition
            csv_files = list(data_path.glob("*.csv"))
            return len(csv_files) > 0
        return False
    
    def cleanup_old_partitions(self, dry_run: bool = False, test_mode: bool = False) -> Dict:
        """
        Clean up old OHLCV partitions based on retention policy.
        
        Args:
            dry_run: If True, don't actually delete files
            test_mode: If True, clean test directories
            
        Returns:
            Dictionary containing cleanup results
        """
        retention_days = self.config.get("retention_days", 3)
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        if test_mode:
            base_data_path = Path("data/test/raw")
            base_log_path = Path("logs/test/fetch")
        else:
            base_data_path = Path(self.config["base_data_path"]) / self.config["ohlcv_data_path"]
            base_log_path = Path(self.config["base_log_path"]) / self.config["ohlcv_log_path"]
        
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
            "test_mode": test_mode
        }
        
        if test_mode:
            cleanup_log_path = Path("logs/test/cleanup")
        else:
            cleanup_log_path = Path(self.config["base_log_path"]) / self.config["cleanup_log_path"]
        
        cleanup_log_path.mkdir(parents=True, exist_ok=True)
        cleanup_file = cleanup_log_path / f"cleanup_{datetime.now().strftime('%Y-%m-%d')}.json"
        
        if not dry_run:
            with open(cleanup_file, 'w') as f:
                json.dump(cleanup_log, f, indent=2)
            logging.info(f"Saved cleanup log to {cleanup_file}")
        
        return cleanup_log
    
    def handle_rate_limit(self, attempt: int) -> None:
        """
        Handle rate limiting with exponential backoff.
        
        Args:
            attempt: Current attempt number
        """
        max_hits = self.config.get("max_rate_limit_hits", 10)
        base_cooldown = self.config.get("base_cooldown_seconds", 1)
        max_cooldown = self.config.get("max_cooldown_seconds", 60)
        
        if attempt >= max_hits:
            cooldown = max_cooldown
        else:
            cooldown = min(base_cooldown * (2 ** attempt), max_cooldown)
        
        debug = self.config.get("debug_rate_limit", False)
        if debug:
            print(f"[DEBUG-RATE-LIMIT] Simulating rate limit hit. Sleeping for {cooldown} seconds (attempt {attempt})")
        logging.info(f"Rate limit cooldown: {cooldown} seconds (attempt {attempt})")
        time.sleep(cooldown)
    
    def run(self, force: bool = False, test: bool = False, dry_run: bool = False, full_test: bool = False) -> Dict:
        """
        Main execution method for fetching OHLCV data.
        
        Args:
            force: If True, re-fetch even if partition exists
            test: If True, run in test mode (limited tickers, test directories)
            dry_run: If True, simulate operations without writing files
            full_test: If True, validate entire ticker universe
            
        Returns:
            Dictionary containing execution results and metadata
        """
        start_time = time.time()
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        metadata = {
            "run_date": date_str,
            "source_primary": "yfinance",
            "source_secondary": "alpha_vantage",
            "tickers_processed": 0,
            "tickers_successful": 0,
            "tickers_failed": 0,
            "status": "failed",
            "runtime_seconds": 0,
            "runtime_minutes": 0,
            "api_retries": 0,
            "rate_limit_hits": 0,
            "rate_limit_strategy": self.config.get("rate_limit_strategy", "exponential_backoff"),
            "error_message": None,
            "test_mode": test,
            "full_test_mode": full_test,
            "dry_run_mode": dry_run,
            "total_sleep_time": 0,
            "parallel_workers": self.config.get("parallel_workers"),
            "batch_size": self.config.get("batch_size", 10)
        }
        
        data_path, log_path = None, None
        errors = []
        
        try:
            # Perform cleanup if enabled
            if self.config.get("cleanup_enabled", True):
                logging.info("Performing retention cleanup...")
                cleanup_results = self.cleanup_old_partitions(dry_run, test)
                logging.info(f"Cleanup completed: {cleanup_results['total_deleted']} partitions deleted")
            
            # Check if partition already exists
            if not force and self.check_existing_partition(date_str, test):
                logging.info("Partition already exists and force=False, skipping fetch")
                metadata["status"] = "skipped"
                metadata["error_message"] = "Partition already exists"
                return metadata
            
            # Get latest ticker file
            ticker_file = self.get_latest_ticker_file(test)
            if not ticker_file:
                raise Exception("No ticker file found")
            
            # Load tickers
            all_tickers = self.load_tickers(ticker_file)
            if not all_tickers:
                raise Exception("No tickers loaded from file")
            
            # Apply test mode limitations
            if test and not full_test:
                # Limit to 5 tickers for test mode
                tickers_to_process = all_tickers[:5]
                logging.info(f"[TEST MODE] Fetching only 5 tickers: {tickers_to_process}")
            else:
                # Process all tickers
                tickers_to_process = all_tickers
                logging.info(f"Fetching data for all {len(tickers_to_process)} tickers")
            
            # Create partition paths
            data_path, log_path = self.create_partition_paths(date_str, test)
            
            # Configure parallel processing
            parallel_workers = self.config.get("parallel_workers")
            if parallel_workers is None:
                parallel_workers = min(8, len(tickers_to_process))  # Default to 8 or number of tickers
            
            # Use progress tracking for ticker processing
            show_progress = self.config.get("progress", True)
            with get_progress_tracker(
                total=len(tickers_to_process), 
                desc="Fetching OHLCV data", 
                unit="ticker",
                disable=not show_progress
            ) as progress:
                
                successful_tickers = []
                failed_tickers = []
                total_sleep_time = 0
                
                if parallel_workers > 1:
                    # Parallel processing
                    logging.info(f"[INFO] Using {parallel_workers} parallel workers for data fetching.")
                    
                    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                        # Submit all tasks
                        future_to_ticker = {
                            executor.submit(self._fetch_save_ticker, ticker, data_path, self.config.get("retention_days", 3), dry_run): ticker
                            for ticker in tickers_to_process
                        }
                        
                        # Process completed tasks
                        for future in concurrent.futures.as_completed(future_to_ticker):
                            ticker = future_to_ticker[future]
                            try:
                                result = future.result()
                                if result:
                                    successful_tickers.append(ticker)
                                else:
                                    failed_tickers.append(ticker)
                                    errors.append({"ticker": ticker, "error": "Fetch failed"})
                            except Exception as e:
                                failed_tickers.append(ticker)
                                errors.append({"ticker": ticker, "error": str(e)})
                            
                            progress.update(1, postfix={"current": ticker})
                            
                            # Sleep between tickers (rate limiting)
                            if not test:  # No cooldown for test mode
                                cooldown = self.config.get("base_cooldown_seconds", 1)
                                time.sleep(cooldown)
                                total_sleep_time += cooldown
                else:
                    # Sequential processing
                    for ticker in tickers_to_process:
                        try:
                            result = self._fetch_save_ticker(ticker, data_path, self.config.get("retention_days", 3), dry_run)
                            if result:
                                successful_tickers.append(ticker)
                            else:
                                failed_tickers.append(ticker)
                                errors.append({"ticker": ticker, "error": "Fetch failed"})
                        except Exception as e:
                            failed_tickers.append(ticker)
                            errors.append({"ticker": ticker, "error": str(e)})
                        
                        progress.update(1, postfix={"current": ticker})
                        
                        # Sleep between tickers (rate limiting)
                        if not test:  # No cooldown for test mode
                            cooldown = self.config.get("base_cooldown_seconds", 1)
                            time.sleep(cooldown)
                            total_sleep_time += cooldown
            
            # Update metadata
            runtime = time.time() - start_time
            metadata.update({
                "tickers_processed": len(tickers_to_process),
                "tickers_successful": len(successful_tickers),
                "tickers_failed": len(failed_tickers),
                "skipped_tickers": 0,  # No tickers are skipped in this implementation
                "status": "success",
                "runtime_seconds": round(runtime, 2),
                "runtime_minutes": round(runtime / 60, 2),
                "total_sleep_time": total_sleep_time,
                "data_path": str(data_path),
                "metadata_path": str(log_path / "metadata.json"),
                "errors_path": str(log_path / "errors.json") if errors else None
            })
            
            # Save metadata
            self.save_metadata(metadata, log_path, dry_run)
            
            # Save errors if any
            if errors:
                self.save_errors(errors, log_path, dry_run)
            
            return metadata
            
        except Exception as e:
            runtime = time.time() - start_time
            metadata.update({
                "status": "failed",
                "error_message": str(e),
                "runtime_seconds": round(runtime, 2),
                "runtime_minutes": round(runtime / 60, 2)
            })
            
            if log_path:
                self.save_metadata(metadata, log_path, dry_run)
                if errors:
                    self.save_errors(errors, log_path, dry_run)
            
            logging.error(f"OHLCV fetch failed: {e}")
            raise
    
    def _fetch_save_ticker(self, ticker, data_path, retention_days, dry_run):
        """Helper for parallel fetch and save"""
        try:
            data = self.fetch_ohlcv_data(ticker, retention_days)
            if data is not None:
                return self.save_ticker_data(ticker, data, data_path, dry_run)
            return False
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Fetch OHLCV data for stock tickers")
    parser.add_argument(
        "--force", 
        action="store_true", 
        help="Force re-fetch even if partition already exists"
    )
    parser.add_argument(
        "--test", 
        action="store_true", 
        help="Test mode: fetch limited tickers and use test directories"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Simulate operations without writing files"
    )
    parser.add_argument(
        "--full-test", 
        action="store_true", 
        help="Full test mode: validate entire ticker universe"
    )
    parser.add_argument(
        "--cooldown", 
        type=float, 
        default=None,
        help="Cooldown (seconds) between API calls (overrides config)"
    )
    parser.add_argument(
        "--parallel", 
        type=int, 
        default=None,
        help="Number of parallel workers (overrides config)"
    )
    parser.add_argument(
        "--progress", 
        action="store_true", 
        help="Show progress bar (enabled by default for full runs)"
    )
    parser.add_argument(
        "--no-progress", 
        action="store_true", 
        help="Disable progress bar"
    )
    parser.add_argument(
        "--config", 
        default="config/settings.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    fetcher = OHLCVFetcher(args.config)
    
    # CLI overrides
    if args.cooldown is not None:
        fetcher.config['base_cooldown_seconds'] = args.cooldown
    if args.parallel is not None:
        fetcher.config['parallel_workers'] = args.parallel
    
    # Progress configuration
    if args.no_progress:
        fetcher.config['progress'] = False
    elif args.progress:
        fetcher.config['progress'] = True
    else:
        # Default: enable progress for full runs, disable for test mode
        fetcher.config['progress'] = not args.test
    
    # Test mode configuration
    if args.test and not args.full_test:
        fetcher.config['test_mode'] = True
        fetcher.config['base_cooldown_seconds'] = 0  # No cooldown for test mode
        print("[TEST MODE] Only 5 tickers will be fetched. Cooldown is set to 0.")
    else:
        fetcher.config['test_mode'] = False
    
    result = fetcher.run(force=args.force, test=args.test, dry_run=args.dry_run, full_test=args.full_test)
    
    # Print summary
    print(f"\n=== OHLCV Data Fetch Summary ===")
    print(f"Status: {result['status']}")
    print(f"Tickers Processed: {result.get('tickers_processed', 0)}")
    print(f"Tickers Successful: {result.get('tickers_successful', 0)}")
    print(f"Tickers Failed: {result.get('tickers_failed', 0)}")
    print(f"Runtime: {result.get('runtime_seconds', 0)} seconds ({result.get('runtime_minutes', 0):.2f} minutes)")
    if result.get('api_retries', 0) > 0:
        print(f"API Retries: {result['api_retries']}")
    if result.get('rate_limit_hits', 0) > 0:
        print(f"Rate Limit Hits: {result['rate_limit_hits']}")
    if result.get('total_sleep_time', 0) > 0:
        print(f"Total Sleep Time: {result['total_sleep_time']} seconds")
    if result.get('parallel_workers', 0) > 0:
        print(f"Parallel Workers: {result['parallel_workers']}")
    if result.get('status') == 'success':
        print(f"Data saved to: {result.get('data_path','')}")
        print(f"Metadata saved to: {result.get('metadata_path','')}")
        if result.get('errors_path'):
            print(f"Error log saved to: {result['errors_path']}")
    elif result.get('status') == 'failed':
        print(f"Error: {result.get('error_message','')}")
    
    # Always print summary log
    print("--- SUMMARY LOG ---")
    print(f"Tickers processed: {result.get('tickers_processed', 0)}")
    print(f"Total runtime: {result.get('runtime_seconds', 0)} seconds")
    print(f"Total sleep time: {result.get('total_sleep_time', 0)} seconds")
    print(f"Parallel Workers: {result.get('parallel_workers', 0)} (final: {result.get('parallel_workers', 0)})")
    print(f"Adaptive Reductions: [{result.get('parallel_workers', 0)}]")
    print(f"Total Threads Executed: {result.get('tickers_processed', 0)}")
    print(f"Errors: {result.get('errors_path', '')}")
    
    sys.exit(0 if result.get('status') in ['success', 'skipped'] else 1)


if __name__ == "__main__":
    main() 