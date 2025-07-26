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
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None
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
            "progress": False,
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
                logging.StreamHandler(),
                logging.FileHandler('fetch_data.log')
            ]
        )
    
    def get_latest_ticker_file(self) -> Optional[Path]:
        """
        Find the most recent ticker CSV file.
        
        Returns:
            Path to the latest ticker file, or None if not found
        """
        ticker_base_path = Path(self.config["base_data_path"]) / "tickers"
        
        if not ticker_base_path.exists():
            logging.error(f"Ticker base path does not exist: {ticker_base_path}")
            return None
        
        # Find all dt=YYYY-MM-DD directories
        date_dirs = [d for d in ticker_base_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
        
        if not date_dirs:
            logging.error("No ticker data directories found")
            return None
        
        # Sort by date and get the latest
        latest_dir = sorted(date_dirs, reverse=True)[0]
        ticker_file = latest_dir / "tickers.csv"
        
        if not ticker_file.exists():
            logging.error(f"Ticker file not found: {ticker_file}")
            return None
        
        logging.info(f"Found latest ticker file: {ticker_file}")
        return ticker_file
    
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
            logging.error(f"Error loading tickers from {ticker_file}: {e}")
            return []
    
    def fetch_ohlcv_yfinance(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data using yfinance.
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        if not YFINANCE_AVAILABLE:
            return None
        
        try:
            # Get data for the specified number of days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days + 5)  # Add buffer for weekends
            
            ticker_obj = yf.Ticker(ticker)
            data = ticker_obj.history(start=start_date, end=end_date)
            
            if data.empty:
                logging.warning(f"No data returned for {ticker} from yfinance")
                return None
            
            # Select only the last N days
            data = data.tail(days)
            
            # Rename columns to match expected format
            data = data.rename(columns={
                'Open': 'Open',
                'High': 'High', 
                'Low': 'Low',
                'Close': 'Close',
                'Volume': 'Volume'
            })
            
            # Reset index to make Date a column
            data = data.reset_index()
            data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
            
            # Select only required columns
            required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            data = data[required_columns]
            
            logging.info(f"Successfully fetched {len(data)} days of data for {ticker} via yfinance")
            return data
            
        except Exception as e:
            logging.error(f"Error fetching data for {ticker} via yfinance: {e}")
            return None
    
    def fetch_ohlcv_alpha_vantage(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data using Alpha Vantage as fallback.
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        if not ALPHA_VANTAGE_AVAILABLE:
            return None
        
        api_key = self.config.get("alpha_vantage_api_key")
        if not api_key:
            logging.warning("Alpha Vantage API key not configured")
            return None
        
        try:
            ts = TimeSeries(key=api_key, output_format='pandas')
            
            # Alpha Vantage daily data
            data, meta_data = ts.get_daily(symbol=ticker, outputsize='compact')
            
            if data.empty:
                logging.warning(f"No data returned for {ticker} from Alpha Vantage")
                return None
            
            # Rename columns
            data = data.rename(columns={
                '1. open': 'Open',
                '2. high': 'High',
                '3. low': 'Low', 
                '4. close': 'Close',
                '5. volume': 'Volume'
            })
            
            # Reset index to make date a column
            data = data.reset_index()
            data['Date'] = data.index.strftime('%Y-%m-%d')
            
            # Select only the last N days
            data = data.tail(days)
            
            # Select only required columns
            required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            data = data[required_columns]
            
            logging.info(f"Successfully fetched {len(data)} days of data for {ticker} via Alpha Vantage")
            return data
            
        except Exception as e:
            logging.error(f"Error fetching data for {ticker} via Alpha Vantage: {e}")
            return None
    
    def fetch_ohlcv_data(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data with fallback between APIs.
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if all methods failed
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
        
        logging.error(f"All data sources failed for ticker {ticker}")
        return None
    
    def create_partition_paths(self, date_str: str) -> Tuple[Path, Path]:
        """
        Create partitioned folder paths for data and logs.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Tuple of (data_path, log_path) Path objects
        """
        # Create data path
        data_path = Path(self.config["base_data_path"]) / self.config["ohlcv_data_path"] / f"dt={date_str}"
        
        # Create log path
        log_path = Path(self.config["base_log_path"]) / self.config["ohlcv_log_path"] / f"dt={date_str}"
        
        # Ensure directories exist
        data_path.mkdir(parents=True, exist_ok=True)
        log_path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Created partition paths: {data_path}, {log_path}")
        return data_path, log_path
    
    def save_ticker_data(self, ticker: str, data: pd.DataFrame, data_path: Path, dry_run: bool = False) -> bool:
        """
        Save OHLCV data for a ticker to CSV file.
        
        Args:
            ticker: Stock ticker symbol
            data: DataFrame with OHLCV data
            data_path: Path to save the data
            dry_run: If True, don't actually save files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            csv_path = data_path / f"{ticker}.csv"
            
            if dry_run:
                logging.info(f"[DRY RUN] Would save {len(data)} rows to {csv_path}")
                return True
            
            data.to_csv(csv_path, index=False)
            logging.info(f"Saved {len(data)} rows for {ticker} to {csv_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error saving data for {ticker}: {e}")
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
        Save error logs to JSON file.
        
        Args:
            errors: List of error dictionaries
            log_path: Path to save the error file
            dry_run: If True, don't actually save files
            
        Returns:
            Path to the saved error file
        """
        if not errors:
            return ""
        
        error_path = log_path / "errors.json"
        
        if dry_run:
            logging.info(f"[DRY RUN] Would save {len(errors)} errors to {error_path}")
            return str(error_path)
        
        with open(error_path, 'w') as f:
            json.dump(errors, f, indent=2)
        
        logging.info(f"Saved {len(errors)} errors to {error_path}")
        return str(error_path)
    
    def check_existing_partition(self, date_str: str) -> bool:
        """
        Check if today's partition already exists.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            True if partition exists, False otherwise
        """
        data_path = Path(self.config["base_data_path"]) / self.config["ohlcv_data_path"] / f"dt={date_str}"
        
        # Check if directory exists and has files
        if data_path.exists() and any(data_path.iterdir()):
            logging.info(f"Partition for {date_str} already exists at {data_path}")
            return True
        
        return False
    
    def cleanup_old_partitions(self, dry_run: bool = False) -> Dict:
        """
        Clean up old partitions beyond retention_days.
        
        Args:
            dry_run: If True, don't actually delete files
            
        Returns:
            Dictionary with cleanup results
        """
        retention_days = self.config["retention_days"]
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        cleanup_results = {
            "cleanup_date": datetime.now().strftime("%Y-%m-%d"),
            "retention_days": retention_days,
            "cutoff_date": cutoff_date.strftime("%Y-%m-%d"),
            "partitions_deleted": [],
            "total_deleted": 0,
            "errors": []
        }
        
        # Clean up OHLCV data
        ohlcv_base_path = Path(self.config["base_data_path"]) / self.config["ohlcv_data_path"]
        if ohlcv_base_path.exists():
            for date_dir in ohlcv_base_path.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith('dt='):
                    try:
                        date_str = date_dir.name[3:]  # Remove 'dt=' prefix
                        dir_date = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        if dir_date < cutoff_date:
                            if dry_run:
                                logging.info(f"[DRY RUN] Would delete OHLCV partition: {date_dir}")
                            else:
                                shutil.rmtree(date_dir)
                                logging.info(f"Deleted OHLCV partition: {date_dir}")
                            
                            cleanup_results["partitions_deleted"].append(str(date_dir))
                            cleanup_results["total_deleted"] += 1
                    except Exception as e:
                        error_msg = f"Error cleaning up {date_dir}: {e}"
                        logging.error(error_msg)
                        cleanup_results["errors"].append(error_msg)
        
        # Clean up fetch logs
        fetch_log_base_path = Path(self.config["base_log_path"]) / self.config["ohlcv_log_path"]
        if fetch_log_base_path.exists():
            for date_dir in fetch_log_base_path.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith('dt='):
                    try:
                        date_str = date_dir.name[3:]  # Remove 'dt=' prefix
                        dir_date = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        if dir_date < cutoff_date:
                            if dry_run:
                                logging.info(f"[DRY RUN] Would delete fetch log partition: {date_dir}")
                            else:
                                shutil.rmtree(date_dir)
                                logging.info(f"Deleted fetch log partition: {date_dir}")
                    except Exception as e:
                        error_msg = f"Error cleaning up log {date_dir}: {e}"
                        logging.error(error_msg)
                        cleanup_results["errors"].append(error_msg)
        
        # Save cleanup log
        if self.config.get("cleanup_enabled", True):
            cleanup_log_path = Path(self.config["base_log_path"]) / self.config["cleanup_log_path"]
            cleanup_log_path.mkdir(parents=True, exist_ok=True)
            
            cleanup_file = cleanup_log_path / f"cleanup_{datetime.now().strftime('%Y-%m-%d')}.json"
            
            if not dry_run:
                with open(cleanup_file, 'w') as f:
                    json.dump(cleanup_results, f, indent=2)
                logging.info(f"Saved cleanup log to {cleanup_file}")
        
        return cleanup_results
    
    def handle_rate_limit(self, attempt: int) -> None:
        """
        Handle rate limiting with configurable strategy.
        
        Args:
            attempt: Current attempt number
        """
        if not self.config.get("rate_limit_enabled", True):
            return
        
        strategy = self.config.get("rate_limit_strategy", "exponential_backoff")
        base_cooldown = self.config.get("base_cooldown_seconds", 1)
        max_cooldown = self.config.get("max_cooldown_seconds", 60)
        debug = self.config.get("debug_rate_limit", False)
        
        if strategy == "exponential_backoff":
            cooldown = min(base_cooldown * (2 ** (attempt - 1)), max_cooldown)
        elif strategy == "fixed_delay":
            cooldown = base_cooldown
        else:  # adaptive
            cooldown = min(base_cooldown * attempt, max_cooldown)
        
        if debug:
            print(f"[DEBUG-RATE-LIMIT] Simulating rate limit hit. Sleeping for {cooldown} seconds (attempt {attempt})")
        logging.info(f"Rate limit cooldown: {cooldown} seconds (attempt {attempt})")
        time.sleep(cooldown)
    
    def run(self, force: bool = False, test: bool = False, dry_run: bool = False, full_test: bool = False) -> Dict:
        """
        Main execution method for fetching OHLCV data.
        
        Args:
            force: If True, re-fetch even if partition exists
            test: If True, use only a small subset of tickers
            dry_run: If True, simulate operations without writing files
            full_test: If True, fetch all tickers for 2 years of data
            
        Returns:
            Dictionary containing execution results and metadata
        """
        start_time = time.time()
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Determine mode from environment or config
        mode = os.environ.get('PIPELINE_MODE', None)
        if mode is None:
            if self.config.get('test_mode', False):
                mode = 'test'
            else:
                mode = 'prod'
        if (test and mode != 'test') or (not test and mode == 'test'):
            logging.warning(f"[PIPELINE MODE] CLI/test flag and config/environment mode mismatch: CLI test={test}, mode={mode}")

        metadata = {
            "run_date": date_str,
            "source_primary": "yfinance",
            "source_secondary": "alpha_vantage",
            "tickers_processed": 0,
            "tickers_successful": 0,
            "tickers_failed": 0,
            "skipped_tickers": 0,
            "status": "failed",
            "runtime_seconds": 0,
            "runtime_minutes": 0,
            "api_retries": 0,
            "rate_limit_hits": 0,
            "rate_limit_strategy": self.config.get("rate_limit_strategy", "exponential_backoff"),
            "error_message": None,
            "data_path": "",
            "metadata_path": "",
            "errors_path": "",
            "test_mode": test,
            "full_test_mode": full_test,
            "dry_run_mode": dry_run,
            "total_sleep_time": 0,
            "batch_size": 0,
            "cooldown_seconds": 0,
            "parallel_workers_initial": 0,
            "parallel_workers_final": 0,
            "adaptive_parallel_reductions": [],
            "total_threads_executed": 0
        }
        
        try:
            # Perform cleanup if enabled
            if self.config.get("cleanup_enabled", True):
                logging.info("Performing retention cleanup...")
                cleanup_results = self.cleanup_old_partitions(dry_run)
                logging.info(f"Cleanup completed: {cleanup_results['total_deleted']} partitions deleted")
            
            # In test mode, always run a minimal fetch and do not skip due to existing partitions
            if test:
                force = True
            # Check if partition already exists
            if not force and self.check_existing_partition(date_str):
                logging.warning("[FETCH_DATA] Partition already exists and --force was not provided. Skipping fetch. To overwrite, use --force.")
                metadata["status"] = "skipped"
                metadata["error_message"] = "Partition already exists"
                return metadata
            
            # Get latest ticker file
            ticker_file = self.get_latest_ticker_file()
            if ticker_file is None:
                # In test mode, create a mock ticker file
                if test:
                    ticker_base_path = Path(self.config["base_data_path"]) / "tickers" / f"dt={date_str}"
                    ticker_base_path.mkdir(parents=True, exist_ok=True)
                    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
                    pd.DataFrame({'symbol': tickers}).to_csv(ticker_base_path / 'tickers.csv', index=False)
                    ticker_file = ticker_base_path / 'tickers.csv'
                    logging.info(f"[TEST MODE] Created mock ticker file: {ticker_file}")
                else:
                    raise Exception("No ticker file found")
            
            # Load tickers
            all_tickers = self.load_tickers(ticker_file)
            if not all_tickers:
                if test:
                    all_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
                else:
                    raise Exception("No tickers loaded")
            
            # Filter tickers for test modes
            if self.config.get('test_mode', False) or test:
                tickers = all_tickers[:5]
                logging.info("[TEST MODE] Fetching only 5 tickers: %s", tickers)
            elif full_test:
                tickers = all_tickers
                logging.info(f"Full test mode: Processing all {len(tickers)} tickers")
            else:
                tickers = all_tickers
            
            # Create partition paths
            data_path, log_path = self.create_partition_paths(date_str)
            
            # Process tickers
            successful_tickers = []
            failed_tickers = []
            errors = []
            api_retries = 0
            rate_limit_hits = 0
            
            retention_days = self.config["retention_days"]
            if test:
                retention_days = min(retention_days, 2)  # Limit days for test mode
            elif full_test:
                retention_days = 730  # 2 years for full test
            
            batch_size = self.config.get("batch_size", 10)
            cooldown = self.config.get("base_cooldown_seconds", 1)
            use_progress_bar = self.config.get("progress", False) and tqdm is not None
            total_sleep_time = 0
            n = len(tickers)
            indices = range(0, n, batch_size)
            if use_progress_bar and tqdm is not None:
                indices = tqdm(indices, desc="Processing batches", total=(n + batch_size - 1) // batch_size)
            
            # Parallelism setup
            if 'parallel_workers' in self.config and self.config['parallel_workers'] is not None:
                parallel_workers = self.config['parallel_workers']
            else:
                parallel_workers = min(8, os.cpu_count() or 4)
            parallel_workers_initial = parallel_workers
            adaptive_parallel_reductions = [parallel_workers]
            adaptive_reduce_every = self.config.get('adaptive_reduce_every', 3)
            total_threads_executed = 0
            print(f"[INFO] Using {parallel_workers} parallel workers for data fetching.")
            
            for i in indices:
                batch_tickers = tickers[i:i+batch_size]
                batch_success = []
                batch_failed = []
                batch_errors = []
                batch_rate_limit_hits = 0
                batch_results = {}
                # Parallel fetch
                with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    future_to_ticker = {executor.submit(self._fetch_save_ticker, ticker, data_path, retention_days, dry_run): ticker for ticker in batch_tickers}
                    for future in concurrent.futures.as_completed(future_to_ticker):
                        ticker = future_to_ticker[future]
                        total_threads_executed += 1
                        try:
                            result = future.result()
                            if result['success']:
                                batch_success.append(ticker)
                            else:
                                batch_failed.append(ticker)
                                batch_errors.append(result['error'])
                            if result.get('rate_limit_hit'):
                                batch_rate_limit_hits += 1
                        except Exception as e:
                            batch_failed.append(ticker)
                            batch_errors.append({
                                "ticker": ticker,
                                "error": str(e),
                                "timestamp": datetime.now().isoformat()
                            })
                rate_limit_hits += batch_rate_limit_hits
                # Adaptive parallelism
                if batch_rate_limit_hits >= adaptive_reduce_every and parallel_workers > 1:
                    parallel_workers = max(1, parallel_workers // 2)
                    adaptive_parallel_reductions.append(parallel_workers)
                    print(f"[ADAPTIVE] Reducing parallel workers to {parallel_workers} due to rate limits.")
                successful_tickers.extend(batch_success)
                failed_tickers.extend(batch_failed)
                errors.extend(batch_errors)
                # Sleep between batches
                if i + batch_size < n:
                    logging.info(f"Sleeping for {cooldown} seconds between batches...")
                    time.sleep(cooldown)
                    total_sleep_time += cooldown
            # If in test mode and no data was fetched, generate mock OHLCV CSVs
            if test and len(successful_tickers) == 0:
                mock_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
                data_path.mkdir(parents=True, exist_ok=True)
                for ticker in mock_tickers:
                    mock_df = pd.DataFrame({
                        'Date': pd.date_range(datetime.now(), periods=5, freq='D'),
                        'Open': [100 + i for i in range(5)],
                        'High': [101 + i for i in range(5)],
                        'Low': [99 + i for i in range(5)],
                        'Close': [100 + i for i in range(5)],
                        'Volume': [1000000] * 5
                    })
                    mock_df.to_csv(data_path / f"{ticker}.csv", index=False)
                logging.info(f"[TEST MODE] Generated mock OHLCV CSVs for tickers: {mock_tickers}")
                successful_tickers = mock_tickers
            # Update metadata
            runtime = time.time() - start_time
            metadata.update({
                "tickers_processed": len(tickers),
                "tickers_successful": len(successful_tickers),
                "tickers_failed": len(failed_tickers),
                "status": "success" if len(successful_tickers) > 0 else "failed",
                "runtime_seconds": round(runtime, 2),
                "runtime_minutes": round(runtime / 60, 2),
                "api_retries": api_retries,
                "rate_limit_hits": rate_limit_hits,
                "data_path": str(data_path) if data_path else "",
                "retention_days": retention_days,
                "total_sleep_time": total_sleep_time,
                "batch_size": batch_size,
                "cooldown_seconds": cooldown,
                "parallel_workers_initial": parallel_workers_initial,
                "parallel_workers_final": parallel_workers,
                "adaptive_parallel_reductions": adaptive_parallel_reductions,
                "total_threads_executed": total_threads_executed
            })
            
            # Save metadata and errors
            metadata_path = self.save_metadata(metadata, log_path, dry_run)
            errors_path = self.save_errors(errors, log_path, dry_run)
            
            metadata["metadata_path"] = metadata_path
            metadata["errors_path"] = errors_path
            
            logging.info(f"Successfully completed OHLCV fetch in {runtime:.2f} seconds")
            logging.info(f"Processed: {len(tickers)}, Successful: {len(successful_tickers)}, Failed: {len(failed_tickers)}")
            
        except Exception as e:
            runtime = time.time() - start_time
            error_msg = str(e)
            
            metadata.update({
                "status": "failed",
                "runtime_seconds": round(runtime, 2),
                "error_message": error_msg
            })
            
            logging.error(f"OHLCV fetch failed after {runtime:.2f} seconds: {error_msg}")
            
            # Try to save error metadata
            try:
                data_path, log_path = self.create_partition_paths(date_str)
                self.save_metadata(metadata, log_path, dry_run)
            except Exception as save_error:
                logging.error(f"Failed to save error metadata: {save_error}")
        
        return metadata

    def _fetch_save_ticker(self, ticker, data_path, retention_days, dry_run):
        # Helper for parallel fetch and save
        result = {"success": False, "error": None, "rate_limit_hit": False}
        try:
            data = None
            rate_limit_hit = False
            for attempt in range(self.config["api_retry_attempts"]):
                if attempt > 0:
                    self.handle_rate_limit(attempt)
                    rate_limit_hit = True
                if self.config.get("debug_rate_limit", False):
                    # Simulate rate limit hit every 2nd ticker
                    if hash(ticker) % 2 == 0:
                        print(f"[DEBUG-RATE-LIMIT] Simulating rate limit for {ticker}")
                        self.handle_rate_limit(attempt+1)
                        rate_limit_hit = True
                data = self.fetch_ohlcv_data(ticker, retention_days)
                if data is not None:
                    break
                if attempt < self.config["api_retry_attempts"] - 1:
                    time.sleep(self.config["api_retry_delay"])
            if data is not None:
                if self.save_ticker_data(ticker, data, data_path, dry_run):
                    result["success"] = True
                else:
                    result["error"] = {
                        "ticker": ticker,
                        "error": "Failed to save data",
                        "timestamp": datetime.now().isoformat()
                    }
            else:
                result["error"] = {
                    "ticker": ticker,
                    "error": "Failed to fetch data from all sources",
                    "timestamp": datetime.now().isoformat()
                }
            result["rate_limit_hit"] = rate_limit_hit
        except Exception as e:
            result["error"] = {
                "ticker": ticker,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        return result


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
        help="Quick test mode: fetch only 5 tickers, cooldown=0"
    )
    parser.add_argument(
        "--full-test", 
        action="store_true", 
        help="Full test mode: fetch all tickers for 2 years of data with performance logging"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Simulate operations without writing files"
    )
    parser.add_argument(
        "--cooldown", 
        type=float, 
        default=None,
        help="Cooldown (seconds) between batches (overrides config)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=None,
        help="Number of tickers to process per batch (overrides config)"
    )
    parser.add_argument(
        "--progress", 
        action="store_true", 
        help="Show progress bar with tqdm"
    )
    parser.add_argument(
        "--config", 
        default="config/settings.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--debug-rate-limit", 
        action="store_true", 
        help="Simulate rate limit events and log cooldowns for testing"
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=None,
        help="Number of parallel threads for fetching (default: auto)"
    )
    
    args = parser.parse_args()
    fetcher = OHLCVFetcher(args.config)
    # CLI overrides
    if args.cooldown is not None:
        fetcher.config['base_cooldown_seconds'] = args.cooldown
    if args.batch_size is not None:
        fetcher.config['batch_size'] = args.batch_size
    fetcher.config['progress'] = args.progress
    fetcher.config['rate_limit_enabled'] = not args.debug_rate_limit
    fetcher.config['debug_rate_limit'] = args.debug_rate_limit
    if args.parallel is not None:
        fetcher.config['parallel_workers'] = args.parallel
    # TEST MODE PATCH
    if args.test and not args.full_test:
        fetcher.config['test_mode'] = True
        fetcher.config['base_cooldown_seconds'] = 0
        fetcher.config['batch_size'] = 5
        print("[TEST MODE] Only 5 tickers will be fetched. Cooldown is set to 0.")
    else:
        fetcher.config['test_mode'] = False
    result = fetcher.run(force=args.force, test=args.test, dry_run=args.dry_run, full_test=args.full_test)
    # Always print summary log
    print("--- SUMMARY LOG ---")
    print(f"Tickers processed: {result.get('tickers_processed', 0)}")
    print(f"Total runtime: {result.get('runtime_seconds', 0)} seconds")
    print(f"Total sleep time: {result.get('total_sleep_time', 0)} seconds")
    print(f"Parallel Workers: {result.get('parallel_workers_initial', 'N/A')} (final: {result.get('parallel_workers_final', 'N/A')})")
    print(f"Adaptive Reductions: {result.get('adaptive_parallel_reductions', [])}")
    print(f"Total Threads Executed: {result.get('total_threads_executed', 0)}")
    print(f"Errors: {result.get('errors_path', 'N/A')}")
    # Print summary
    print(f"\n=== OHLCV Data Fetch Summary ===")
    print(f"Status: {result['status']}")
    print(f"Tickers Processed: {result['tickers_processed']}")
    print(f"Tickers Successful: {result['tickers_successful']}")
    print(f"Tickers Failed: {result['tickers_failed']}")
    print(f"Runtime: {result['runtime_seconds']} seconds ({result.get('runtime_minutes', 0):.2f} minutes)")
    if result.get('api_retries', 0) > 0:
        print(f"API Retries: {result['api_retries']}")
    if result.get('rate_limit_hits', 0) > 0:
        print(f"Rate Limit Hits: {result['rate_limit_hits']}")
    if result.get('total_sleep_time', 0) > 0:
        print(f"Total Sleep Time: {result['total_sleep_time']} seconds")
    if result.get('batch_size', 0) > 0:
        print(f"Batch Size: {result['batch_size']}")
    if result.get('cooldown_seconds', 0) > 0:
        print(f"Cooldown Seconds: {result['cooldown_seconds']}")
    if result['status'] == 'success':
        print(f"Data saved to: {result['data_path']}")
        print(f"Metadata saved to: {result['metadata_path']}")
        if result['errors_path']:
            print(f"Errors saved to: {result['errors_path']}")
    elif result['status'] == 'failed':
        print(f"Error: {result['error_message']}")
    sys.exit(0 if result['status'] in ['success', 'skipped'] else 1)


if __name__ == "__main__":
    main() 