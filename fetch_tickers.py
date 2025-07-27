#!/usr/bin/env python3
"""
Stock Ticker Fetching Script for MVP Pipeline

This script fetches the current S&P 500 ticker list from Wikipedia,
cleans and standardizes the tickers, and saves them to a partitioned
folder structure with metadata logging.

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
from typing import List, Dict, Optional, Tuple, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yaml
import sys
from progress import get_progress_tracker


class TickerFetcher:
    """Handles fetching and processing of stock ticker data."""
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Initialize the TickerFetcher with configuration.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.setup_logging()
        
    def _load_config(self, config_path: str) -> Dict:
        """
        Load configuration from YAML file with fallback defaults.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Dictionary containing configuration settings
        """
        default_config = {
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
    
    def fetch_sp500_tickers(self) -> Tuple[List[str], List[str]]:
        """
        Fetch S&P 500 ticker symbols from Wikipedia.
        
        Returns:
            Tuple of (tickers, company_names) lists
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        
        for attempt in range(self.config.get("api_retry_attempts", 3)):
            try:
                logging.info(f"Fetching S&P 500 tickers from Wikipedia (attempt {attempt + 1})")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find the main table with ticker data
                table = soup.find('table', {'class': 'wikitable'})
                if not table:
                    raise ValueError("Could not find ticker table on Wikipedia page")
                
                tickers = []
                company_names = []
                
                # Extract ticker symbols and company names from table rows
                rows = table.find_all('tr')[1:]  # Skip header row
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        ticker = cells[0].get_text(strip=True)
                        company_name = cells[1].get_text(strip=True)
                        
                        if ticker and company_name:
                            tickers.append(ticker)
                            company_names.append(company_name)
                
                if len(tickers) > 0:
                    logging.info(f"Successfully fetched {len(tickers)} tickers")
                    return tickers, company_names
                else:
                    raise ValueError("No tickers found in table")
                    
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.config.get("api_retry_attempts", 3) - 1:
                    delay = self.config.get("api_retry_delay", 1) * (2 ** attempt)
                    logging.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise
        
        raise Exception("Failed to fetch tickers after all retry attempts")
    
    def clean_ticker_symbols(self, tickers: List[str]) -> List[str]:
        """
        Clean and standardize ticker symbols.
        
        Args:
            tickers: List of raw ticker symbols
            
        Returns:
            List of cleaned ticker symbols
        """
        cleaned = []
        for ticker in tickers:
            # Remove common suffixes and clean up
            cleaned_ticker = ticker.strip().upper()
            # Remove any non-alphanumeric characters except dots
            cleaned_ticker = ''.join(c for c in cleaned_ticker if c.isalnum() or c == '.')
            if cleaned_ticker:
                cleaned.append(cleaned_ticker)
        
        logging.info(f"Cleaned {len(cleaned)} ticker symbols")
        return cleaned
    
    def create_partition_paths(self, date_str: str, test_mode: bool = False) -> Tuple[Path, Path]:
        """
        Create partitioned folder paths for data and logs.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            test_mode: If True, use test directories instead of production
            
        Returns:
            Tuple of (data_path, log_path) Path objects
        """
        if test_mode:
            # Use test directories for test mode
            data_path = Path("data/test/tickers") / f"dt={date_str}"
            log_path = Path("logs/test/tickers") / f"dt={date_str}"
        else:
            # Use production directories
            data_path = Path(self.config["base_data_path"]) / self.config["ticker_data_path"] / f"dt={date_str}"
            log_path = Path(self.config["base_log_path"]) / self.config["ticker_log_path"] / f"dt={date_str}"
        
        # Ensure directories exist
        data_path.mkdir(parents=True, exist_ok=True)
        log_path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Created partition paths: {data_path}, {log_path}")
        return data_path, log_path
    
    def save_tickers_csv(self, tickers: List[str], company_names: List[str], data_path: Path, dry_run: bool = False) -> str:
        """
        Save tickers to CSV file.
        
        Args:
            tickers: List of ticker symbols
            company_names: List of company names
            data_path: Path to save the CSV file
            dry_run: If True, don't actually save files
            
        Returns:
            Path to the saved CSV file
        """
        df = pd.DataFrame({
            'symbol': tickers,
            'company_name': company_names
        })
        
        csv_path = data_path / "tickers.csv"
        
        if dry_run:
            logging.info(f"[DRY RUN] Would save {len(tickers)} tickers to {csv_path}")
            return str(csv_path)
        
        df.to_csv(csv_path, index=False)
        
        logging.info(f"Saved {len(tickers)} tickers to {csv_path}")
        return str(csv_path)
    
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
    
    def validate_ticker_count(self, ticker_count: int) -> bool:
        """
        Validate that the number of tickers is within expected range.
        
        Args:
            ticker_count: Number of tickers fetched
            
        Returns:
            True if count is valid, False otherwise
        """
        min_expected = self.config["min_tickers_expected"]
        max_expected = self.config["max_tickers_expected"]
        
        if min_expected <= ticker_count <= max_expected:
            logging.info(f"Ticker count validation passed: {ticker_count}")
            return True
        else:
            logging.warning(f"Ticker count {ticker_count} outside expected range [{min_expected}, {max_expected}]")
            return False
    
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
        csv_path = data_path / "tickers.csv"
        
        if csv_path.exists():
            logging.info(f"Partition already exists: {csv_path}")
            return True
        else:
            logging.info(f"Partition does not exist: {csv_path}")
            return False
    
    def get_previous_ticker_set(self, test_mode: bool = False) -> Set[str]:
        """
        Get the set of tickers from the previous day's partition.
        
        Args:
            test_mode: If True, look in test directories
            
        Returns:
            Set of ticker symbols from previous day
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        data_path, _ = self.create_partition_paths(yesterday, test_mode)
        csv_path = data_path / "tickers.csv"
        
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                tickers = set(df['symbol'].tolist())
                logging.info(f"Found {len(tickers)} tickers from previous day")
                return tickers
            except Exception as e:
                logging.warning(f"Could not read previous tickers: {e}")
                return set()
        else:
            logging.info("No previous ticker file found")
            return set()
    
    def calculate_ticker_changes(self, current_tickers: List[str], previous_tickers: Set[str]) -> Tuple[List[str], List[str]]:
        """
        Calculate which tickers were added or removed.
        
        Args:
            current_tickers: List of current ticker symbols
            previous_tickers: Set of previous ticker symbols
            
        Returns:
            Tuple of (added_tickers, removed_tickers) lists
        """
        current_set = set(current_tickers)
        added = list(current_set - previous_tickers)
        removed = list(previous_tickers - current_set)
        
        if added:
            logging.info(f"Added tickers: {added}")
        if removed:
            logging.info(f"Removed tickers: {removed}")
            
        return added, removed
    
    def save_diff_log(self, added_tickers: List[str], removed_tickers: List[str], log_path: Path, dry_run: bool = False) -> str:
        """
        Save ticker changes to diff log file.
        
        Args:
            added_tickers: List of added ticker symbols
            removed_tickers: List of removed ticker symbols
            log_path: Path to save the diff log
            dry_run: If True, don't actually save files
            
        Returns:
            Path to the saved diff log file
        """
        diff_data = {
            "date": datetime.now().isoformat(),
            "added": added_tickers,
            "removed": removed_tickers,
            "total_added": len(added_tickers),
            "total_removed": len(removed_tickers)
        }
        
        diff_path = log_path / "diff.json"
        
        if dry_run:
            logging.info(f"[DRY RUN] Would save diff log to {diff_path}")
            return str(diff_path)
        
        with open(diff_path, 'w') as f:
            json.dump(diff_data, f, indent=2)
        
        logging.info(f"Saved ticker diff to {diff_path}")
        return str(diff_path)
    
    def cleanup_old_partitions(self, dry_run: bool = False, test_mode: bool = False) -> Dict:
        """
        Clean up old ticker partitions based on retention policy.
        
        Args:
            dry_run: If True, don't actually delete files
            test_mode: If True, clean test directories
            
        Returns:
            Dictionary containing cleanup results
        """
        retention_days = self.config.get("retention_days", 30)
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        if test_mode:
            base_data_path = Path("data/test/tickers")
            base_log_path = Path("logs/test/tickers")
        else:
            base_data_path = Path(self.config["base_data_path"]) / self.config["ticker_data_path"]
            base_log_path = Path(self.config["base_log_path"]) / self.config["ticker_log_path"]
        
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
    
    def run(self, force: bool = False, dry_run: bool = False, full_test: bool = False, test: bool = False) -> Dict:
        """
        Main execution method for fetching tickers.
        
        Args:
            force: If True, re-fetch even if partition exists
            dry_run: If True, simulate operations without writing files
            full_test: If True, validate entire ticker universe
            test: If True, run in test mode (limited tickers, test directories)
            
        Returns:
            Dictionary containing execution results and metadata
        """
        start_time = time.time()
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        metadata = {
            "run_date": date_str,
            "source_primary": "wikipedia_sp500",
            "source_secondary": None,
            "tickers_fetched": 0,
            "tickers_added": 0,
            "tickers_removed": 0,
            "skipped_tickers": 0,
            "status": "failed",
            "runtime_seconds": 0,
            "runtime_minutes": 0,
            "api_retries": 0,
            "rate_limit_hits": 0,
            "rate_limit_strategy": self.config.get("rate_limit_strategy", "exponential_backoff"),
            "error_message": None,
            "full_test_mode": full_test,
            "test_mode": test,
            "dry_run_mode": dry_run,
            "total_sleep_time": 0,
            "batch_size": self.config.get("batch_size", 10)
        }
        
        data_path, log_path = None, None
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
            
            # Fetch tickers (all at once)
            tickers, company_names = self.fetch_sp500_tickers()
            
            # Apply test mode limitations if specified
            if test and not full_test:
                # Limit to 5 tickers for test mode
                tickers = tickers[:5]
                company_names = company_names[:5]
                logging.info(f"[TEST MODE] Fetching only 5 tickers for smoke test: {tickers}")
            
            cleaned_tickers = self.clean_ticker_symbols(tickers)
            
            # Create partition paths
            data_path, log_path = self.create_partition_paths(date_str, test)
            
            # Process tickers with progress tracking
            batch_size = self.config.get("batch_size", 10)
            cooldown = self.config.get("base_cooldown_seconds", 1)
            show_progress = self.config.get("progress", True)
            
            # Use progress tracker for batch processing
            total_batches = (len(cleaned_tickers) + batch_size - 1) // batch_size
            with get_progress_tracker(
                total=total_batches, 
                desc="Processing ticker batches", 
                unit="batch",
                disable=not show_progress
            ) as progress:
                
                total_sleep_time = 0
                previous_tickers = self.get_previous_ticker_set(test)
                added_total, removed_total = [], []
                
                for i in range(0, len(cleaned_tickers), batch_size):
                    batch_tickers = cleaned_tickers[i:i+batch_size]
                    batch_companies = company_names[i:i+batch_size]
                    
                    # Calculate diff for this batch
                    added, removed = self.calculate_ticker_changes(batch_tickers, previous_tickers)
                    added_total.extend(added)
                    removed_total.extend(removed)
                    
                    # Save batch to CSV (append or overwrite for first batch)
                    mode = 'w' if i == 0 else 'a'
                    df = pd.DataFrame({'symbol': batch_tickers, 'company_name': batch_companies})
                    csv_path = data_path / "tickers.csv"
                    
                    if not dry_run:
                        df.to_csv(csv_path, mode=mode, header=(i==0), index=False)
                    
                    # Save diff log for this batch (overwrite for first batch)
                    if i == 0:
                        self.save_diff_log(added_total, removed_total, log_path, dry_run)
                    
                    # Log ticker-by-ticker
                    for ticker in batch_tickers:
                        logging.info(f"Processed ticker: {ticker}")
                    
                    # Sleep between batches (except for test mode)
                    if test and not full_test:
                        # No cooldown for test mode
                        pass
                    elif i + batch_size < len(cleaned_tickers):
                        logging.info(f"Sleeping for {cooldown} seconds between batches...")
                        time.sleep(cooldown)
                        total_sleep_time += cooldown
                    
                    previous_tickers = set(batch_tickers)
                    progress.update(1, postfix={"current": f"{min(i+batch_size, len(cleaned_tickers))}/{len(cleaned_tickers)}"})
            
            # Update metadata
            runtime = time.time() - start_time
            metadata.update({
                "tickers_fetched": len(cleaned_tickers),
                "tickers_added": len(added_total),
                "tickers_removed": len(removed_total),
                "status": "success",
                "runtime_seconds": round(runtime, 2),
                "runtime_minutes": round(runtime / 60, 2),
                "total_sleep_time": total_sleep_time,
                "csv_path": str(data_path / "tickers.csv"),
                "metadata_path": str(log_path / "metadata.json"),
                "diff_path": str(log_path / "diff.json")
            })
            
            # Save metadata
            self.save_metadata(metadata, log_path, dry_run)
            
            # Validate ticker count
            if not test:  # Only validate for production runs
                self.validate_ticker_count(len(cleaned_tickers))
            
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
            
            logging.error(f"Ticker fetch failed: {e}")
            raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Fetch S&P 500 ticker symbols")
    parser.add_argument(
        "--force", 
        action="store_true", 
        help="Force re-fetch even if partition already exists"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Simulate operations without writing files"
    )
    parser.add_argument(
        "--full-test", 
        action="store_true", 
        help="Validate entire ticker universe (detect IPO additions/removals)"
    )
    parser.add_argument(
        "--test", 
        action="store_true", 
        help="Test mode: fetch limited tickers and use test directories"
    )
    parser.add_argument(
        "--cooldown", 
        type=float, 
        default=None,
        help="Cooldown (seconds) between API calls or batches (overrides config)"
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
        help="Show progress bar (enabled by default for full runs)"
    )
    parser.add_argument(
        "--no-progress", 
        action="store_true", 
        help="Disable progress bar"
    )
    parser.add_argument(
        "--debug-rate-limit", 
        action="store_true", 
        help="Simulate rate limit events and log cooldowns for testing"
    )
    parser.add_argument(
        "--config", 
        default="config/settings.yaml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    fetcher = TickerFetcher(args.config)
    
    # CLI overrides
    if args.cooldown is not None:
        fetcher.config['base_cooldown_seconds'] = args.cooldown
    if args.batch_size is not None:
        fetcher.config['batch_size'] = args.batch_size
    
    # Progress configuration
    if args.no_progress:
        fetcher.config['progress'] = False
    elif args.progress:
        fetcher.config['progress'] = True
    else:
        # Default: enable progress for full runs, disable for test mode
        fetcher.config['progress'] = not args.test
    
    fetcher.config['debug_rate_limit'] = args.debug_rate_limit
    
    # Test mode configuration
    if args.test and not args.full_test:
        fetcher.config['test_mode'] = True
        fetcher.config['base_cooldown_seconds'] = 0  # No cooldown for test mode
        print("[TEST MODE] Fetching limited tickers for smoke test.")
    else:
        fetcher.config['test_mode'] = False
    
    result = fetcher.run(force=args.force, dry_run=args.dry_run, full_test=args.full_test, test=args.test)
    
    # Print summary
    print(f"\n=== Ticker Fetch Summary ===")
    print(f"Status: {result['status']}")
    print(f"Tickers Fetched: {result.get('tickers_fetched', 0)}")
    if result.get('tickers_added', 0) > 0 or result.get('tickers_removed', 0) > 0:
        print(f"Ticker Changes: +{result.get('tickers_added', 0)} added, -{result.get('tickers_removed', 0)} removed")
    print(f"Runtime: {result.get('runtime_seconds', 0)} seconds ({result.get('runtime_minutes', 0):.2f} minutes)")
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
    if result.get('status') == 'success':
        print(f"Data saved to: {result.get('csv_path','')}")
        print(f"Metadata saved to: {result.get('metadata_path','')}")
        if result.get('diff_path'):
            print(f"Diff log saved to: {result['diff_path']}")
    elif result.get('status') == 'failed':
        print(f"Error: {result.get('error_message','')}")
    
    # Always print summary log
    print("--- SUMMARY LOG ---")
    print(f"Tickers processed: {result.get('tickers_fetched', 0)}")
    print(f"Total runtime: {result.get('runtime_seconds', 0)} seconds")
    print(f"Total sleep time: {result.get('total_sleep_time', 0)} seconds")
    print(f"Errors: {result.get('errors_path', 'N/A')}")
    
    sys.exit(0 if result.get('status') in ['success', 'skipped'] else 1)


if __name__ == "__main__":
    main() 