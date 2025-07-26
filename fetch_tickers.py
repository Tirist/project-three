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
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


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
                logging.StreamHandler(),
                logging.FileHandler('fetch_tickers.log')
            ]
        )
    
    def fetch_sp500_tickers(self) -> Tuple[List[str], List[str]]:
        """
        Fetch S&P 500 ticker symbols from Wikipedia.
        
        Returns:
            Tuple of (tickers, company_names) lists
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        
        for attempt in range(self.config["api_retry_attempts"]):
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
                
                # Extract data from table rows
                rows = table.find_all('tr')[1:]  # Skip header row
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        ticker = cells[0].get_text(strip=True)
                        company_name = cells[1].get_text(strip=True)
                        
                        if ticker and ticker != 'Symbol':
                            tickers.append(ticker)
                            company_names.append(company_name)
                
                logging.info(f"Successfully fetched {len(tickers)} tickers")
                return tickers, company_names
                
            except requests.RequestException as e:
                logging.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < self.config["api_retry_attempts"] - 1:
                    time.sleep(self.config["api_retry_delay"])
                else:
                    raise
            except Exception as e:
                logging.error(f"Unexpected error fetching tickers: {e}")
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
        cleaned_tickers = []
        
        for ticker in tickers:
            # Remove any whitespace
            ticker = ticker.strip()
            
            # Handle special cases like BRK.B -> BRK-B and BF.B -> BF-B
            if ticker in ['BRK.B', 'BF.B']:
                ticker = ticker.replace('.', '-')
            else:
                # For other cases, remove dots entirely
                ticker = ticker.replace('.', '')
            
            # Remove any non-alphanumeric characters except hyphens
            ticker = ''.join(c for c in ticker if c.isalnum() or c == '-')
            
            if ticker:
                cleaned_tickers.append(ticker)
        
        logging.info(f"Cleaned {len(cleaned_tickers)} ticker symbols")
        return cleaned_tickers
    
    def create_partition_paths(self, date_str: str) -> Tuple[Path, Path]:
        """
        Create partitioned folder paths for data and logs.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Tuple of (data_path, log_path) Path objects
        """
        # Create data path
        data_path = Path(self.config["base_data_path"]) / self.config["ticker_data_path"] / f"dt={date_str}"
        
        # Create log path
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
    
    def check_existing_partition(self, date_str: str) -> bool:
        """
        Check if today's partition already exists.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            True if partition exists, False otherwise
        """
        data_path = Path(self.config["base_data_path"]) / self.config["ticker_data_path"] / f"dt={date_str}"
        csv_path = data_path / "tickers.csv"
        
        exists = csv_path.exists()
        if exists:
            logging.info(f"Partition for {date_str} already exists at {csv_path}")
        
        return exists
    
    def get_previous_ticker_set(self) -> Set[str]:
        """
        Get the set of tickers from the previous successful run.
        
        Returns:
            Set of ticker symbols from previous run
        """
        ticker_base_path = Path(self.config["base_data_path"]) / self.config["ticker_data_path"]
        
        if not ticker_base_path.exists():
            return set()
        
        # Find all dt=YYYY-MM-DD directories
        date_dirs = [d for d in ticker_base_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
        
        if len(date_dirs) < 2:
            return set()
        
        # Sort by date and get the second latest (previous run)
        sorted_dirs = sorted(date_dirs, reverse=True)
        previous_dir = sorted_dirs[1]  # Skip the latest
        ticker_file = previous_dir / "tickers.csv"
        
        if not ticker_file.exists():
            return set()
        
        try:
            df = pd.read_csv(ticker_file)
            return set(df['symbol'].tolist())
        except Exception as e:
            logging.warning(f"Error reading previous ticker file {ticker_file}: {e}")
            return set()
    
    def calculate_ticker_changes(self, current_tickers: List[str], previous_tickers: Set[str]) -> Tuple[List[str], List[str]]:
        """
        Calculate ticker additions and removals.
        
        Args:
            current_tickers: List of current ticker symbols
            previous_tickers: Set of previous ticker symbols
            
        Returns:
            Tuple of (added_tickers, removed_tickers)
        """
        current_set = set(current_tickers)
        added = list(current_set - previous_tickers)
        removed = list(previous_tickers - current_set)
        
        return added, removed
    
    def save_diff_log(self, added_tickers: List[str], removed_tickers: List[str], log_path: Path, dry_run: bool = False) -> str:
        """
        Save ticker changes to diff.json file.
        
        Args:
            added_tickers: List of newly added tickers
            removed_tickers: List of removed tickers
            log_path: Path to save the diff file
            dry_run: If True, don't actually save files
            
        Returns:
            Path to the saved diff file
        """
        diff_data = {
            "run_date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": datetime.now().isoformat(),
            "tickers_added": added_tickers,
            "tickers_removed": removed_tickers,
            "total_added": len(added_tickers),
            "total_removed": len(removed_tickers),
            "net_change": len(added_tickers) - len(removed_tickers)
        }
        
        diff_path = log_path / "diff.json"
        
        if dry_run:
            logging.info(f"[DRY RUN] Would save diff to {diff_path}")
            return str(diff_path)
        
        with open(diff_path, 'w') as f:
            json.dump(diff_data, f, indent=2)
        
        logging.info(f"Saved ticker diff to {diff_path}")
        return str(diff_path)
    
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
        
        # Clean up ticker data
        ticker_base_path = Path(self.config["base_data_path"]) / self.config["ticker_data_path"]
        if ticker_base_path.exists():
            for date_dir in ticker_base_path.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith('dt='):
                    try:
                        date_str = date_dir.name[3:]  # Remove 'dt=' prefix
                        dir_date = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        if dir_date < cutoff_date:
                            if dry_run:
                                logging.info(f"[DRY RUN] Would delete ticker partition: {date_dir}")
                            else:
                                shutil.rmtree(date_dir)
                                logging.info(f"Deleted ticker partition: {date_dir}")
                            
                            cleanup_results["partitions_deleted"].append(str(date_dir))
                            cleanup_results["total_deleted"] += 1
                    except Exception as e:
                        error_msg = f"Error cleaning up {date_dir}: {e}"
                        logging.error(error_msg)
                        cleanup_results["errors"].append(error_msg)
        
        # Clean up ticker logs
        ticker_log_base_path = Path(self.config["base_log_path"]) / self.config["ticker_log_path"]
        if ticker_log_base_path.exists():
            for date_dir in ticker_log_base_path.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith('dt='):
                    try:
                        date_str = date_dir.name[3:]  # Remove 'dt=' prefix
                        dir_date = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        if dir_date < cutoff_date:
                            if dry_run:
                                logging.info(f"[DRY RUN] Would delete ticker log partition: {date_dir}")
                            else:
                                shutil.rmtree(date_dir)
                                logging.info(f"Deleted ticker log partition: {date_dir}")
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
    
    def run(self, force: bool = False, dry_run: bool = False, full_test: bool = False, test: bool = False) -> Dict:
        """
        Main execution method for fetching tickers.
        
        Args:
            force: If True, re-fetch even if partition exists
            dry_run: If True, simulate operations without writing files
            full_test: If True, validate entire ticker universe
            
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
            "dry_run_mode": dry_run,
            "total_sleep_time": 0,
            "batch_size": self.config.get("batch_size", 10)
        }
        
        data_path, log_path = None, None
        try:
            # Perform cleanup if enabled
            if self.config.get("cleanup_enabled", True):
                logging.info("Performing retention cleanup...")
                cleanup_results = self.cleanup_old_partitions(dry_run)
                logging.info(f"Cleanup completed: {cleanup_results['total_deleted']} partitions deleted")
            
            # Check if partition already exists
            if not force and self.check_existing_partition(date_str):
                logging.info("Partition already exists and force=False, skipping fetch")
                metadata["status"] = "skipped"
                metadata["error_message"] = "Partition already exists"
                return metadata
            
            # Fetch tickers (all at once, as before)
            tickers, company_names = self.fetch_sp500_tickers()
            # TEST MODE PATCH: limit to 5 tickers
            if self.config.get('test_mode', False):
                tickers = tickers[:5]
                company_names = company_names[:5]
                logging.info("[TEST MODE] Fetching only 5 tickers for smoke test: %s", tickers)
            cleaned_tickers = self.clean_ticker_symbols(tickers)
            # Batching
            batch_size = self.config.get("batch_size", 10)
            cooldown = self.config.get("base_cooldown_seconds", 1)
            use_progress_bar = self.config.get("progress", False) and tqdm is not None
            total_sleep_time = 0
            previous_tickers = self.get_previous_ticker_set()
            added_total, removed_total = [], []
            n = len(cleaned_tickers)
            indices = range(0, n, batch_size)
            if use_progress_bar and tqdm is not None:
                indices = tqdm(indices, desc="Processing batches", total=(n + batch_size - 1) // batch_size)
            for i in indices:
                batch_tickers = cleaned_tickers[i:i+batch_size]
                batch_companies = company_names[i:i+batch_size]
                # Calculate diff for this batch
                added, removed = self.calculate_ticker_changes(batch_tickers, previous_tickers)
                added_total.extend(added)
                removed_total.extend(removed)
                # Save batch to CSV (append or overwrite for first batch)
                data_path, log_path = self.create_partition_paths(date_str)
                mode = 'w' if i == 0 else 'a'
                # Save only the batch rows
                df = pd.DataFrame({'symbol': batch_tickers, 'company_name': batch_companies})
                csv_path = data_path / "tickers.csv"
                if not dry_run:
                    df.to_csv(csv_path, mode=mode, header=(i==0), index=False)
                # Save diff log for this batch (overwrite for first batch)
                if i == 0:
                    self.save_diff_log(added_total, removed_total, log_path, dry_run)
                # Log ticker-by-ticker
                for t in batch_tickers:
                    logging.info(f"Processed ticker: {t}")
                # Sleep between batches
                if i + batch_size < n:
                    logging.info(f"Sleeping for {cooldown} seconds between batches...")
                    time.sleep(cooldown)
                    total_sleep_time += cooldown
                previous_tickers = set(batch_tickers)
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
                "csv_path": str(data_path / "tickers.csv") if data_path else "",
                "diff_path": str(log_path / "diff.json") if log_path else ""
            })
            if log_path:
                metadata_path = self.save_metadata(metadata, log_path, dry_run)
                metadata["metadata_path"] = metadata_path
            logging.info(f"Successfully completed ticker fetch in {runtime:.2f} seconds")
            if added_total or removed_total:
                logging.info(f"Ticker changes: +{len(added_total)} added, -{len(removed_total)} removed")
        except Exception as e:
            runtime = time.time() - start_time
            error_msg = str(e)
            
            metadata.update({
                "status": "failed",
                "runtime_seconds": round(runtime, 2),
                "error_message": error_msg
            })
            
            logging.error(f"Ticker fetch failed after {runtime:.2f} seconds: {error_msg}")
            
            # Try to save error metadata
            try:
                if data_path is not None and log_path is not None:
                    self.save_metadata(metadata, log_path)
            except Exception as save_error:
                logging.error(f"Failed to save error metadata: {save_error}")
        
        return metadata


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
        help="Quick test mode: fetch only 5 tickers, no cooldowns"
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
        help="Show progress bar with tqdm"
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
    fetcher.config['progress'] = args.progress
    fetcher.config['debug_rate_limit'] = args.debug_rate_limit
    # TEST MODE PATCH
    if args.test and not args.full_test:
        fetcher.config['test_mode'] = True
        fetcher.config['base_cooldown_seconds'] = 0
        fetcher.config['batch_size'] = 5
        print("[TEST MODE] Fetching only 5 tickers for smoke test.")
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