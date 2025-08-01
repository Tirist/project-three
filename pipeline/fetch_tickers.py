#!/usr/bin/env python3
"""
Ticker Fetcher Module

This module fetches S&P 500 ticker symbols from Wikipedia and saves them to partitioned storage.
It supports both test mode (limited processing) and production mode.

Usage:
    python pipeline/fetch_tickers.py [--force] [--dry-run] [--test] [--full-test]

Features:
    - Fetches S&P 500 tickers from Wikipedia
    - Validates ticker count against expected range
    - Tracks changes from previous day
    - Supports test mode for development
    - Generates detailed metadata and logs
    - Automatic cleanup of old partitions
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Import from utils directory
from utils.common import create_partition_paths, save_metadata_to_file, cleanup_old_partitions, handle_rate_limit, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TickerFetcher:
    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config = load_config(config_path, "tickers")
        self.logger = logging.getLogger(__name__)
        
        # Set up logging
        log_dir = Path(self.config.get("base_log_path", "logs/")) / self.config.get("ticker_log_path", "tickers")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine mode from environment variable or config
        import os
        self.mode = os.environ.get('PIPELINE_MODE', None)
        if self.mode is None:
            if self.config.get('test_mode', False):
                self.mode = 'test'
            else:
                self.mode = 'prod'
        self.logger.info(f"[PIPELINE MODE] Running in {self.mode.upper()} mode.")

    def fetch_sp500_tickers(self) -> Tuple[List[str], List[str]]:
        """
        Fetch S&P 500 ticker symbols and company names from Wikipedia.
        
        Returns:
            Tuple of (tickers, company_names) lists
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        
        try:
            self.logger.info(f"Fetching S&P 500 tickers from {url}")
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
                    
                    if ticker and company_name:
                        tickers.append(ticker)
                        company_names.append(company_name)
            
            self.logger.info(f"Successfully fetched {len(tickers)} tickers")
            return tickers, company_names
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch tickers: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error parsing ticker data: {e}")
            raise

    def clean_ticker_symbols(self, tickers: List[str]) -> List[str]:
        """
        Clean and validate ticker symbols.
        
        Args:
            tickers: List of raw ticker symbols
            
        Returns:
            List of cleaned ticker symbols
        """
        cleaned = []
        for ticker in tickers:
            # Remove any whitespace and convert to uppercase
            cleaned_ticker = ticker.strip().upper()
            
            # Basic validation - ticker should be 1-5 characters, alphanumeric
            if (1 <= len(cleaned_ticker) <= 5 and 
                cleaned_ticker.isalnum() and 
                cleaned_ticker not in cleaned):
                cleaned.append(cleaned_ticker)
            else:
                self.logger.warning(f"Invalid ticker symbol: {ticker}")
        
        self.logger.info(f"Cleaned {len(cleaned)} valid ticker symbols from {len(tickers)} raw symbols")
        return cleaned

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
            self.logger.info(f"[DRY RUN] Would save {len(tickers)} tickers to {csv_path}")
            return str(csv_path)
        
        df.to_csv(csv_path, index=False)
        
        self.logger.info(f"Saved {len(tickers)} tickers to {csv_path}")
        return str(csv_path)

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
            self.logger.info(f"Ticker count validation passed: {ticker_count}")
            return True
        else:
            self.logger.warning(f"Ticker count {ticker_count} outside expected range [{min_expected}, {max_expected}]")
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
        data_path, _ = create_partition_paths(date_str, self.config, "tickers", test_mode)
        csv_path = data_path / "tickers.csv"
        
        if csv_path.exists():
            self.logger.info(f"Partition already exists: {csv_path}")
            return True
        else:
            self.logger.info(f"Partition does not exist: {csv_path}")
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
        data_path, _ = create_partition_paths(yesterday, self.config, "tickers", test_mode)
        csv_path = data_path / "tickers.csv"
        
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                tickers = set(df['symbol'].tolist())
                self.logger.info(f"Found {len(tickers)} tickers from previous day")
                return tickers
            except Exception as e:
                self.logger.warning(f"Could not read previous tickers: {e}")
                return set()
        else:
            self.logger.info("No previous ticker file found")
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
            self.logger.info(f"Added tickers: {added}")
        if removed:
            self.logger.info(f"Removed tickers: {removed}")
            
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
            self.logger.info(f"[DRY RUN] Would save diff log to {diff_path}")
            return str(diff_path)
        
        with open(diff_path, 'w') as f:
            json.dump(diff_data, f, indent=2)
        
        self.logger.info(f"Saved ticker diff to {diff_path}")
        return str(diff_path)

    def run(self, force: bool = False, dry_run: bool = False, full_test: bool = False, test: bool = False) -> Dict:
        """
        Run the ticker fetching pipeline.
        
        This method fetches S&P 500 ticker symbols from Wikipedia, validates them,
        and saves them to partitioned storage. It supports both test mode (limited processing)
        and production mode.
        
        Args:
            force (bool): If True, overwrite existing partition. Defaults to False.
            dry_run (bool): If True, don't actually save files. Defaults to False.
            full_test (bool): If True, run full test mode. Defaults to False.
            test (bool): If True, run in test mode. Defaults to False.
        
        Returns:
            Dict: Dictionary containing run results and metadata
        
        Raises:
            Exception: If ticker fetching fails or validation fails
        """
        start_time = time.time()
        
        # Determine test mode
        test_mode = test or full_test or self.config.get('test_mode', False)
        
        # Get current date
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Check if partition already exists
        if not force and self.check_existing_partition(date_str, test_mode):
            self.logger.info("Partition already exists and force=False, skipping")
            return {
                "status": "skipped",
                "message": "Partition already exists",
                "date": date_str,
                "test_mode": test_mode
            }
        
        # Clean up old partitions if enabled
        if self.config.get("cleanup_enabled", True):
            cleanup_results = cleanup_old_partitions(self.config, "tickers", dry_run, test_mode)
            self.logger.info(f"Cleanup completed: {cleanup_results['total_deleted']} partitions deleted")
        
        # Create partition paths
        data_path, log_path = create_partition_paths(date_str, self.config, "tickers", test_mode)
        
        # Fetch tickers with retry logic
        max_retries = self.config.get("api_retry_attempts", 3)
        retry_delay = self.config.get("api_retry_delay", 1)
        
        for attempt in range(max_retries):
            try:
                tickers, company_names = self.fetch_sp500_tickers()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    handle_rate_limit(attempt, self.config)
                else:
                    self.logger.error(f"All {max_retries} attempts failed")
                    raise
        
        # Clean ticker symbols
        cleaned_tickers = self.clean_ticker_symbols(tickers)
        
        # Filter company names to match cleaned tickers
        # Create a mapping from original tickers to company names
        ticker_to_company = dict(zip(tickers, company_names))
        cleaned_company_names = [ticker_to_company[ticker] for ticker in cleaned_tickers]
        
        # Validate ticker count
        if not self.validate_ticker_count(len(cleaned_tickers)):
            self.logger.warning("Ticker count validation failed, but continuing")
        
        # Get previous tickers for comparison
        previous_tickers = self.get_previous_ticker_set(test_mode)
        added_tickers, removed_tickers = self.calculate_ticker_changes(cleaned_tickers, previous_tickers)
        
        # Save tickers to CSV
        csv_path = self.save_tickers_csv(cleaned_tickers, cleaned_company_names, data_path, dry_run)
        
        # Save diff log
        diff_path = self.save_diff_log(added_tickers, removed_tickers, log_path, dry_run)
        
        # Calculate runtime
        runtime = time.time() - start_time
        
        # Prepare metadata
        metadata = {
            "run_date": datetime.now().strftime('%Y-%m-%d'),
            "processing_date": datetime.now().isoformat(),
            "tickers_fetched": len(cleaned_tickers),
            "tickers_added": len(added_tickers),
            "tickers_removed": len(removed_tickers),
            "net_change": len(added_tickers) - len(removed_tickers),
            "validation_passed": self.validate_ticker_count(len(cleaned_tickers)),
            "status": "success",
            "runtime_seconds": runtime,
            "runtime_minutes": runtime / 60,
            "error_message": None,
            "data_path": str(data_path),
            "log_path": str(log_path),
            "csv_path": csv_path,
            "diff_path": diff_path,
            "test_mode": test_mode,
            "dry_run": dry_run,
            "force": force
        }
        
        # Save metadata
        save_metadata_to_file(metadata, log_path, dry_run)
        
        # Log summary
        self.logger.info(f"Ticker fetching completed in {runtime:.2f} seconds")
        self.logger.info(f"Fetched {len(cleaned_tickers)} tickers")
        if added_tickers:
            self.logger.info(f"Added: {added_tickers}")
        if removed_tickers:
            self.logger.info(f"Removed: {removed_tickers}")
        
        return metadata


def main():
    parser = argparse.ArgumentParser(description="Fetch S&P 500 ticker symbols")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing partition")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually save files")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--full-test", action="store_true", help="Run full test mode")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to configuration file")
    
    args = parser.parse_args()
    
    fetcher = TickerFetcher(args.config)
    
    try:
        result = fetcher.run(
            force=args.force,
            dry_run=args.dry_run,
            full_test=args.full_test,
            test=args.test
        )
        
        if result["status"] == "success":
            print("Ticker fetching completed successfully!")
            sys.exit(0)
        elif result["status"] == "skipped":
            print("Ticker fetching skipped (partition already exists)")
            sys.exit(0)
        else:
            print("Ticker fetching failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"Ticker fetching failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 