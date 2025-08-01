#!/usr/bin/env python3
"""
OHLCV Data Fetcher Module

This module fetches OHLCV (Open, High, Low, Close, Volume) data for stock tickers
and saves it to partitioned storage. It supports both incremental processing
(combining historical and current data) and full reprocessing modes.

Usage:
    python pipeline/fetch_data.py [--force] [--test] [--dry-run] [--full-test]

Features:
    - Fetches OHLCV data from multiple sources (yfinance, Alpha Vantage)
    - Incremental processing with historical data integration
    - Test mode for development and debugging
    - Progress tracking and detailed logging
    - Metadata generation for processing runs
    - Support for partitioned data storage
    - Automatic cleanup of old partitions
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

# Import from utils directory
from utils.common import create_partition_paths, save_metadata_to_file, cleanup_old_partitions, handle_rate_limit, load_config
from utils.progress import get_progress_tracker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class OHLCVFetcher:
    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config = load_config(config_path, "ohlcv")
        self.logger = logging.getLogger(__name__)
        
        # Set up logging
        log_dir = Path(self.config.get("base_log_path", "logs/")) / self.config.get("ohlcv_log_path", "fetch")
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

    def get_latest_ticker_file(self, test_mode: bool = False) -> Optional[Path]:
        """
        Get the latest ticker file from the most recent partition.
        
        Args:
            test_mode: If True, look in test directories
            
        Returns:
            Path to the latest ticker file, or None if not found
        """
        if test_mode:
            ticker_base_path = Path("data/test/tickers")
        else:
            ticker_base_path = Path(self.config.get("base_data_path", "data/")) / self.config.get("ticker_data_path", "tickers")
        
        if not ticker_base_path.exists():
            self.logger.error(f"Ticker directory not found: {ticker_base_path}")
            return None
        
        # Find all dt=* directories and get the latest one
        partitions = [d for d in ticker_base_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
        if not partitions:
            self.logger.error(f"No ticker partitions found in {ticker_base_path}")
            return None
        
        latest_partition = max(partitions, key=lambda x: x.name)
        ticker_file = latest_partition / "tickers.csv"
        
        if not ticker_file.exists():
            self.logger.error(f"Ticker file not found: {ticker_file}")
            return None
        
        self.logger.info(f"Found latest ticker file: {ticker_file}")
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
            self.logger.info(f"Loaded {len(tickers)} tickers from {ticker_file}")
            return tickers
        except Exception as e:
            self.logger.error(f"Failed to load tickers from {ticker_file}: {e}")
            raise

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
            self.logger.debug(f"Fetching {ticker} data from yfinance ({days} days)")
            stock = yf.Ticker(ticker)
            data = stock.history(period=f"{days}d")
            
            if data.empty:
                self.logger.warning(f"No data returned for {ticker}")
                return None
            
            # Reset index to make date a column
            data = data.reset_index()
            
            # Rename columns to lowercase
            data.columns = [col.lower() for col in data.columns]
            
            # Ensure required columns exist
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in data.columns for col in required_columns):
                missing = [col for col in required_columns if col not in data.columns]
                self.logger.warning(f"Missing columns for {ticker}: {missing}")
                return None
            
            # Convert date to datetime if needed
            data['date'] = pd.to_datetime(data['date'])
            
            self.logger.debug(f"Successfully fetched {len(data)} rows for {ticker}")
            return data
            
        except Exception as e:
            self.logger.error(f"Error fetching {ticker} from yfinance: {e}")
            return None

    def fetch_ohlcv_alpha_vantage(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data using Alpha Vantage API.
        
        Args:
            ticker: Ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        api_key = self.config.get("alpha_vantage_api_key")
        if not api_key:
            self.logger.warning("Alpha Vantage API key not configured")
            return None
        
        try:
            import requests
            
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "apikey": api_key,
                "outputsize": "compact" if days <= 100 else "full"
            }
            
            self.logger.debug(f"Fetching {ticker} data from Alpha Vantage")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "Error Message" in data:
                self.logger.error(f"Alpha Vantage error for {ticker}: {data['Error Message']}")
                return None
            
            if "Note" in data:
                self.logger.warning(f"Alpha Vantage rate limit for {ticker}: {data['Note']}")
                return None
            
            time_series = data.get("Time Series (Daily)")
            if not time_series:
                self.logger.warning(f"No time series data for {ticker}")
                return None
            
            # Convert to DataFrame
            records = []
            for date, values in time_series.items():
                records.append({
                    'date': date,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                })
            
            df = pd.DataFrame(records)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # Limit to requested days
            if len(df) > days:
                df = df.tail(days)
            
            self.logger.debug(f"Successfully fetched {len(df)} rows for {ticker}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching {ticker} from Alpha Vantage: {e}")
            return None

    def fetch_ohlcv_data(self, ticker: str, days: int) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data from available sources.
        
        Args:
            ticker: Ticker symbol
            days: Number of days to fetch
            
        Returns:
            DataFrame with OHLCV data, or None if failed
        """
        # Try yfinance first
        data = self.fetch_ohlcv_yfinance(ticker, days)
        if data is not None:
            return data
        
        # Fall back to Alpha Vantage if yfinance fails
        self.logger.info(f"yfinance failed for {ticker}, trying Alpha Vantage")
        data = self.fetch_ohlcv_alpha_vantage(ticker, days)
        if data is not None:
            return data
        
        self.logger.error(f"All data sources failed for {ticker}")
        return None

    def load_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load historical data for a ticker from partitioned storage.
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            DataFrame with historical data, or None if not found
        """
        try:
            historical_path = Path(self.config.get("historical_data_path", "data/raw/historical"))
            ticker_dir = historical_path / f"ticker={ticker}"
            
            if not ticker_dir.exists():
                self.logger.debug(f"No historical data found for {ticker}")
                return None
            
            # Load all year partitions
            all_data = []
            for year_dir in ticker_dir.glob("year=*"):
                data_file = year_dir / "data.parquet"
                if data_file.exists():
                    year_data = pd.read_parquet(data_file)
                    all_data.append(year_data)
            
            if not all_data:
                self.logger.debug(f"No historical data files found for {ticker}")
                return None
            
            # Combine all years
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            combined_df = combined_df.sort_values('date').reset_index(drop=True)
            
            self.logger.debug(f"Loaded {len(combined_df)} historical rows for {ticker}")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error loading historical data for {ticker}: {e}")
            return None

    def get_latest_date(self, ticker: str) -> Optional[datetime]:
        """
        Get the latest date for which we have data for a ticker.
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Latest date, or None if no data found
        """
        historical_df = self.load_historical_data(ticker)
        if historical_df is not None and not historical_df.empty:
            return historical_df['date'].max()
        return None

    def check_historical_completeness(self, ticker: str) -> Tuple[bool, int]:
        """
        Check if historical data is complete for a ticker.
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Tuple of (is_complete, days_missing)
        """
        min_days = self.config.get("min_historical_days", 730)  # 2 years
        historical_df = self.load_historical_data(ticker)
        
        if historical_df is None or historical_df.empty:
            return False, min_days
        
        days_available = len(historical_df)
        days_missing = max(0, min_days - days_available)
        
        return days_missing == 0, days_missing

    def fetch_incremental_data(self, ticker: str, days_back: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch incremental data for a ticker (only new data since last update).
        
        Args:
            ticker: Ticker symbol
            days_back: Number of days to look back for new data
            
        Returns:
            DataFrame with new data, or None if no new data needed
        """
        latest_date = self.get_latest_date(ticker)
        
        if latest_date is None:
            # No historical data, fetch full dataset
            self.logger.info(f"No historical data for {ticker}, fetching full dataset")
            return self.fetch_ohlcv_data(ticker, days_back)
        
        # Check if we need to fetch new data
        # Ensure both datetime objects are timezone-naive for comparison
        now = datetime.now()
        if latest_date.tzinfo is not None:
            latest_date = latest_date.replace(tzinfo=None)
        
        days_since_update = (now - latest_date).days
        
        if days_since_update <= 1:
            self.logger.debug(f"{ticker} data is up to date (last update: {latest_date})")
            return None
        
        # Fetch data since last update
        self.logger.info(f"Fetching incremental data for {ticker} (last update: {latest_date})")
        new_data = self.fetch_ohlcv_data(ticker, days_since_update + 5)  # Add buffer
        
        if new_data is not None:
            # Filter to only new data
            # Ensure both DataFrame dates and latest_date are timezone-naive for comparison
            if new_data['date'].dt.tz is not None:
                new_data = new_data.copy()
                new_data['date'] = new_data['date'].dt.tz_localize(None)
            
            new_data = new_data[new_data['date'] > latest_date]
            
            if not new_data.empty:
                self.logger.info(f"Found {len(new_data)} new rows for {ticker}")
                return new_data
            else:
                self.logger.debug(f"No new data found for {ticker}")
                return None
        
        return None

    def merge_with_historical(self, ticker: str, new_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Merge new data with historical data for a ticker.
        
        Args:
            ticker: Ticker symbol
            new_data: New data to merge
            
        Returns:
            Combined DataFrame, or None if failed
        """
        historical_df = self.load_historical_data(ticker)
        
        if historical_df is None or historical_df.empty:
            self.logger.info(f"No historical data for {ticker}, using new data only")
            return new_data
        
        # Ensure both DataFrames have timezone-naive dates for comparison
        if new_data['date'].dt.tz is not None:
            new_data = new_data.copy()
            new_data['date'] = new_data['date'].dt.tz_localize(None)
        
        if historical_df['date'].dt.tz is not None:
            historical_df = historical_df.copy()
            historical_df['date'] = historical_df['date'].dt.tz_localize(None)
        
        # Combine data, keeping the most recent version of any duplicate dates
        combined_df = pd.concat([historical_df, new_data], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
        combined_df = combined_df.sort_values('date').reset_index(drop=True)
        
        self.logger.info(f"Merged data for {ticker}: {len(historical_df)} historical + {len(new_data)} new = {len(combined_df)} total")
        return combined_df

    def save_historical_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """
        Save historical data for a ticker to partitioned storage.
        
        Args:
            ticker: Ticker symbol
            df: DataFrame to save
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            historical_path = Path(self.config.get("historical_data_path", "data/raw/historical"))
            ticker_dir = historical_path / f"ticker={ticker}"
            
            # Group by year and save each year to its own partition
            df['year'] = df['date'].dt.year
            
            for year, year_data in df.groupby('year'):
                year_dir = ticker_dir / f"year={year}"
                year_dir.mkdir(parents=True, exist_ok=True)
                
                data_file = year_dir / "data.parquet"
                year_data = year_data.drop(columns=['year'])
                year_data.to_parquet(data_file, index=False)
            
            self.logger.debug(f"Saved historical data for {ticker} ({len(df)} rows)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving historical data for {ticker}: {e}")
            return False

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
            self.logger.info(f"[DRY RUN] Would save {len(data)} rows for {ticker} to {csv_path}")
            return True
        
        try:
            data.to_csv(csv_path, index=False)
            self.logger.info(f"Saved {len(data)} rows for {ticker} to {csv_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save data for {ticker}: {e}")
            return False

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
            self.logger.info(f"[DRY RUN] Would save error log to {errors_path}")
            return str(errors_path)
        
        with open(errors_path, 'w') as f:
            json.dump(errors, f, indent=2)
        
        self.logger.info(f"Saved error log to {errors_path}")
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
        data_path, _ = create_partition_paths(date_str, self.config, "raw", test_mode)
        
        if data_path.exists():
            # Check if there are any CSV files in the partition
            csv_files = list(data_path.glob("*.csv"))
            return len(csv_files) > 0
        return False

    def get_historical_data_path(self, ticker: str) -> Path:
        """
        Get the path for historical data storage for a ticker.
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Path to historical data directory
        """
        historical_path = Path(self.config.get("historical_data_path", "data/raw/historical"))
        return historical_path / f"ticker={ticker}"

    def run(self, force: bool = False, test: bool = False, dry_run: bool = False, full_test: bool = False) -> Dict:
        """
        Run the OHLCV data fetching pipeline.
        
        This method fetches OHLCV data for stock tickers and saves it to partitioned storage.
        It supports both incremental processing (combining historical and current data)
        and full reprocessing modes.
        
        Args:
            force (bool): If True, overwrite existing partition. Defaults to False.
            test (bool): If True, run in test mode. Defaults to False.
            dry_run (bool): If True, don't actually save files. Defaults to False.
            full_test (bool): If True, run full test mode. Defaults to False.
        
        Returns:
            Dict: Dictionary containing run results and metadata
        
        Raises:
            Exception: If data fetching fails or validation fails
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
            cleanup_results = cleanup_old_partitions(self.config, "raw", dry_run, test_mode)
            self.logger.info(f"Cleanup completed: {cleanup_results['total_deleted']} partitions deleted")
        
        # Get latest ticker file
        ticker_file = self.get_latest_ticker_file(test_mode)
        if ticker_file is None:
            raise FileNotFoundError("No ticker file found")
        
        # Load tickers
        tickers = self.load_tickers(ticker_file)
        
        # Apply test mode limitations
        if test_mode and not full_test:
            # Limit to 5 tickers for test mode
            tickers = tickers[:5]
            self.logger.info(f"[TEST MODE] Processing only 5 tickers: {tickers}")
        
        # Create partition paths
        data_path, log_path = create_partition_paths(date_str, self.config, "raw", test_mode)
        
        # Process tickers with progress tracking
        show_progress = self.config.get("progress", True)
        with get_progress_tracker(
            total=len(tickers), 
            desc="Fetching OHLCV data", 
            unit="ticker",
            disable=not show_progress
        ) as progress:
            
            successful_tickers = []
            failed_tickers = []
            errors = []
            total_rows = 0
            
            for ticker in tickers:
                try:
                    # Fetch data based on mode
                    if self.config.get("incremental_mode", True):
                        # Incremental mode: fetch only new data
                        new_data = self.fetch_incremental_data(ticker)
                        if new_data is not None:
                            # Merge with historical data
                            combined_data = self.merge_with_historical(ticker, new_data)
                            if combined_data is not None:
                                # Save historical data
                                self.save_historical_data(ticker, combined_data)
                                # Use only recent data for output
                                output_data = combined_data.tail(30)
                            else:
                                output_data = new_data
                        else:
                            # No new data needed, use recent historical data
                            historical_data = self.load_historical_data(ticker)
                            if historical_data is not None:
                                output_data = historical_data.tail(30)
                            else:
                                self.logger.warning(f"No data available for {ticker}")
                                failed_tickers.append(ticker)
                                progress.update(1, postfix={"current": ticker})
                                continue
                    else:
                        # Full mode: fetch all data
                        output_data = self.fetch_ohlcv_data(ticker, 30)
                        if output_data is None:
                            failed_tickers.append(ticker)
                            progress.update(1, postfix={"current": ticker})
                            continue
                    
                    # Save ticker data
                    if self.save_ticker_data(ticker, output_data, data_path, dry_run):
                        successful_tickers.append(ticker)
                        total_rows += len(output_data)
                        self.logger.info(f"Processed {ticker}: {len(output_data)} rows")
                    else:
                        failed_tickers.append(ticker)
                        errors.append({
                            "ticker": ticker,
                            "error": "Failed to save data",
                            "timestamp": datetime.now().isoformat()
                        })
                        
                except Exception as e:
                    failed_tickers.append(ticker)
                    error_msg = f"Error processing {ticker}: {e}"
                    self.logger.error(error_msg)
                    errors.append({
                        "ticker": ticker,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                
                progress.update(1, postfix={"current": ticker})
            
            # Save error log if there are errors
            if errors:
                self.save_errors(errors, log_path, dry_run)
            
            # Calculate runtime
            runtime = time.time() - start_time
            
            # Prepare metadata
            metadata = {
                "run_date": datetime.now().strftime('%Y-%m-%d'),
                "processing_date": datetime.now().isoformat(),
                "tickers_processed": len(tickers),
                "tickers_successful": len(successful_tickers),
                "tickers_failed": len(failed_tickers),
                "total_rows": total_rows,
                "status": "success" if len(failed_tickers) == 0 else "partial_success",
                "runtime_seconds": runtime,
                "runtime_minutes": runtime / 60,
                "error_message": None if len(failed_tickers) == 0 else f"Failed to process {len(failed_tickers)} tickers",
                "data_path": str(data_path),
                "log_path": str(log_path),
                "test_mode": test_mode,
                "dry_run": dry_run,
                "force": force,
                "incremental_mode": self.config.get("incremental_mode", True),
                "failed_tickers": failed_tickers,
                "successful_tickers": successful_tickers
            }
            
            # Save metadata
            save_metadata_to_file(metadata, log_path, dry_run)
            
            # Log summary
            self.logger.info(f"OHLCV fetching completed in {runtime:.2f} seconds")
            self.logger.info(f"Processed {len(successful_tickers)} tickers, {total_rows} total rows")
            if failed_tickers:
                self.logger.warning(f"Failed to process {len(failed_tickers)} tickers: {failed_tickers}")
            
            return metadata


def main():
    parser = argparse.ArgumentParser(description="Fetch OHLCV data for stock tickers")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing partition")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually save files")
    parser.add_argument("--full-test", action="store_true", help="Run full test mode")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to configuration file")
    parser.add_argument("--progress", action="store_true", help="Show progress bar (enabled by default for full runs)")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    
    args = parser.parse_args()
    
    fetcher = OHLCVFetcher(args.config)
    
    # Progress configuration
    if args.no_progress:
        fetcher.config['progress'] = False
    elif args.progress:
        fetcher.config['progress'] = True
    else:
        # Default: enable progress for full runs, disable for test mode
        fetcher.config['progress'] = not args.test
    
    # Test mode configuration
    if args.test:
        fetcher.config['test_mode'] = True
        print("[TEST MODE] Processing limited tickers for testing.")
    
    try:
        result = fetcher.run(
            force=args.force,
            test=args.test,
            dry_run=args.dry_run,
            full_test=args.full_test
        )
        
        if result["status"] == "success":
            print("OHLCV fetching completed successfully!")
            sys.exit(0)
        elif result["status"] == "skipped":
            print("OHLCV fetching skipped (partition already exists)")
            sys.exit(0)
        else:
            print("OHLCV fetching failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"OHLCV fetching failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 