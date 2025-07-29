#!/usr/bin/env python3
import argparse
import json
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
import logging
import subprocess
import sys
import argparse
import os

# Add utils directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from progress import get_progress_tracker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class FeatureProcessor:
    def __init__(self, config_path="config/settings.yaml"):
        self.config = self.load_config(config_path)
        self.raw_path = Path(self.config.get("raw_data_path", "data/raw"))
        self.processed_path = Path(self.config.get("processed_data_path", "data/processed"))
        self.historical_path = Path(self.config.get("historical_data_path", "data/raw/historical"))
        self.metadata_path = Path("logs/features")
        self.metadata_path.mkdir(parents=True, exist_ok=True)
        # Determine mode from environment variable or config
        self.mode = os.environ.get('PIPELINE_MODE', None)
        if self.mode is None:
            if self.config.get('test_mode', False):
                self.mode = 'test'
            else:
                self.mode = 'prod'
        logging.info(f"[PIPELINE MODE] Running in {self.mode.upper()} mode.")

    def load_config(self, config_path):
        path = Path(config_path)
        if not path.exists():
            yaml_path = Path("config/settings.yaml")
            if yaml_path.exists():
                logging.warning(f"{config_path} not found, using {yaml_path}")
                with open(yaml_path, "r") as f:
                    return yaml.safe_load(f)
            raise FileNotFoundError(f"Config file not found at {config_path}")
        if path.suffix == ".yaml":
            with open(path, "r") as f:
                return yaml.safe_load(f)
        else:
            with open(path, "r") as f:
                return json.load(f)

    def add_features(self, df, ticker=None):
        # Ensure date is a column and lowercase
        if 'date' not in df.columns:
            if df.index.name and str(df.index.name).lower() == 'date':
                df = df.reset_index()
            else:
                df['date'] = df.index
        df['date'] = pd.to_datetime(df['date'])
        if ticker is not None:
            df['ticker'] = ticker
        df.columns = [c.lower() for c in df.columns]
        required = {'close', 'open', 'high', 'low', 'volume', 'date'}
        if not required.issubset(set(df.columns)):
            missing = required - set(df.columns)
            logging.warning(f"Missing required columns: {missing}. Skipping ticker {ticker if ticker else ''}.")
            return None, 0
        df = df.sort_values("date")
        # SMA/EMA
        df["sma_50"] = df["close"].rolling(window=50).mean()
        df["sma_200"] = df["close"].rolling(window=200).mean()
        df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()
        # MACD
        ema12 = df["close"].ewm(span=12, adjust=False).mean()
        ema26 = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = ema12 - ema26
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macd_histogram"] = df["macd"] - df["macd_signal"]
        # RSI
        delta = df["close"].diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = pd.Series(gain).rolling(14).mean()
        avg_loss = pd.Series(loss).rolling(14).mean()
        rs = avg_gain / avg_loss
        df["rsi_14"] = 100 - (100 / (1 + rs))
        # Bollinger Bands
        df["bb_middle"] = df["close"].rolling(20).mean()
        df["bb_std"] = df["close"].rolling(20).std()
        df["bb_upper"] = df["bb_middle"] + 2 * df["bb_std"]
        df["bb_lower"] = df["bb_middle"] - 2 * df["bb_std"]
        df = df.drop(columns=["bb_std"], errors="ignore")
        # Lowercase columns again (in case new ones added)
        df.columns = [c.lower() for c in df.columns]
        # Drop NaN rows caused by rolling calculations
        rows_before = len(df)
        df = df.dropna().copy()
        rows_dropped = rows_before - len(df)
        return df, rows_dropped

    def get_latest_raw_data(self, test_mode=False):
        """Get the latest raw data directory."""
        if test_mode:
            raw_base_path = Path("data/test/raw")
        else:
            raw_base_path = self.raw_path
        
        if not raw_base_path.exists():
            raise FileNotFoundError(f"Raw data directory not found: {raw_base_path}")
        
        # Find all dt=* directories and get the latest one
        partitions = [d for d in raw_base_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
        if not partitions:
            raise FileNotFoundError(f"No raw data partitions found in {raw_base_path}")
        
        latest_partition = max(partitions, key=lambda x: x.name)
        logging.info(f"Found latest raw data partition: {latest_partition}")
        return latest_partition
    
    def load_historical_data(self, ticker: str) -> pd.DataFrame:
        """
        Load historical data for a ticker from partitioned storage.
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            DataFrame with historical data or empty DataFrame if not found
        """
        try:
            ticker_dir = self.historical_path / f"ticker={ticker}"
            if not ticker_dir.exists():
                logging.debug(f"No historical data found for {ticker}")
                return pd.DataFrame()
            
            # Load all year partitions
            all_data = []
            for year_dir in ticker_dir.glob("year=*"):
                data_file = year_dir / "data.parquet"
                if data_file.exists():
                    year_data = pd.read_parquet(data_file)
                    all_data.append(year_data)
            
            if not all_data:
                logging.debug(f"No historical data files found for {ticker}")
                return pd.DataFrame()
            
            # Combine all years
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            combined_df = combined_df.sort_values('date').reset_index(drop=True)
            
            logging.debug(f"Loaded {len(combined_df)} historical rows for {ticker}")
            return combined_df
            
        except Exception as e:
            logging.error(f"Error loading historical data for {ticker}: {e}")
            return pd.DataFrame()
    
    def combine_historical_and_current(self, ticker: str, current_data: pd.DataFrame) -> pd.DataFrame:
        """
        Combine historical data with current data for feature calculation.
        
        Args:
            ticker: Ticker symbol
            current_data: Current day's data
            
        Returns:
            Combined DataFrame with historical + current data
        """
        historical_df = self.load_historical_data(ticker)
        
        if historical_df.empty:
            logging.info(f"No historical data for {ticker}, using current data only")
            return current_data
        
        # Ensure date columns are datetime
        current_data['date'] = pd.to_datetime(current_data['date'])
        historical_df['date'] = pd.to_datetime(historical_df['date'])
        
        # Combine data, keeping the most recent version of any duplicate dates
        combined_df = pd.concat([historical_df, current_data], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
        combined_df = combined_df.sort_values('date').reset_index(drop=True)
        
        logging.info(f"Combined data for {ticker}: {len(historical_df)} historical + {len(current_data)} current = {len(combined_df)} total")
        return combined_df
    
    def process_ticker_with_historical(self, ticker: str, current_data: pd.DataFrame) -> tuple:
        """
        Process a ticker using combined historical and current data.
        
        Args:
            ticker: Ticker symbol
            current_data: Current day's data
            
        Returns:
            Tuple of (processed_data, rows_dropped)
        """
        # Combine historical and current data
        combined_data = self.combine_historical_and_current(ticker, current_data)
        
        if combined_data.empty:
            logging.warning(f"No data available for {ticker}")
            return None, 0
        
        # Add features using the combined dataset
        processed_data, rows_dropped = self.add_features(combined_data, ticker)
        
        if processed_data is not None:
            # Keep only the most recent data for output (last 30 days)
            recent_data = processed_data.tail(30)
            logging.info(f"Processed {ticker}: {len(combined_data)} total rows, {len(recent_data)} recent rows output")
            return recent_data, rows_dropped
        else:
            return None, rows_dropped

    def create_output_paths(self, date_str, test_mode=False):
        """Create output paths for processed data and metadata."""
        if test_mode:
            processed_path = Path("data/test/processed") / f"dt={date_str}"
            metadata_path = Path("logs/test/features") / f"dt={date_str}"
        else:
            processed_path = self.processed_path / f"dt={date_str}"
            metadata_path = self.metadata_path / f"dt={date_str}"
        
        processed_path.mkdir(parents=True, exist_ok=True)
        metadata_path.mkdir(parents=True, exist_ok=True)
        
        return processed_path, metadata_path

    def run(self, test_mode=False, drop_incomplete=False):
        import time
        start_time = time.time()
        
        # Get latest raw data
        try:
            latest_raw = self.get_latest_raw_data(test_mode)
            date_str = latest_raw.name[3:]  # Remove "dt=" prefix
        except FileNotFoundError as e:
            logging.error(f"Could not find raw data: {e}")
            return False
        
        # Create output paths
        processed_path, metadata_path = self.create_output_paths(date_str, test_mode)
        
        # Get all CSV files in the raw data directory
        csv_files = list(latest_raw.glob("*.csv"))
        if not csv_files:
            logging.error(f"No CSV files found in {latest_raw}")
            return False
        
        logging.info(f"Found {len(csv_files)} CSV files to process")
        
        # Apply test mode limitations
        if test_mode:
            # Limit to 5 files for test mode
            csv_files = csv_files[:5]
            logging.info(f"[TEST MODE] Processing only 5 files: {[f.stem for f in csv_files]}")
        
        # Process files with progress tracking
        show_progress = self.config.get("progress", True)
        with get_progress_tracker(
            total=len(csv_files), 
            desc="Processing features", 
            unit="file",
            disable=not show_progress
        ) as progress:
            
            processed_data = []
            failed_tickers = []
            total_rows_dropped = 0
            
            for csv_file in csv_files:
                ticker = csv_file.stem
                try:
                    # Load data
                    df = pd.read_csv(csv_file)
                    
                    # Add features using historical data if available
                    if self.config.get("incremental_mode", True):
                        processed_df, rows_dropped = self.process_ticker_with_historical(ticker, df)
                    else:
                        processed_df, rows_dropped = self.add_features(df, ticker)
                    
                    if processed_df is not None:
                        processed_data.append(processed_df)
                        total_rows_dropped += rows_dropped
                        logging.info(f"Processed {ticker}: {len(processed_df)} rows (dropped {rows_dropped})")
                    else:
                        failed_tickers.append(ticker)
                        logging.warning(f"Failed to process {ticker}")
                        
                except Exception as e:
                    failed_tickers.append(ticker)
                    logging.error(f"Error processing {ticker}: {e}")
                
                progress.update(1, postfix={"current": ticker})
            
            # Combine all processed data
            if not processed_data:
                logging.error("No data was successfully processed")
                return False
            
            combined_df = pd.concat(processed_data, ignore_index=True)
            
            # Drop incomplete tickers if requested
            if drop_incomplete:
                min_rows = self.config.get("min_rows_per_ticker", 500)
                ticker_counts = combined_df['ticker'].value_counts()
                valid_tickers = ticker_counts[ticker_counts >= min_rows].index.tolist()
                combined_df = combined_df[combined_df['ticker'].isin(valid_tickers)]
                dropped_tickers = set(ticker_counts.index.tolist()) - set(valid_tickers)
                if dropped_tickers:
                    logging.info(f"Dropped {len(dropped_tickers)} tickers with <{min_rows} rows: {dropped_tickers}")
            
            # Save processed data
            output_file = processed_path / "features.parquet"
            combined_df.to_parquet(output_file, index=False)
            
            # Calculate runtime
            runtime = time.time() - start_time
            
            # Save metadata
            metadata = {
                "run_date": datetime.now().strftime('%Y-%m-%d'),
                "processing_date": datetime.now().isoformat(),
                "raw_data_date": date_str,
                "files_processed": len(csv_files),
                "tickers_processed": len(processed_data),
                "tickers_successful": len(processed_data),
                "tickers_failed": len(failed_tickers),
                "features_generated": len(combined_df.columns) - 7,  # Subtract base columns (date, ticker, open, high, low, close, volume)
                "total_rows": len(combined_df),
                "rows_dropped": total_rows_dropped,
                "tickers_with_insufficient_data": 0,  # Will be calculated if drop_incomplete is used
                "rows_dropped_due_to_nans": total_rows_dropped,
                "features_computed": len(combined_df.columns) - 7,
                "failed_tickers": failed_tickers,
                "status": "success" if len(failed_tickers) == 0 else "partial_success",
                "runtime_seconds": runtime,
                "runtime_minutes": runtime / 60,
                "error_message": None if len(failed_tickers) == 0 else f"Failed to process {len(failed_tickers)} tickers",
                "data_path": str(processed_path),
                "metadata_path": str(metadata_path / "metadata.json"),
                "test_mode": test_mode,
                "dry_run_mode": False,
                "drop_incomplete": drop_incomplete,
                "output_file": str(output_file)
            }
            
            metadata_file = metadata_path / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            runtime = time.time() - start_time
            logging.info(f"Feature processing completed in {runtime:.2f} seconds")
            logging.info(f"Processed {len(processed_data)} tickers, {len(combined_df)} total rows")
            logging.info(f"Output saved to: {output_file}")
            
            return True


def main():
    parser = argparse.ArgumentParser(description="Process OHLCV data and add technical indicators")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode (limited files, test directories)")
    parser.add_argument("--drop-incomplete", action="store_true", help="Drop tickers with insufficient data")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to configuration file")
    parser.add_argument("--progress", action="store_true", help="Show progress bar (enabled by default for full runs)")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    
    args = parser.parse_args()
    
    processor = FeatureProcessor(args.config)
    
    # Progress configuration
    if args.no_progress:
        processor.config['progress'] = False
    elif args.progress:
        processor.config['progress'] = True
    else:
        # Default: enable progress for full runs, disable for test mode
        processor.config['progress'] = not args.test_mode
    
    # Test mode configuration
    if args.test_mode:
        processor.config['test_mode'] = True
        print("[TEST MODE] Processing limited files for testing.")
    
    success = processor.run(test_mode=args.test_mode, drop_incomplete=args.drop_incomplete)
    
    if success:
        print("Feature processing completed successfully!")
        sys.exit(0)
    else:
        print("Feature processing failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
