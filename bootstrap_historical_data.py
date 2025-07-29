#!/usr/bin/env python3
"""
Bootstrap Historical Data Script

Fetches 2 years of historical OHLCV data for all S&P 500 tickers.
Handles Alpha Vantage rate limits and provides comprehensive logging.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from tqdm import tqdm

import logging
import sys
from pathlib import Path

# Add pipeline directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "pipeline"))
try:
    from fetch_tickers import TickerFetcher
except ImportError:
    # Fallback for different directory structure
    sys.path.insert(0, str(Path(__file__).parent))
    from pipeline.fetch_tickers import TickerFetcher


class HistoricalDataBootstrapper:
    """Handles bulk historical data fetching with rate limiting."""
    
    def __init__(self, api_key: str, output_dir: Path, batch_size: int = 10):
        self.api_key = api_key
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.base_url = "https://www.alphavantage.co/query"
        
        # Rate limiting: 5 calls per minute = 12 seconds between calls
        self.rate_limit_delay = 12
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistics tracking
        self.stats = {
            "total_tickers": 0,
            "successful_tickers": 0,
            "failed_tickers": 0,
            "total_rows": 0,
            "start_time": None,
            "end_time": None,
            "failed_tickers_list": [],
            "errors": {}
        }
    
    def fetch_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch 2 years of historical data for a single ticker."""
        try:
            # Calculate date range (2 years back from today)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=730)  # ~2 years
            
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "apikey": self.api_key,
                "outputsize": "full"  # Get full history
            }
            
            self.logger.debug(f"Fetching data for {ticker}")
            self.logger.debug(f"API URL: {self.base_url}")
            self.logger.debug(f"API params: {params}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                error_msg = data["Error Message"]
                self.logger.error(f"API error for {ticker}: {error_msg}")
                self.stats["errors"][ticker] = error_msg
                return None
            
            if "Note" in data:
                # Rate limit hit
                self.logger.warning(f"Rate limit hit for {ticker}, waiting longer...")
                time.sleep(self.rate_limit_delay * 2)
                return None
            
            # Extract time series data
            time_series = data.get("Time Series (Daily)")
            if not time_series:
                self.logger.error(f"No time series data found for {ticker}")
                self.stats["errors"][ticker] = "No time series data"
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient='index')
            
            # Rename columns to match our schema
            column_mapping = {
                "1. open": "open",
                "2. high": "high", 
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume"
            }
            df = df.rename(columns=column_mapping)
            
            # Add missing columns for compatibility
            df['adjusted_close'] = df['close']  # Use close as adjusted close
            df['dividends'] = 0.0  # Default to 0
            df['stock_splits'] = 1.0  # Default to 1
            
            # Convert to numeric
            numeric_columns = ["open", "high", "low", "close", "adjusted_close", "volume", "dividends", "stock_splits"]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add ticker column
            df['ticker'] = ticker
            
            # Reset index to make date a column
            df = df.reset_index()
            df = df.rename(columns={'index': 'date'})
            
            # Convert date to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Filter to last 2 years
            df = df[df['date'] >= start_date]
            
            # Sort by date
            df = df.sort_values('date', ascending=True)
            
            self.logger.info(f"Successfully fetched {len(df)} rows for {ticker}")
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for {ticker}: {e}")
            self.stats["errors"][ticker] = str(e)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for {ticker}: {e}")
            self.stats["errors"][ticker] = str(e)
            return None
    
    def save_ticker_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save ticker data in partitioned format."""
        try:
            # Create ticker directory
            ticker_dir = self.output_dir / f"ticker={ticker}"
            ticker_dir.mkdir(exist_ok=True)
            
            # Group by year and save
            df['year'] = df['date'].dt.year
            
            for year, year_data in df.groupby('year'):
                year_dir = ticker_dir / f"year={year}"
                year_dir.mkdir(exist_ok=True)
                
                # Save as parquet for efficiency
                output_file = year_dir / "data.parquet"
                year_data.to_parquet(output_file, index=False)
                
                self.logger.debug(f"Saved {len(year_data)} rows for {ticker} year {year}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data for {ticker}: {e}")
            return False
    
    def process_batch(self, tickers: List[str]) -> None:
        """Process a batch of tickers with rate limiting."""
        for ticker in tickers:
            self.logger.info(f"Processing ticker: {ticker}")
            
            # Fetch data
            df = self.fetch_historical_data(ticker)
            
            if df is not None and len(df) > 0:
                # Save data
                if self.save_ticker_data(ticker, df):
                    self.stats["successful_tickers"] += 1
                    self.stats["total_rows"] += len(df)
                    self.logger.info(f"✅ Successfully processed {ticker} ({len(df)} rows)")
                else:
                    self.stats["failed_tickers"] += 1
                    self.stats["failed_tickers_list"].append(ticker)
                    self.logger.error(f"❌ Failed to save data for {ticker}")
            else:
                self.stats["failed_tickers"] += 1
                self.stats["failed_tickers_list"].append(ticker)
                self.logger.error(f"❌ Failed to fetch data for {ticker}")
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
    
    def run(self, tickers: List[str]) -> Dict:
        """Run the bootstrap process."""
        self.stats["start_time"] = datetime.now()
        self.stats["total_tickers"] = len(tickers)
        
        self.logger.info(f"Starting bootstrap for {len(tickers)} tickers")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Rate limit delay: {self.rate_limit_delay} seconds")
        
        # Process tickers in batches
        batches = [tickers[i:i + self.batch_size] for i in range(0, len(tickers), self.batch_size)]
        
        with tqdm(total=len(tickers), desc="Bootstrap Progress") as pbar:
            for batch in batches:
                self.process_batch(batch)
                pbar.update(len(batch))
        
        self.stats["end_time"] = datetime.now()
        runtime = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        # Generate summary
        summary = {
            "bootstrap_summary": {
                "total_tickers": self.stats["total_tickers"],
                "successful_tickers": self.stats["successful_tickers"],
                "failed_tickers": self.stats["failed_tickers"],
                "success_rate": f"{(self.stats['successful_tickers'] / self.stats['total_tickers'] * 100):.2f}%",
                "total_rows": self.stats["total_rows"],
                "runtime_seconds": runtime,
                "runtime_minutes": runtime / 60,
                "runtime_hours": runtime / 3600,
                "start_time": self.stats["start_time"].isoformat(),
                "end_time": self.stats["end_time"].isoformat(),
                "failed_tickers_list": self.stats["failed_tickers_list"],
                "errors": self.stats["errors"]
            }
        }
        
        # Save summary
        summary_file = self.output_dir / "bootstrap_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Log summary
        self.logger.info("=" * 60)
        self.logger.info("BOOTSTRAP SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total tickers: {self.stats['total_tickers']}")
        self.logger.info(f"Successful: {self.stats['successful_tickers']}")
        self.logger.info(f"Failed: {self.stats['failed_tickers']}")
        self.logger.info(f"Success rate: {summary['bootstrap_summary']['success_rate']}")
        self.logger.info(f"Total rows: {self.stats['total_rows']:,}")
        self.logger.info(f"Runtime: {runtime/3600:.2f} hours")
        self.logger.info(f"Summary saved to: {summary_file}")
        
        if self.stats["failed_tickers_list"]:
            self.logger.warning(f"Failed tickers: {', '.join(self.stats['failed_tickers_list'])}")
        
        return summary


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Bootstrap historical data for S&P 500 tickers")
    parser.add_argument("--api-key", help="Alpha Vantage API key (will use config file if not provided)")
    parser.add_argument("--output-dir", default="data/raw/historical", help="Output directory")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to process (default: all S&P 500)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to configuration file")
    
    args = parser.parse_args()
    
    # Load API key from config file if not provided
    api_key = args.api_key
    if not api_key:
        try:
            import yaml
            with open(args.config, 'r') as f:
                config = yaml.safe_load(f)
            api_key = config.get('alpha_vantage_api_key')
            if not api_key:
                print("❌ No API key found in config file and --api-key not provided")
                print("Please either:")
                print("1. Add 'alpha_vantage_api_key: your_key' to config/settings.yaml")
                print("2. Use --api-key command line argument")
                return 1
            print(f"✅ Using API key from config file: {api_key[:8]}...")
        except Exception as e:
            print(f"❌ Error loading config file: {e}")
            print("Please provide --api-key argument")
            return 1
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Get project root
    project_root = Path(__file__).parent
    output_dir = project_root / args.output_dir
    
    # Get tickers
    if args.tickers:
        tickers = args.tickers
        logger.info(f"Using specified tickers: {tickers}")
    else:
        # Create ticker fetcher to get S&P 500 tickers
        ticker_fetcher = TickerFetcher()
        tickers, _ = ticker_fetcher.fetch_sp500_tickers()
        tickers = ticker_fetcher.clean_ticker_symbols(tickers)
        logger.info(f"Using all S&P 500 tickers: {len(tickers)} total")
    
    # Create bootstrapper
    bootstrapper = HistoricalDataBootstrapper(
        api_key=api_key,
        output_dir=output_dir,
        batch_size=args.batch_size
    )
    
    # Run bootstrap
    try:
        summary = bootstrapper.run(tickers)
        logger.info("Bootstrap completed successfully!")
        return 0
    except KeyboardInterrupt:
        logger.info("Bootstrap interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 