#!/usr/bin/env python3
"""
Bootstrap Historical Data with yfinance
Fetches historical OHLCV data using yfinance to avoid rate limiting issues.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from tqdm import tqdm

import sys
from pathlib import Path

# Add pipeline directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipeline"))
try:
    from fetch_tickers import TickerFetcher
except ImportError:
    # Fallback for different directory structure
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from pipeline.fetch_tickers import TickerFetcher


class YFinanceBootstrapper:
    """Handles bulk historical data fetching using yfinance with rate limiting."""
    
    def __init__(self, output_dir: Path, batch_size: int = 10):
        self.output_dir = output_dir
        self.batch_size = batch_size
        
        # Rate limiting: 1 second between requests (conservative)
        self.rate_limit_delay = 1
        
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
        """Fetch 2 years of historical data for a single ticker using yfinance."""
        try:
            # Ensure ticker is a string
            if not isinstance(ticker, str):
                self.logger.error(f"Invalid ticker type: {type(ticker)}, ticker: {ticker}")
                return None
            
            self.logger.info(f"Processing ticker: {ticker}")
            
            # Create yfinance ticker object
            ticker_obj = yf.Ticker(ticker)
            
            # Fetch 2 years of daily data
            hist = ticker_obj.history(period="2y")
            
            if hist.empty:
                self.logger.error(f"No time series data found for {ticker}")
                self.stats["errors"][ticker] = "No time series data found"
                return None
            
            # Add ticker column
            hist['ticker'] = ticker
            
            # Reset index to make date a column
            hist = hist.reset_index()
            
            # Rename columns to lowercase
            hist.columns = [col.lower() for col in hist.columns]
            
            self.logger.info(f"Successfully fetched {len(hist)} rows for {ticker}")
            return hist
            
        except Exception as e:
            error_msg = f"Error fetching data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            self.stats["errors"][ticker] = str(e)
            return None
    
    def save_ticker_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save ticker data to CSV file."""
        try:
            # Create ticker-specific directory
            ticker_dir = self.output_dir / ticker
            ticker_dir.mkdir(exist_ok=True)
            
            # Save to CSV
            output_file = ticker_dir / f"{ticker}_historical.csv"
            df.to_csv(output_file, index=False)
            
            self.logger.info(f"✅ Successfully processed {ticker} ({len(df)} rows)")
            return True
            
        except Exception as e:
            error_msg = f"Error saving data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            self.stats["errors"][ticker] = str(e)
            return False
    
    def process_batch(self, tickers: List[str]) -> None:
        """Process a batch of tickers with rate limiting."""
        for ticker in tickers:
            try:
                # Ensure ticker is a string
                if not isinstance(ticker, str):
                    self.logger.error(f"Skipping invalid ticker: {ticker} (type: {type(ticker)})")
                    self.stats["failed_tickers"] += 1
                    self.stats["failed_tickers_list"].append(str(ticker))
                    continue
                
                # Fetch data
                df = self.fetch_historical_data(ticker)
                
                if df is not None:
                    # Save data
                    if self.save_ticker_data(ticker, df):
                        self.stats["successful_tickers"] += 1
                        self.stats["total_rows"] += len(df)
                    else:
                        self.stats["failed_tickers"] += 1
                        self.stats["failed_tickers_list"].append(ticker)
                else:
                    self.stats["failed_tickers"] += 1
                    self.stats["failed_tickers_list"].append(ticker)
                
                # Rate limiting delay
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                error_msg = f"Unexpected error processing {ticker}: {str(e)}"
                self.logger.error(error_msg)
                self.stats["errors"][str(ticker)] = str(e)
                self.stats["failed_tickers"] += 1
                self.stats["failed_tickers_list"].append(str(ticker))
    
    def run(self, tickers: List[str]) -> Dict:
        """Run the bootstrap process for all tickers."""
        # Ensure tickers is a list of strings
        if not isinstance(tickers, list):
            self.logger.error(f"Tickers must be a list, got: {type(tickers)}")
            return self.stats
        
        # Filter out any non-string items
        valid_tickers = []
        for ticker in tickers:
            if isinstance(ticker, str):
                valid_tickers.append(ticker)
            else:
                self.logger.warning(f"Skipping invalid ticker: {ticker} (type: {type(ticker)})")
        
        self.stats["start_time"] = datetime.now()
        self.stats["total_tickers"] = len(valid_tickers)
        
        self.logger.info(f"Starting bootstrap for {len(valid_tickers)} tickers")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Rate limit delay: {self.rate_limit_delay} seconds")
        
        # Process tickers in batches
        for i in range(0, len(valid_tickers), self.batch_size):
            batch = valid_tickers[i:i + self.batch_size]
            self.logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(valid_tickers) + self.batch_size - 1)//self.batch_size}")
            self.process_batch(batch)
        
        self.stats["end_time"] = datetime.now()
        
        # Calculate runtime
        runtime = self.stats["end_time"] - self.stats["start_time"]
        self.stats["runtime_seconds"] = runtime.total_seconds()
        
        # Log final statistics
        self.logger.info("=== Bootstrap Complete ===")
        self.logger.info(f"Total tickers: {self.stats['total_tickers']}")
        self.logger.info(f"Successful: {self.stats['successful_tickers']}")
        self.logger.info(f"Failed: {self.stats['failed_tickers']}")
        if self.stats['total_tickers'] > 0:
            self.logger.info(f"Success rate: {self.stats['successful_tickers']/self.stats['total_tickers']*100:.1f}%")
        self.logger.info(f"Total rows: {self.stats['total_rows']}")
        self.logger.info(f"Runtime: {self.stats['runtime_seconds']:.1f} seconds")
        
        if self.stats["failed_tickers_list"]:
            self.logger.info(f"Failed tickers: {self.stats['failed_tickers_list']}")
        
        return self.stats


def main():
    """Main function to run the bootstrap process."""
    parser = argparse.ArgumentParser(description="Bootstrap historical data using yfinance")
    parser.add_argument("--output-dir", default="data/historical", help="Output directory")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to process")
    parser.add_argument("--sp500", action="store_true", help="Use S&P 500 tickers")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get tickers
    if args.tickers:
        tickers = args.tickers
    elif args.sp500:
        # Fetch S&P 500 tickers
        try:
            fetcher = TickerFetcher()
            tickers_result = fetcher.fetch_sp500_tickers()
            
            # Handle tuple return (tickers, company_names)
            if isinstance(tickers_result, tuple) and len(tickers_result) == 2:
                tickers, company_names = tickers_result
                print(f"✅ Fetched {len(tickers)} S&P 500 tickers")
            elif isinstance(tickers_result, list):
                tickers = tickers_result
                print(f"✅ Fetched {len(tickers)} S&P 500 tickers")
            else:
                print(f"❌ Unexpected tickers format: {type(tickers_result)}")
                return 1
                
            if not tickers:
                print("❌ Failed to fetch S&P 500 tickers")
                return 1
        except Exception as e:
            print(f"❌ Error fetching S&P 500 tickers: {e}")
            return 1
    else:
        # Default test tickers
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    # Validate tickers
    if not isinstance(tickers, list):
        print(f"❌ Invalid tickers format: {type(tickers)}")
        return 1
    
    print(f"📊 Processing {len(tickers)} tickers")
    
    # Create bootstrapper
    output_dir = Path(args.output_dir)
    bootstrapper = YFinanceBootstrapper(output_dir, args.batch_size)
    
    # Run bootstrap
    try:
        stats = bootstrapper.run(tickers)
        
        # Save statistics
        stats_file = output_dir / "bootstrap_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        print(f"✅ Bootstrap completed successfully!")
        print(f"📊 Statistics saved to: {stats_file}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Bootstrap interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Bootstrap failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 