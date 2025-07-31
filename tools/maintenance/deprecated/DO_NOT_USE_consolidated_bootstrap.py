#!/usr/bin/env python3
"""
Consolidated Bootstrap Script

This script uses yfinance (the primary data source) to bootstrap historical data.
It's designed to work with the existing pipeline that uses yfinance as the primary source.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yfinance as yf

from base_bootstrapper import BaseBootstrapper
from bootstrap_utils import (
    create_common_parser, get_tickers_from_args, print_bootstrap_info,
    setup_logging, validate_tickers
)


class YFinanceConsolidatedBootstrapper(BaseBootstrapper):
    """Consolidated bootstrapper using yfinance as the primary data source."""
    
    def __init__(self, output_dir: Path, batch_size: int = 10):
        # yfinance has much better rate limits than Alpha Vantage
        super().__init__(output_dir, batch_size, rate_limit_delay=0.5)
    
    def fetch_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch 2 years of historical data using yfinance."""
        try:
            self.logger.info(f"Fetching data for {ticker} using yfinance")
            
            # Create yfinance ticker object
            ticker_obj = yf.Ticker(ticker)
            
            # Fetch 2 years of daily data
            hist = ticker_obj.history(period="2y")
            
            if hist.empty:
                self.logger.error(f"No data found for {ticker}")
                return None
            
            # Add ticker column
            hist['ticker'] = ticker
            
            # Reset index to make date a column
            hist = hist.reset_index()
            
            # Rename columns to lowercase for consistency
            hist.columns = [col.lower() for col in hist.columns]
            
            # Ensure we have the required columns
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
            missing_columns = [col for col in required_columns if col not in hist.columns]
            
            if missing_columns:
                self.logger.error(f"Missing required columns for {ticker}: {missing_columns}")
                return None
            
            # Add missing columns for compatibility
            if 'adjusted_close' not in hist.columns:
                hist['adjusted_close'] = hist['close']
            if 'dividends' not in hist.columns:
                hist['dividends'] = 0.0
            if 'stock_splits' not in hist.columns:
                hist['stock_splits'] = 1.0
            
            # Convert to numeric
            numeric_columns = ["open", "high", "low", "close", "adjusted_close", "volume", "dividends", "stock_splits"]
            for col in numeric_columns:
                if col in hist.columns:
                    hist[col] = pd.to_numeric(hist[col], errors='coerce')
            
            # Convert date to datetime
            hist['date'] = pd.to_datetime(hist['date'])
            
            # Sort by date
            hist = hist.sort_values('date', ascending=True)
            
            self.logger.info(f"Successfully fetched {len(hist)} rows for {ticker}")
            return hist
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {ticker}: {e}")
            return None
    
    def save_ticker_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save ticker data in partitioned format (same as Alpha Vantage script)."""
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


def main():
    """Main function."""
    parser = create_common_parser("Consolidated bootstrap using yfinance")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level, args.verbose)
    
    # Get tickers
    tickers, is_sp500 = get_tickers_from_args(args)
    if not validate_tickers(tickers):
        return 1
    
    # Get project root and output directory
    project_root = Path(__file__).parent.parent.parent
    output_dir = project_root / args.output_dir
    
    # Print bootstrap info
    print_bootstrap_info(tickers, output_dir, args.batch_size, 0.5)  # yfinance rate limit
    
    # Create bootstrapper
    bootstrapper = YFinanceConsolidatedBootstrapper(output_dir, args.batch_size)
    
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