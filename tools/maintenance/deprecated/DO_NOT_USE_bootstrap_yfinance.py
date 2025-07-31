#!/usr/bin/env python3
"""
Bootstrap Historical Data with yfinance
Fetches historical OHLCV data using yfinance to avoid rate limiting issues.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from base_bootstrapper import BaseBootstrapper
from bootstrap_utils import (
    create_common_parser, get_tickers_from_args, print_bootstrap_info,
    setup_logging, validate_tickers
)


class YFinanceBootstrapper(BaseBootstrapper):
    """Handles bulk historical data fetching using yfinance with rate limiting."""
    
    def __init__(self, output_dir: Path, batch_size: int = 10):
        # Rate limiting: 1 second between requests (conservative)
        super().__init__(output_dir, batch_size, rate_limit_delay=1.0)
    
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
            
            return True
            
        except Exception as e:
            error_msg = f"Error saving data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False


def main():
    """Main function to run the bootstrap process."""
    parser = create_common_parser("Bootstrap historical data using yfinance")
    
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
    print_bootstrap_info(tickers, output_dir, args.batch_size, 1.0)  # yfinance rate limit
    
    # Create bootstrapper
    bootstrapper = YFinanceBootstrapper(output_dir, args.batch_size)
    
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