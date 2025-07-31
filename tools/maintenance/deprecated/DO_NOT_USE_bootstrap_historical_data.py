#!/usr/bin/env python3
"""
Bootstrap Historical Data Script

Fetches 2 years of historical OHLCV data for all S&P 500 tickers.
Handles Alpha Vantage rate limits and provides comprehensive logging.
"""

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from base_bootstrapper import BaseBootstrapper
from bootstrap_utils import (
    create_common_parser, get_api_key_from_config, get_tickers_from_args,
    load_config, print_bootstrap_info, setup_logging, validate_tickers
)


class AlphaVantageBootstrapper(BaseBootstrapper):
    """Handles bulk historical data fetching with Alpha Vantage API."""
    
    def __init__(self, api_key: str, output_dir: Path, batch_size: int = 10):
        # Rate limiting: 5 calls per minute = 12 seconds between calls
        super().__init__(output_dir, batch_size, rate_limit_delay=12)
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
    
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


def main():
    """Main function."""
    parser = create_common_parser("Bootstrap historical data for S&P 500 tickers using Alpha Vantage")
    parser.add_argument("--api-key", help="Alpha Vantage API key (will use config file if not provided)")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level, args.verbose)
    
    # Load config and get API key
    # Adjust config path to be relative to project root
    config_path = Path(__file__).parent.parent.parent / args.config
    config = load_config(str(config_path))
    api_key = get_api_key_from_config(config, args.api_key)
    if not api_key:
        return 1
    
    # Get tickers
    tickers, is_sp500 = get_tickers_from_args(args)
    if not validate_tickers(tickers):
        return 1
    
    # Get project root and output directory
    project_root = Path(__file__).parent.parent.parent
    output_dir = project_root / args.output_dir
    
    # Print bootstrap info
    print_bootstrap_info(tickers, output_dir, args.batch_size, 12.0)  # Alpha Vantage rate limit
    
    # Create bootstrapper
    bootstrapper = AlphaVantageBootstrapper(
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