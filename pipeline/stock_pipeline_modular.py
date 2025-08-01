#!/usr/bin/env python3
"""
Modular Stock Pipeline

A simplified, modularized version of the stock evaluation pipeline designed for cloud environments.
Breaks down the pipeline into three main functions:
- fetch_data(): fetches raw stock data
- clean_data(): applies filtering/processing logic  
- store_data(): saves to local path

Usage:
    python pipeline/stock_pipeline_modular.py

Environment Variables:
    - TICKER_SYMBOLS: Comma-separated list of ticker symbols (default: S&P 500)
    - DATA_DAYS: Number of days of data to fetch (default: 30)
    - OUTPUT_PATH: Output directory path (default: data/processed)
    - API_KEY: API key for data sources (optional)
    - TEST_MODE: Set to 'true' for test mode (default: false)
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_config_from_env() -> Dict:
    """Get configuration from environment variables with fallback defaults."""
    config = {
        # Ticker symbols - default to S&P 500
        'ticker_symbols': os.environ.get('TICKER_SYMBOLS', 'AAPL,MSFT,GOOGL,AMZN,TSLA'),
        
        # Data fetch parameters
        'data_days': int(os.environ.get('DATA_DAYS', '30')),
        
        # Output paths
        'output_path': os.environ.get('OUTPUT_PATH', 'data/processed'),
        'raw_path': os.environ.get('RAW_PATH', 'data/raw'),
        
        # API configuration
        'api_key': os.environ.get('API_KEY', None),
        
        # Mode settings
        'test_mode': os.environ.get('TEST_MODE', 'false').lower() == 'true',
        
        # Data source preferences
        'use_yfinance': os.environ.get('USE_YFINANCE', 'true').lower() == 'true',
        'use_alpha_vantage': os.environ.get('USE_ALPHA_VANTAGE', 'false').lower() == 'true',
    }
    
    # Parse ticker symbols if provided as comma-separated string
    if isinstance(config['ticker_symbols'], str):
        config['ticker_symbols'] = [t.strip() for t in config['ticker_symbols'].split(',')]
    
    return config

def fetch_sp500_tickers() -> List[str]:
    """Fetch S&P 500 ticker symbols from Wikipedia."""
    try:
        logger.info("Fetching S&P 500 ticker symbols from Wikipedia...")
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'wikitable'})
        
        if not table:
            logger.warning("Could not find S&P 500 table, using default tickers")
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        tickers = []
        for row in table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all('td')
            if cells:
                ticker = cells[0].text.strip()
                if ticker:
                    tickers.append(ticker)
        
        logger.info(f"Successfully fetched {len(tickers)} S&P 500 tickers")
        return tickers[:500]  # Ensure we don't exceed 500
        
    except Exception as e:
        logger.error(f"Error fetching S&P 500 tickers: {e}")
        logger.info("Falling back to default tickers")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

def fetch_data() -> Dict[str, pd.DataFrame]:
    """
    Fetch raw stock data for configured ticker symbols.
    
    Returns:
        Dict mapping ticker symbols to their OHLCV data DataFrames
    """
    config = get_config_from_env()
    
    # Get ticker symbols
    if config['ticker_symbols'] == ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'] and not config['test_mode']:
        # If using default tickers and not in test mode, fetch full S&P 500
        tickers = fetch_sp500_tickers()
    else:
        tickers = config['ticker_symbols']
    
    if config['test_mode']:
        # Limit tickers in test mode
        tickers = tickers[:5]
        logger.info(f"Test mode: Using {len(tickers)} tickers")
    
    logger.info(f"Fetching data for {len(tickers)} tickers over {config['data_days']} days")
    
    data = {}
    successful_fetches = 0
    
    for i, ticker in enumerate(tickers, 1):
        try:
            logger.info(f"Fetching data for {ticker} ({i}/{len(tickers)})")
            
            # Fetch data using yfinance
            if config['use_yfinance']:
                ticker_obj = yf.Ticker(ticker)
                df = ticker_obj.history(period=f"{config['data_days']}d")
                
                if df.empty:
                    logger.warning(f"No data returned for {ticker}")
                    continue
                
                # Standardize column names
                df.columns = [col.lower() for col in df.columns]
                df = df.reset_index()
                df['date'] = pd.to_datetime(df['Date'])
                df = df.drop('Date', axis=1)
                
                # Add ticker column
                df['ticker'] = ticker
                
                data[ticker] = df
                successful_fetches += 1
                
                logger.info(f"Successfully fetched {len(df)} rows for {ticker}")
                
            else:
                logger.warning(f"yfinance disabled, skipping {ticker}")
                
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
            continue
    
    logger.info(f"Data fetch complete: {successful_fetches}/{len(tickers)} tickers successful")
    return data

def clean_data(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Apply filtering and processing logic to the raw data.
    
    Args:
        data: Dict mapping ticker symbols to their raw OHLCV data
        
    Returns:
        Dict mapping ticker symbols to their cleaned and processed data
    """
    config = get_config_from_env()
    logger.info("Starting data cleaning and processing...")
    
    cleaned_data = {}
    processed_tickers = 0
    
    for ticker, df in data.items():
        try:
            logger.info(f"Processing {ticker} ({len(df)} rows)")
            
            # Basic data validation
            required_columns = ['open', 'high', 'low', 'close', 'volume', 'date']
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"Missing required columns for {ticker}, skipping")
                continue
            
            # Remove rows with missing values
            initial_rows = len(df)
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
            if len(df) < initial_rows:
                logger.info(f"Removed {initial_rows - len(df)} rows with missing values for {ticker}")
            
            # Ensure data is sorted by date
            df = df.sort_values('date')
            
            # Add basic technical indicators
            if len(df) >= 50:
                # Simple Moving Averages
                df['sma_20'] = df['close'].rolling(window=20).mean()
                df['sma_50'] = df['close'].rolling(window=50).mean()
                
                # Exponential Moving Averages
                df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
                df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
                
                # MACD
                df['macd'] = df['ema_12'] - df['ema_26']
                df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
                
                # RSI
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                df['rsi'] = 100 - (100 / (1 + rs))
                
                # Price changes
                df['price_change'] = df['close'].pct_change()
                df['price_change_pct'] = df['price_change'] * 100
                
                # Volume indicators
                df['volume_sma'] = df['volume'].rolling(window=20).mean()
                df['volume_ratio'] = df['volume'] / df['volume_sma']
                
                logger.info(f"Added technical indicators for {ticker}")
            else:
                logger.warning(f"Insufficient data for technical indicators for {ticker} ({len(df)} rows)")
            
            # Remove rows with NaN values from technical indicators
            df = df.dropna()
            
            if len(df) > 0:
                cleaned_data[ticker] = df
                processed_tickers += 1
                logger.info(f"Successfully processed {ticker}: {len(df)} final rows")
            else:
                logger.warning(f"No valid data remaining for {ticker} after processing")
                
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            continue
    
    logger.info(f"Data cleaning complete: {processed_tickers}/{len(data)} tickers processed")
    return cleaned_data

def store_data(data: Dict[str, pd.DataFrame]) -> str:
    """
    Save the processed data to the configured output path.
    
    Args:
        data: Dict mapping ticker symbols to their processed data
        
    Returns:
        Path to the saved data file
    """
    config = get_config_from_env()
    
    if not data:
        logger.warning("No data to store")
        return ""
    
    # Create output directory
    output_path = Path(config['output_path'])
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create date-based subdirectory
    today = datetime.now().strftime('%Y-%m-%d')
    date_path = output_path / today
    date_path.mkdir(exist_ok=True)
    
    # Combine all data into a single DataFrame
    logger.info("Combining data from all tickers...")
    combined_data = []
    
    for ticker, df in data.items():
        combined_data.append(df)
    
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
        logger.info(f"Combined data: {len(final_df)} total rows from {len(data)} tickers")
        
        # Save as parquet file
        output_file = date_path / 'stock_data.parquet'
        final_df.to_parquet(output_file, index=False)
        
        # Save as CSV for compatibility
        csv_file = date_path / 'stock_data.csv'
        final_df.to_csv(csv_file, index=False)
        
        # Save metadata
        metadata = {
            'generated_at': datetime.now().isoformat(),
            'ticker_count': len(data),
            'total_rows': len(final_df),
            'date_range': {
                'start': final_df['date'].min().isoformat(),
                'end': final_df['date'].max().isoformat()
            },
            'tickers': list(data.keys()),
            'columns': list(final_df.columns),
            'test_mode': config['test_mode']
        }
        
        metadata_file = date_path / 'metadata.json'
        import json
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Data stored successfully:")
        logger.info(f"  - Parquet: {output_file}")
        logger.info(f"  - CSV: {csv_file}")
        logger.info(f"  - Metadata: {metadata_file}")
        
        return str(output_file)
    else:
        logger.warning("No data to combine and store")
        return ""

def main():
    """Main function that orchestrates the pipeline: fetch → clean → store."""
    start_time = time.time()
    
    logger.info("=== Starting Modular Stock Pipeline ===")
    
    try:
        # Step 1: Fetch data
        logger.info("Step 1: Fetching raw stock data...")
        raw_data = fetch_data()
        
        if not raw_data:
            logger.error("No data fetched, exiting")
            return False
        
        # Step 2: Clean and process data
        logger.info("Step 2: Cleaning and processing data...")
        processed_data = clean_data(raw_data)
        
        if not processed_data:
            logger.error("No data after processing, exiting")
            return False
        
        # Step 3: Store data
        logger.info("Step 3: Storing processed data...")
        output_file = store_data(processed_data)
        
        if not output_file:
            logger.error("Failed to store data")
            return False
        
        # Pipeline complete
        total_time = time.time() - start_time
        logger.info(f"=== Pipeline completed successfully in {total_time:.2f} seconds ===")
        logger.info(f"Output file: {output_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 