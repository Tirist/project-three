#!/usr/bin/env python3
"""
Populate Historical Data Script

This script reads the 7/29 raw data and saves it to the historical data format
to populate the data/raw/historical directory.
"""

import pandas as pd
import logging
from pathlib import Path
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def populate_historical_data():
    """Populate historical data from 7/29 raw data."""
    
    # Paths
    raw_data_path = Path("data/raw/dt=2025-07-29")
    historical_path = Path("data/raw/historical")
    
    if not raw_data_path.exists():
        logging.error(f"Raw data path not found: {raw_data_path}")
        return False
    
    # Create historical directory
    historical_path.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = list(raw_data_path.glob("*.csv"))
    logging.info(f"Found {len(csv_files)} CSV files to process")
    
    successful = 0
    failed = 0
    
    for csv_file in csv_files:
        ticker = csv_file.stem
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Ensure date column is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Create ticker directory
            ticker_dir = historical_path / f"ticker={ticker}"
            ticker_dir.mkdir(parents=True, exist_ok=True)
            
            # Group by year and save
            df['year'] = df['date'].dt.year
            
            for year, year_data in df.groupby('year'):
                year_dir = ticker_dir / f"year={year}"
                year_dir.mkdir(exist_ok=True)
                
                # Save as parquet for efficiency
                output_file = year_dir / "data.parquet"
                year_data.to_parquet(output_file, index=False)
                
                logging.debug(f"Saved {len(year_data)} rows for {ticker} year {year}")
            
            successful += 1
            if successful % 50 == 0:
                logging.info(f"Processed {successful} tickers...")
                
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            failed += 1
    
    logging.info(f"Historical data population completed: {successful} successful, {failed} failed")
    return successful > 0

if __name__ == "__main__":
    success = populate_historical_data()
    sys.exit(0 if success else 1) 