#!/usr/bin/env python3
"""
Fill Data Gaps Script

This script checks the last date in historical data and runs the pipeline
to fill any gaps. Much more efficient than bootstrap scripts.
"""

import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


def get_latest_date_from_historical(ticker: str = "AAPL") -> Optional[datetime]:
    """Get the latest date from historical data for a sample ticker."""
    try:
        historical_path = Path("data/raw/historical")
        ticker_dir = historical_path / f"ticker={ticker}"
        
        if not ticker_dir.exists():
            print(f"❌ No historical data found for {ticker}")
            return None
        
        # Load all year partitions
        all_data = []
        for year_dir in ticker_dir.glob("year=*"):
            data_file = year_dir / "data.parquet"
            if data_file.exists():
                year_data = pd.read_parquet(data_file)
                all_data.append(year_data)
        
        if not all_data:
            print(f"❌ No historical data files found for {ticker}")
            return None
        
        # Combine all years
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['date'] = pd.to_datetime(combined_df['date'], utc=True)
        latest_date = combined_df['date'].max()
        
        print(f"✅ Latest date in historical data: {latest_date.strftime('%Y-%m-%d')}")
        return latest_date
        
    except Exception as e:
        print(f"❌ Error getting latest date: {e}")
        return None


def check_if_pipeline_needs_to_run(latest_date: datetime) -> bool:
    """Check if the pipeline needs to run to fill gaps."""
    today = datetime.now().date()
    latest_date_only = latest_date.date()
    
    # Check if we're missing recent days
    days_missing = (today - latest_date_only).days
    
    if days_missing > 1:  # Allow for 1 day lag (weekends, holidays)
        print(f"⚠️  Missing {days_missing} days of data")
        print(f"   Latest: {latest_date_only}")
        print(f"   Today:  {today}")
        return True
    else:
        print(f"✅ Data is up to date (latest: {latest_date_only}, today: {today})")
        return False


def run_pipeline():
    """Run the pipeline to fetch missing data."""
    print("🔄 Running pipeline to fetch missing data...")
    
    try:
        # Run the pipeline with correct arguments
        cmd = [sys.executable, "pipeline/run_pipeline.py", "--full"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Pipeline completed successfully!")
            print("STDOUT:", result.stdout[-500:])  # Last 500 chars
            return True
        else:
            print("❌ Pipeline failed!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return False


def main():
    """Main function."""
    print("🔍 Checking for data gaps...")
    print("=" * 40)
    
    # Get latest date from historical data
    latest_date = get_latest_date_from_historical()
    if latest_date is None:
        print("❌ Could not determine latest date. Exiting.")
        return 1
    
    # Check if pipeline needs to run
    if check_if_pipeline_needs_to_run(latest_date):
        print("\n🚀 Starting pipeline to fill gaps...")
        success = run_pipeline()
        
        if success:
            print("\n✅ Data gaps filled successfully!")
            return 0
        else:
            print("\n❌ Failed to fill data gaps.")
            return 1
    else:
        print("\n✅ No action needed - data is current.")
        return 0


if __name__ == "__main__":
    exit(main()) 