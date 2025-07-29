#!/usr/bin/env python3
"""
Demo script for historical data bootstrap functionality.

This script demonstrates the bootstrap process with a small sample of tickers
to show how the historical data system works.
"""

import os
import sys
from pathlib import Path
import subprocess
import json
import time

def check_api_key():
    """Check if Alpha Vantage API key is available."""
    api_key = os.environ.get('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("❌ ALPHA_VANTAGE_API_KEY environment variable not set")
        print("Please set your Alpha Vantage API key:")
        print("export ALPHA_VANTAGE_API_KEY='your_api_key_here'")
        return False
    return True

def run_bootstrap_demo():
    """Run a demo bootstrap with a small sample of tickers."""
    print("🚀 Starting Historical Data Bootstrap Demo")
    print("=" * 50)
    
    # Check API key
    if not check_api_key():
        return False
    
    # Demo tickers (small sample for demonstration)
    demo_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    print(f"📊 Demo tickers: {', '.join(demo_tickers)}")
    print("⏱️  This will take about 1-2 minutes due to rate limiting...")
    print()
    
    # Run bootstrap with demo tickers
    cmd = [
        sys.executable, "bootstrap_historical_data.py",
        "--api-key", os.environ['ALPHA_VANTAGE_API_KEY'],
        "--tickers"
    ] + demo_tickers + [
        "--batch-size", "2",
        "--log-level", "INFO"
    ]
    
    try:
        print("🔄 Running bootstrap...")
        start_time = time.time()
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        runtime = time.time() - start_time
        
        if result.returncode == 0:
            print("✅ Bootstrap completed successfully!")
            print(f"⏱️  Runtime: {runtime:.1f} seconds")
            print()
            
            # Show results
            show_bootstrap_results()
            return True
        else:
            print("❌ Bootstrap failed!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Error running bootstrap: {e}")
        return False

def show_bootstrap_results():
    """Show the results of the bootstrap process."""
    historical_path = Path("data/raw/historical")
    summary_file = historical_path / "bootstrap_summary.json"
    
    if not summary_file.exists():
        print("❌ Bootstrap summary not found")
        return
    
    print("📈 Bootstrap Results:")
    print("-" * 30)
    
    with open(summary_file, 'r') as f:
        summary = json.load(f)
    
    bootstrap_info = summary.get("bootstrap_summary", {})
    
    print(f"✅ Successful tickers: {bootstrap_info.get('successful_tickers', 0)}")
    print(f"❌ Failed tickers: {bootstrap_info.get('failed_tickers', 0)}")
    print(f"📊 Success rate: {bootstrap_info.get('success_rate', '0%')}")
    print(f"📅 Total rows: {bootstrap_info.get('total_rows', 0):,}")
    print(f"⏱️  Runtime: {bootstrap_info.get('runtime_minutes', 0):.1f} minutes")
    
    if bootstrap_info.get("failed_tickers_list"):
        print(f"❌ Failed tickers: {', '.join(bootstrap_info['failed_tickers_list'])}")
    
    print()
    print("📁 Data Structure Created:")
    print("-" * 30)
    
    # Show directory structure
    if historical_path.exists():
        for ticker_dir in historical_path.glob("ticker=*"):
            ticker = ticker_dir.name.replace("ticker=", "")
            year_dirs = list(ticker_dir.glob("year=*"))
            print(f"  📂 {ticker}/")
            for year_dir in year_dirs:
                year = year_dir.name.replace("year=", "")
                data_file = year_dir / "data.parquet"
                if data_file.exists():
                    print(f"    📄 year={year}/data.parquet")
    
    print()

def demonstrate_incremental_mode():
    """Demonstrate how incremental mode works."""
    print("🔄 Demonstrating Incremental Mode")
    print("=" * 40)
    
    try:
        from pipeline.fetch_data import OHLCVFetcher
        
        fetcher = OHLCVFetcher()
        
        # Check historical data for a ticker
        ticker = "AAPL"
        latest_date = fetcher.get_latest_date(ticker)
        
        if latest_date:
            print(f"📅 Latest data for {ticker}: {latest_date.strftime('%Y-%m-%d')}")
            
            # Check completeness
            is_complete, days_available = fetcher.check_historical_completeness(ticker)
            print(f"📊 Data completeness: {days_available} days available")
            print(f"✅ Sufficient for technical indicators: {is_complete}")
            
            if is_complete:
                print("🎯 Ready for incremental updates!")
            else:
                print("⚠️  May need more historical data for optimal performance")
        else:
            print(f"❌ No historical data found for {ticker}")
            
    except ImportError as e:
        print(f"⚠️  Could not import pipeline modules: {e}")
        print("   This is expected if running demo outside the main project")
    
    print()

def show_next_steps():
    """Show next steps for using the historical data system."""
    print("🎯 Next Steps")
    print("=" * 20)
    print("1. 📚 Read the full guide: HISTORICAL_DATA_GUIDE.md")
    print("2. 🚀 Run full bootstrap for all S&P 500 tickers:")
    print("   python bootstrap_historical_data.py --api-key $ALPHA_VANTAGE_API_KEY")
    print("3. ⚡ Enjoy faster daily pipeline runs!")
    print("4. 📊 Monitor performance improvements")
    print()
    print("💡 The daily pipeline will automatically use incremental mode")
    print("   when historical data is available.")

def main():
    """Main demo function."""
    print("🎬 Historical Data Bootstrap Demo")
    print("=" * 40)
    print()
    
    # Check if we're in the right directory
    if not Path("bootstrap_historical_data.py").exists():
        print("❌ bootstrap_historical_data.py not found")
        print("Please run this demo from the project root directory")
        return False
    
    # Run demo
    success = run_bootstrap_demo()
    
    if success:
        # Show results
        show_bootstrap_results()
        
        # Demonstrate incremental mode
        demonstrate_incremental_mode()
        
        # Show next steps
        show_next_steps()
        
        print("🎉 Demo completed successfully!")
        return True
    else:
        print("❌ Demo failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 