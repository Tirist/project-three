#!/usr/bin/env python3
"""
Investigate API Connectivity Issues
Test API connectivity and identify issues causing fetch_data failures.
"""

import requests
import yfinance as yf
import time
from pathlib import Path
import json

def test_yfinance_api():
    """Test yfinance API connectivity."""
    print("🔍 Testing yfinance API connectivity...")
    
    test_tickers = ["AAPL", "MSFT", "GOOGL", "BF.B", "BRK.B"]
    results = {}
    
    for ticker in test_tickers:
        try:
            print(f"  Testing {ticker}...")
            ticker_obj = yf.Ticker(ticker)
            
            # Try to get basic info
            info = ticker_obj.info
            if info:
                results[ticker] = {
                    "status": "success",
                    "info_keys": len(info.keys()),
                    "name": info.get('longName', 'Unknown')
                }
                print(f"    ✅ {ticker}: Success ({len(info.keys())} info fields)")
            else:
                results[ticker] = {
                    "status": "failed",
                    "error": "No info returned"
                }
                print(f"    ❌ {ticker}: No info returned")
                
        except Exception as e:
            results[ticker] = {
                "status": "failed",
                "error": str(e)
            }
            print(f"    ❌ {ticker}: {str(e)}")
        
        # Add delay to avoid rate limiting
        time.sleep(1)
    
    return results

def test_alpha_vantage_api():
    """Test Alpha Vantage API connectivity (if configured)."""
    print("🔍 Testing Alpha Vantage API connectivity...")
    
    # Check if API key is configured
    config_file = Path("config/settings.yaml")
    if not config_file.exists():
        print("  ⚠️ No config file found")
        return {"status": "no_config"}
    
    try:
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        api_key = config.get('alpha_vantage', {}).get('api_key')
        if not api_key:
            print("  ⚠️ No Alpha Vantage API key found in config")
            return {"status": "no_api_key"}
        
        # Test API call
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={api_key}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if "Error Message" in data:
                return {
                    "status": "failed",
                    "error": data["Error Message"]
                }
            else:
                return {
                    "status": "success",
                    "response_keys": list(data.keys())
                }
        else:
            return {
                "status": "failed",
                "error": f"HTTP {response.status_code}"
            }
            
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }

def analyze_failed_tickers():
    """Analyze the specific tickers that failed in recent runs."""
    print("🔍 Analyzing failed tickers from recent runs...")
    
    # Check recent error logs
    fetch_errors = Path("logs/fetch/dt=2025-07-28/errors.json")
    if fetch_errors.exists():
        with open(fetch_errors, 'r') as f:
            errors = json.load(f)
        
        print(f"  Found {len(errors)} failed tickers:")
        for error in errors:
            ticker = error.get("ticker")
            error_msg = error.get("error")
            print(f"    ❌ {ticker}: {error_msg}")
    
    # Check features errors
    features_meta = Path("logs/features/dt=2025-07-28/metadata.json")
    if features_meta.exists():
        with open(features_meta, 'r') as f:
            meta = json.load(f)
        
        failed_tickers = meta.get("failed_tickers", [])
        if failed_tickers:
            print(f"  Failed tickers in features processing: {failed_tickers}")

def check_rate_limiting():
    """Check for rate limiting issues."""
    print("🔍 Checking for rate limiting issues...")
    
    # Check recent metadata for rate limit hits
    fetch_meta = Path("logs/fetch/dt=2025-07-28/metadata.json")
    if fetch_meta.exists():
        with open(fetch_meta, 'r') as f:
            meta = json.load(f)
        
        rate_limit_hits = meta.get("rate_limit_hits", 0)
        total_sleep_time = meta.get("total_sleep_time", 0)
        
        print(f"  Rate limit hits: {rate_limit_hits}")
        print(f"  Total sleep time: {total_sleep_time} seconds")
        
        if rate_limit_hits > 0:
            print("  ⚠️ Rate limiting detected - consider increasing delays")
        if total_sleep_time > 300:  # 5 minutes
            print("  ⚠️ Excessive sleep time - API may be slow")

def generate_recommendations(results):
    """Generate recommendations based on test results."""
    print("\n🎯 API Connectivity Recommendations:")
    
    yfinance_results = results.get("yfinance", {})
    alpha_vantage_results = results.get("alpha_vantage", {})
    
    # YFinance recommendations
    if yfinance_results:
        failed_count = sum(1 for r in yfinance_results.values() if r.get("status") == "failed")
        if failed_count > 0:
            print("  🔧 YFinance Issues:")
            print("    - Some tickers failing (BF.B, BRK.B are known problematic)")
            print("    - Consider implementing retry logic with exponential backoff")
            print("    - Add fallback data sources for failed tickers")
    
    # Alpha Vantage recommendations
    if alpha_vantage_results.get("status") == "failed":
        print("  🔧 Alpha Vantage Issues:")
        print("    - API connectivity problems detected")
        print("    - Check API key validity and rate limits")
        print("    - Consider upgrading to paid plan for higher limits")
    
    # General recommendations
    print("  🔧 General Improvements:")
    print("    - Implement circuit breaker pattern for API failures")
    print("    - Add health checks before starting data fetch")
    print("    - Consider caching successful responses")
    print("    - Implement graceful degradation for partial failures")

def main():
    """Main function to investigate API issues."""
    print("🚀 Starting API connectivity investigation...\n")
    
    results = {}
    
    # Test YFinance API
    results["yfinance"] = test_yfinance_api()
    print()
    
    # Test Alpha Vantage API
    results["alpha_vantage"] = test_alpha_vantage_api()
    print()
    
    # Analyze failed tickers
    analyze_failed_tickers()
    print()
    
    # Check rate limiting
    check_rate_limiting()
    print()
    
    # Generate recommendations
    generate_recommendations(results)
    
    print("\n✅ API investigation completed")

if __name__ == "__main__":
    main() 