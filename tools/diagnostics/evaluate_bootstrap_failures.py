#!/usr/bin/env python3
"""
Evaluate Bootstrap Failures
Analyze the bootstrap job failures and identify root causes.
"""

import yfinance as yf
import pandas as pd
import time
from pathlib import Path
import json

def test_specific_tickers():
    """Test specific tickers that are failing in the bootstrap."""
    print("🔍 Testing specific failing tickers...")
    
    # Test some of the failing tickers from the log
    failing_tickers = [
        "ALGN", "ALLE", "LNT", "ALL", "GOOGL", "GOOG", "MO", "AMZN", 
        "AMCR", "AEE", "AEP", "AXP", "AIG", "AMT", "AWK", "AMP", "AME", 
        "AMGN", "APH", "ADI", "AON", "APA", "APO", "AAPL", "AMAT", 
        "APTV", "ACGL", "ADM", "ANET", "AJG", "AIZ", "T", "ATO", "ADSK", 
        "ADP", "AZO", "AVB", "AVY", "AXON", "BKR", "BALL", "BAC", "BAX", 
        "BDX", "BRK.B", "BBY", "TECH", "BIIB", "BLK", "BX", "XYZ", "BK", 
        "BA", "BKNG", "BSX", "BMY", "AVGO", "BR", "BRO", "BF.B", "BLDR", 
        "BG", "BXP", "CHRW", "CDNS", "CZR", "CPT", "CPB", "COF", "CAH", 
        "KMX", "CCL", "CARR", "CAT", "CBOE", "CBRE", "CDW", "COR", "CNC", 
        "CNP", "CF", "CRL", "SCHW", "CHTR", "CVX", "CMG", "CB", "CHD", 
        "CI", "CINF", "CTAS", "CSCO", "C", "CFG", "CLX", "CME", "CMS", 
        "KO", "CTSH", "COIN", "CL", "CMCSA", "CAG", "COP"
    ]
    
    results = {}
    success_count = 0
    failure_count = 0
    
    print(f"Testing {len(failing_tickers)} tickers...")
    
    for i, ticker in enumerate(failing_tickers[:20]):  # Test first 20 for speed
        try:
            print(f"  Testing {ticker} ({i+1}/20)...")
            ticker_obj = yf.Ticker(ticker)
            
            # Try to get historical data
            hist = ticker_obj.history(period="1d")
            
            if not hist.empty:
                results[ticker] = {
                    "status": "success",
                    "rows": len(hist),
                    "columns": list(hist.columns)
                }
                success_count += 1
                print(f"    ✅ {ticker}: Success ({len(hist)} rows)")
            else:
                results[ticker] = {
                    "status": "failed",
                    "error": "Empty history"
                }
                failure_count += 1
                print(f"    ❌ {ticker}: Empty history")
                
        except Exception as e:
            results[ticker] = {
                "status": "failed",
                "error": str(e)
            }
            failure_count += 1
            print(f"    ❌ {ticker}: {str(e)}")
        
        # Add delay to avoid rate limiting
        time.sleep(0.5)
    
    print(f"\n📊 Results: {success_count} success, {failure_count} failures")
    return results

def check_yfinance_api_status():
    """Check if yfinance API is working properly."""
    print("🔍 Checking yfinance API status...")
    
    try:
        # Test with a known working ticker
        ticker = yf.Ticker("AAPL")
        info = ticker.info
        
        if info and len(info) > 0:
            print("✅ yfinance API is working")
            print(f"   AAPL info fields: {len(info)}")
            return True
        else:
            print("❌ yfinance API returned empty info")
            return False
            
    except Exception as e:
        print(f"❌ yfinance API error: {e}")
        return False

def analyze_bootstrap_config():
    """Analyze the bootstrap configuration."""
    print("🔍 Analyzing bootstrap configuration...")
    
    bootstrap_file = Path("bootstrap_historical_data.py")
    if not bootstrap_file.exists():
        print("❌ bootstrap_historical_data.py not found")
        return
    
    with open(bootstrap_file, 'r') as f:
        content = f.read()
    
    # Look for key configuration parameters
    if "period=" in content:
        print("✅ Period parameter found in bootstrap")
    else:
        print("❌ No period parameter found")
    
    if "yfinance" in content:
        print("✅ yfinance import found")
    else:
        print("❌ yfinance import not found")

def check_network_connectivity():
    """Check network connectivity to Yahoo Finance."""
    print("🔍 Checking network connectivity...")
    
    try:
        import requests
        
        # Test basic connectivity
        response = requests.get("https://finance.yahoo.com", timeout=10)
        if response.status_code == 200:
            print("✅ Yahoo Finance website accessible")
        else:
            print(f"❌ Yahoo Finance website returned {response.status_code}")
            
        # Test API endpoint
        response = requests.get("https://query1.finance.yahoo.com", timeout=10)
        if response.status_code == 200:
            print("✅ Yahoo Finance API accessible")
        else:
            print(f"❌ Yahoo Finance API returned {response.status_code}")
            
    except Exception as e:
        print(f"❌ Network connectivity error: {e}")

def generate_recommendations(results):
    """Generate recommendations based on analysis."""
    print("\n🎯 Bootstrap Failure Recommendations:")
    
    # Check if yfinance is working
    if check_yfinance_api_status():
        print("  ✅ yfinance API is working - issue may be in bootstrap logic")
        print("  🔧 Recommendations:")
        print("    - Check bootstrap script for incorrect period parameters")
        print("    - Verify ticker symbol formatting")
        print("    - Add retry logic for failed requests")
        print("    - Implement exponential backoff")
        print("    - Add better error handling")
    else:
        print("  ❌ yfinance API issues detected")
        print("  🔧 Recommendations:")
        print("    - Check network connectivity")
        print("    - Verify yfinance package version")
        print("    - Consider alternative data sources")
        print("    - Add API health checks")

def main():
    """Main function to evaluate bootstrap failures."""
    print("🚀 Evaluating bootstrap failures...\n")
    
    # Check network connectivity
    check_network_connectivity()
    print()
    
    # Check yfinance API status
    check_yfinance_api_status()
    print()
    
    # Analyze bootstrap configuration
    analyze_bootstrap_config()
    print()
    
    # Test specific failing tickers
    results = test_specific_tickers()
    print()
    
    # Generate recommendations
    generate_recommendations(results)
    
    print("\n✅ Bootstrap evaluation completed")

if __name__ == "__main__":
    main() 