#!/usr/bin/env python3
"""
Test script for refactored bootstrap scripts.

Verifies that the refactored scripts maintain the same functionality
as the original implementations.
"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from base_bootstrapper import BaseBootstrapper
from bootstrap_utils import (
    create_common_parser, get_api_key_from_config, get_tickers_from_args,
    load_config, setup_logging, validate_tickers
)


class TestBootstrapper(BaseBootstrapper):
    """Test implementation of BaseBootstrapper for testing."""
    
    def __init__(self, output_dir: Path, batch_size: int = 10):
        super().__init__(output_dir, batch_size, rate_limit_delay=0.1)
        self.fetch_calls = []
        self.save_calls = []
    
    def fetch_historical_data(self, ticker: str):
        """Mock implementation that returns test data."""
        self.fetch_calls.append(ticker)
        
        import pandas as pd
        from datetime import datetime, timedelta
        
        # Create test data
        dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
        data = {
            'date': dates,
            'open': [100.0] * len(dates),
            'high': [105.0] * len(dates),
            'low': [95.0] * len(dates),
            'close': [102.0] * len(dates),
            'volume': [1000000] * len(dates),
            'ticker': [ticker] * len(dates)
        }
        return pd.DataFrame(data)
    
    def save_ticker_data(self, ticker: str, df):
        """Mock implementation that tracks save calls."""
        self.save_calls.append((ticker, len(df)))
        return True


def test_base_bootstrapper():
    """Test the BaseBootstrapper functionality."""
    print("Testing BaseBootstrapper...")
    
    # Create test bootstrapper
    output_dir = Path("/tmp/test_bootstrap")
    bootstrapper = TestBootstrapper(output_dir, batch_size=2)
    
    # Test with sample tickers
    test_tickers = ["AAPL", "MSFT", "GOOGL"]
    
    # Run bootstrap
    summary = bootstrapper.run(test_tickers)
    
    # Verify results
    assert summary["bootstrap_summary"]["total_tickers"] == 3
    assert summary["bootstrap_summary"]["successful_tickers"] == 3
    assert summary["bootstrap_summary"]["failed_tickers"] == 0
    assert len(bootstrapper.fetch_calls) == 3
    assert len(bootstrapper.save_calls) == 3
    
    print("✅ BaseBootstrapper test passed")


def test_bootstrap_utils():
    """Test the bootstrap utilities."""
    print("Testing bootstrap utilities...")
    
    # Test argument parser
    parser = create_common_parser("Test parser")
    assert parser is not None
    
    # Test ticker validation
    assert validate_tickers(["AAPL", "MSFT"]) == True
    assert validate_tickers([]) == False
    assert validate_tickers("not_a_list") == False
    
    # Test logging setup
    logger = setup_logging("INFO", False)
    assert logger is not None
    
    print("✅ Bootstrap utilities test passed")


def test_error_handling():
    """Test error handling in the base bootstrapper."""
    print("Testing error handling...")
    
    class ErrorBootstrapper(BaseBootstrapper):
        def __init__(self, output_dir: Path, batch_size: int = 10):
            super().__init__(output_dir, batch_size, rate_limit_delay=0.1)
        
        def fetch_historical_data(self, ticker: str):
            if ticker == "ERROR":
                raise Exception("Test error")
            elif ticker == "AAPL":
                # Return valid data for AAPL
                import pandas as pd
                dates = pd.date_range(start='2023-01-01', end='2023-01-10', freq='D')
                data = {
                    'date': dates,
                    'open': [100.0] * len(dates),
                    'high': [105.0] * len(dates),
                    'low': [95.0] * len(dates),
                    'close': [102.0] * len(dates),
                    'volume': [1000000] * len(dates),
                    'ticker': [ticker] * len(dates)
                }
                return pd.DataFrame(data)
            else:
                return None
        
        def save_ticker_data(self, ticker: str, df):
            if ticker == "SAVE_ERROR":
                return False
            return True
    
    # Create test bootstrapper
    output_dir = Path("/tmp/test_error_bootstrap")
    bootstrapper = ErrorBootstrapper(output_dir)
    
    # Test with problematic tickers
    test_tickers = ["AAPL", "ERROR", "SAVE_ERROR", "MSFT"]
    
    # Run bootstrap
    summary = bootstrapper.run(test_tickers)
    
    # Verify error handling
    assert summary["bootstrap_summary"]["total_tickers"] == 4
    assert summary["bootstrap_summary"]["failed_tickers"] == 3
    assert summary["bootstrap_summary"]["successful_tickers"] == 1
    
    print("✅ Error handling test passed")


def main():
    """Run all tests."""
    print("Running refactored bootstrap tests...")
    print("=" * 50)
    
    try:
        test_base_bootstrapper()
        test_bootstrap_utils()
        test_error_handling()
        
        print("=" * 50)
        print("✅ All tests passed! Refactoring is working correctly.")
        return 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 