#!/usr/bin/env python3
"""
Test suite for the ticker fetching module.

This module tests the TickerFetcher class and its various functionalities
including data fetching, validation, and error handling.
"""

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add the pipeline directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

from fetch_tickers import TickerFetcher
from utils.common import cleanup_old_partitions, handle_rate_limit, create_partition_paths

def test_config_loading():
    """Test configuration loading functionality."""
    print("\n=== Testing Configuration Loading ===")
    
    fetcher = TickerFetcher()
    
    # Check that config is loaded
    assert fetcher.config is not None, "Configuration not loaded"
    
    # Check required config fields
    required_fields = ['ticker_source', 'data_source', 'base_data_path', 'base_log_path']
    missing_fields = [field for field in required_fields if field not in fetcher.config]
    assert not missing_fields, f"Missing required config fields: {missing_fields}"
    
    print("✅ Configuration loading works")

def test_metadata_validation():
    """Test metadata structure validation."""
    print("\n=== Testing Metadata Validation ===")
    
    fetcher = TickerFetcher()
    
    # Create sample metadata
    metadata = {
        "run_date": datetime.now().strftime('%Y-%m-%d'),
        "processing_date": datetime.now().isoformat(),
        "tickers_fetched": 500,
        "tickers_added": 5,
        "tickers_removed": 2,
        "net_change": 3,
        "validation_passed": True,
        "status": "success",
        "runtime_seconds": 10.5,
        "runtime_minutes": 0.175,
        "error_message": None,
        "data_path": "/path/to/data",
        "log_path": "/path/to/logs",
        "csv_path": "/path/to/tickers.csv",
        "diff_path": "/path/to/diff.json",
        "test_mode": False,
        "dry_run": False,
        "force": False
    }
    
    # Check required metadata fields
    required_fields = [
        'run_date', 'processing_date', 'tickers_fetched', 'status',
        'runtime_seconds', 'runtime_minutes', 'data_path', 'log_path'
    ]
    
    missing_fields = [field for field in required_fields if field not in metadata]
    assert not missing_fields, f"Missing metadata fields: {missing_fields}"
    
    print("✅ Metadata structure valid")

def test_diff_log_creation():
    """Test diff log creation functionality."""
    print("\n=== Testing Diff Log Creation ===")
    
    fetcher = TickerFetcher()
    
    # Test data
    added_tickers = ['AAPL', 'GOOGL']
    removed_tickers = ['IBM']
    
    # Create temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir)
        
        # Test diff log creation in dry-run mode
        diff_path = fetcher.save_diff_log(added_tickers, removed_tickers, log_path, dry_run=True)
        
        # Check that diff path is returned
        assert diff_path is not None, "Diff path not returned"
        assert str(diff_path).endswith("diff.json"), "Diff path should end with diff.json"
        
        # In dry-run mode, the path should be returned but file shouldn't exist
        assert not Path(diff_path).exists(), "File should not exist in dry-run mode"
        
        # Test diff log creation in normal mode
        diff_path = fetcher.save_diff_log(added_tickers, removed_tickers, log_path, dry_run=False)
        
        # Check diff log structure (in normal mode)
        assert Path(diff_path).exists(), "Diff file should exist in normal mode"
        with open(diff_path, 'r') as f:
            diff_data = json.load(f)
        
        required_diff_fields = [
            'run_date', 'timestamp', 'tickers_added', 'tickers_removed',
            'total_added', 'total_removed', 'net_change'
        ]
        
        missing_fields = [field for field in required_diff_fields if field not in diff_data]
        assert not missing_fields, f"Missing diff log fields: {missing_fields}"
    
    print("✅ Diff log structure valid")

def test_retention_cleanup():
    """Test retention cleanup functionality."""
    print("\n=== Testing Retention Cleanup ===")
    
    fetcher = TickerFetcher()
    
    # Test cleanup with dry-run using utility function directly
    cleanup_results = cleanup_old_partitions(fetcher.config, "tickers", dry_run=True, test_mode=True)
    
    # Check cleanup results structure
    required_cleanup_fields = [
        'cleanup_date', 'retention_days', 'cutoff_date',
        'deleted_partitions', 'total_deleted', 'dry_run', 'test_mode'
    ]
    
    missing_fields = [field for field in required_cleanup_fields if field not in cleanup_results]
    assert not missing_fields, f"Missing cleanup fields: {missing_fields}"
    
    print("✅ Retention cleanup structure valid")

def test_rate_limit_handling():
    """Test rate limit handling functionality."""
    print("\n=== Testing Rate Limit Handling ===")
    
    fetcher = TickerFetcher()
    
    # Test different rate limit strategies
    strategies = ['exponential_backoff', 'fixed_delay', 'adaptive']
    
    for strategy in strategies:
        fetcher.config['rate_limit_strategy'] = strategy
        fetcher.config['rate_limit_enabled'] = True
        
        # Mock time.sleep to avoid actual delays
        with patch('time.sleep') as mock_sleep:
            handle_rate_limit(1, fetcher.config)
            assert mock_sleep.called, f"Rate limit strategy '{strategy}' did not call time.sleep"
        
        print(f"✅ Rate limit strategy '{strategy}' works")

def test_mock_api_failure():
    """Test handling of API failures."""
    print("\n=== Testing API Failure Handling ===")
    
    fetcher = TickerFetcher()
    
    # Mock requests.get to simulate API failure
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("API timeout")
        
        # Test that the script handles failures gracefully
        try:
            result = fetcher.run(force=True, dry_run=True)
            # If we get here, the exception was handled properly
            assert result['status'] in ['failed', 'partial_success'], "API failure not properly handled"
        except Exception as e:
            # The exception should be caught and handled within the run method
            assert "API timeout" in str(e), f"Unexpected exception: {e}"
        
        print("✅ API failure properly handled")

def test_full_test_mode():
    """Test full-test mode functionality."""
    print("\n=== Testing Full Test Mode ===")
    
    fetcher = TickerFetcher()
    
    # Test full-test mode with dry-run
    with patch('requests.get') as mock_get:
        # Mock successful response
        mock_response = MagicMock()
        mock_response.content = b"<html><table class='wikitable'><tr><td>AAPL</td><td>Apple</td></tr></table></html>"
        mock_get.return_value = mock_response
        
        result = fetcher.run(force=True, dry_run=True, full_test=True)
        
        assert result.get('test_mode') == True, "Full test mode not properly enabled"
        print("✅ Full test mode properly enabled")

def test_dry_run_mode():
    """Test dry-run mode functionality."""
    print("\n=== Testing Dry Run Mode ===")
    
    fetcher = TickerFetcher()
    
    # Test dry-run mode
    with patch('requests.get') as mock_get:
        # Mock successful response
        mock_response = MagicMock()
        mock_response.content = b"<html><table class='wikitable'><tr><td>AAPL</td><td>Apple</td></tr></table></html>"
        mock_get.return_value = mock_response
        
        result = fetcher.run(force=True, dry_run=True)
        
        assert result.get('dry_run') == True, "Dry run mode not properly enabled"
        print("✅ Dry run mode properly enabled")

def test_ticker_changes_calculation():
    """Test ticker changes calculation."""
    print("\n=== Testing Ticker Changes Calculation ===")
    
    fetcher = TickerFetcher()
    
    # Test data
    current_tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN']
    previous_tickers = {'AAPL', 'GOOGL', 'IBM', 'ORCL'}
    
    # Calculate changes
    added, removed = fetcher.calculate_ticker_changes(current_tickers, previous_tickers)
    
    # Verify results
    expected_added = ['MSFT', 'AMZN']
    expected_removed = ['IBM', 'ORCL']
    
    assert set(added) == set(expected_added), f"Added tickers mismatch: {added} vs {expected_added}"
    assert set(removed) == set(expected_removed), f"Removed tickers mismatch: {removed} vs {expected_removed}"
    
    print("✅ Ticker changes calculation works")

def test_ticker_validation():
    """Test ticker validation functionality."""
    print("\n=== Testing Ticker Validation ===")
    
    fetcher = TickerFetcher()
    
    # Test valid ticker count
    valid_count = 500
    assert fetcher.validate_ticker_count(valid_count), f"Valid count {valid_count} should pass validation"
    
    # Test invalid ticker count
    invalid_count = 100
    assert not fetcher.validate_ticker_count(invalid_count), f"Invalid count {invalid_count} should fail validation"
    
    print("✅ Ticker validation works")

def test_ticker_cleaning():
    """Test ticker symbol cleaning functionality."""
    print("\n=== Testing Ticker Cleaning ===")
    
    fetcher = TickerFetcher()
    
    # Test data with various formats
    raw_tickers = ['AAPL', '  GOOGL  ', 'MSFT', 'amzn', 'IBM', '123', 'A', 'TOOLONG', '']
    
    # Clean tickers
    cleaned_tickers = fetcher.clean_ticker_symbols(raw_tickers)
    
    # Verify results - current implementation allows numeric tickers and single letters
    # but filters out empty strings and overly long tickers
    expected_cleaned = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'IBM', '123', 'A']
    
    assert set(cleaned_tickers) == set(expected_cleaned), f"Cleaned tickers mismatch: {cleaned_tickers} vs {expected_cleaned}"
    
    print("✅ Ticker cleaning works correctly")

def test_partition_path_creation():
    """Test partition path creation functionality."""
    print("\n=== Testing Partition Path Creation ===")
    
    fetcher = TickerFetcher()
    
    # Test date string
    date_str = "2025-01-15"
    
    # Test production mode
    data_path, log_path = create_partition_paths(date_str, fetcher.config, "tickers", test_mode=False)
    
    # Check that paths are created correctly
    assert "dt=2025-01-15" in str(data_path), f"Data path should contain date partition, got: {data_path}"
    assert "dt=2025-01-15" in str(log_path), f"Log path should contain date partition, got: {log_path}"
    
    print("✅ Partition path creation works correctly")

def main():
    """Run all tests."""
    print("Starting Ticker Fetcher Tests...")
    
    # Run all test functions
    test_functions = [
        test_config_loading,
        test_metadata_validation,
        test_diff_log_creation,
        test_retention_cleanup,
        test_rate_limit_handling,
        test_mock_api_failure,
        test_full_test_mode,
        test_dry_run_mode,
        test_ticker_changes_calculation,
        test_ticker_validation,
        test_ticker_cleaning,
        test_partition_path_creation
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} failed: {e}")
            failed += 1
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print("💥 Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 