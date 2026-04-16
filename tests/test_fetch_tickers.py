#!/usr/bin/env python3
"""
Test script for fetch_tickers.py functionality.
"""

import os
import json
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Add pipeline directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
from fetch_tickers import TickerFetcher

def test_metadata_validation():
    """Test that metadata.json includes all required fields."""
    print("=== Testing Metadata Validation ===")

    fetcher = TickerFetcher()
    with patch('time.sleep'):
        result = fetcher.run(force=True, dry_run=False, test=True)

    metadata_file = Path(result["metadata_path"])
    assert metadata_file.exists(), f"Metadata file not found: {metadata_file}"

    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    # Check for all new required fields
    required_fields = [
        'run_date', 'source_primary', 'source_secondary', 'tickers_fetched',
        'tickers_added', 'tickers_removed', 'skipped_tickers', 'status',
        'runtime_seconds', 'runtime_minutes', 'api_retries', 'rate_limit_hits',
        'rate_limit_strategy', 'error_message', 'full_test_mode', 'dry_run_mode'
    ]
    
    missing_fields = [field for field in required_fields if field not in metadata]
    assert not missing_fields, f"Missing required fields: {missing_fields}"
    
    print("✅ All required metadata fields present")

def test_diff_log_creation():
    """Test that diff.json is created with ticker changes."""
    print("\n=== Testing Diff Log Creation ===")

    fetcher = TickerFetcher()
    with patch('time.sleep'):
        result = fetcher.run(force=True, dry_run=False, test=True)

    diff_file = Path(result["diff_path"])
    assert diff_file.exists(), f"Diff file not found: {diff_file}"

    with open(diff_file, 'r') as f:
        diff_data = json.load(f)
    
    # Check diff structure
    required_diff_fields = [
        'date', 'added', 'removed', 'total_added', 'total_removed'
    ]
    
    missing_fields = [field for field in required_diff_fields if field not in diff_data]
    assert not missing_fields, f"Missing diff fields: {missing_fields}"
    
    print("✅ Diff log structure valid")

def test_retention_cleanup():
    """Test retention cleanup functionality."""
    print("\n=== Testing Retention Cleanup ===")
    
    fetcher = TickerFetcher()
    
    # Test cleanup with dry-run
    cleanup_results = fetcher.cleanup_old_partitions(dry_run=True)
    
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
            fetcher.handle_rate_limit(1)
            assert mock_sleep.called, f"Rate limit strategy '{strategy}' did not call time.sleep"
        
        print(f"✅ Rate limit strategy '{strategy}' works")

def test_mock_api_failure():
    """Test handling of API failures."""
    print("\n=== Testing API Failure Handling ===")
    
    fetcher = TickerFetcher()
    
    # Mock requests.get to simulate API failure
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("API timeout")
        
        # Current behavior is to raise after final retry in fetch.
        with pytest.raises(Exception, match="API timeout"):
            fetcher.run(force=True, dry_run=True)
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
        
        assert result.get('full_test_mode') == True, "Full test mode not properly enabled"
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
        
        assert result.get('dry_run_mode') == True, "Dry run mode not properly enabled"
        print("✅ Dry run mode properly enabled")

def test_ticker_changes_calculation():
    """Test ticker changes calculation."""
    print("\n=== Testing Ticker Changes Calculation ===")
    
    fetcher = TickerFetcher()
    
    # Test ticker changes calculation
    current_tickers = ['AAPL', 'MSFT', 'GOOGL', 'NEW']
    previous_tickers = {'AAPL', 'MSFT', 'OLD'}
    
    added, removed = fetcher.calculate_ticker_changes(current_tickers, previous_tickers)
    
    expected_added = ['GOOGL', 'NEW']
    expected_removed = ['OLD']
    
    assert set(added) == set(expected_added) and set(removed) == set(expected_removed), f"Ticker changes calculation incorrect. Expected: +{expected_added}, -{expected_removed}. Got: +{added}, -{removed}"
    print("✅ Ticker changes calculation correct")

def main():
    """Run all tests."""
    print("🧪 Running fetch_tickers.py Tests\n")
    
    tests = [
        test_metadata_validation,
        test_diff_log_creation,
        test_retention_cleanup,
        test_rate_limit_handling,
        test_mock_api_failure,
        test_full_test_mode,
        test_dry_run_mode,
        test_ticker_changes_calculation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"❌ Test {test.__name__} failed with assertion error: {e}")
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed!")
        return False

if __name__ == "__main__":
    main() 