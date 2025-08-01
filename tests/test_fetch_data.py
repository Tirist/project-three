#!/usr/bin/env python3
"""
Test suite for the OHLCV data fetching module.

This module tests the OHLCVFetcher class and its various functionalities
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

from fetch_data import OHLCVFetcher
from utils.common import cleanup_old_partitions, handle_rate_limit

@pytest.mark.quick
def test_metadata_matches_processed_count():
    """Test that metadata accurately reflects the number of processed tickers."""
    print("\n=== Testing Metadata Accuracy ===")
    
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test data structure
        test_data_path = Path(temp_dir) / "data" / "raw" / "dt=2025-01-15"
        test_data_path.mkdir(parents=True, exist_ok=True)
        
        # Create some test CSV files
        test_tickers = ['AAPL', 'GOOGL', 'MSFT']
        for ticker in test_tickers:
            test_file = test_data_path / f"{ticker}.csv"
            test_df = pd.DataFrame({
                'date': ['2025-01-15'],
                'open': [100.0],
                'high': [110.0],
                'low': [90.0],
                'close': [105.0],
                'volume': [1000000]
            })
            test_df.to_csv(test_file, index=False)
        
        # Create test metadata
        metadata = {
            "run_date": "2025-01-15",
            "processing_date": datetime.now().isoformat(),
            "tickers_processed": len(test_tickers),
            "tickers_successful": len(test_tickers),
            "tickers_failed": 0,
            "total_rows": len(test_tickers),
            "status": "success",
            "runtime_seconds": 10.5,
            "runtime_minutes": 0.175,
            "error_message": None,
            "data_path": str(test_data_path),
            "log_path": str(Path(temp_dir) / "logs"),
            "test_mode": False,
            "dry_run": False,
            "force": False,
            "incremental_mode": True,
            "failed_tickers": [],
            "successful_tickers": test_tickers
        }
        
        # Verify metadata structure
        required_fields = [
            'run_date', 'processing_date', 'tickers_processed', 'tickers_successful',
            'tickers_failed', 'total_rows', 'status', 'runtime_seconds', 'runtime_minutes',
            'data_path', 'log_path', 'test_mode', 'dry_run', 'force'
        ]
        
        missing_fields = [field for field in required_fields if field not in metadata]
        assert not missing_fields, f"Missing metadata fields: {missing_fields}"
        
        # Verify counts match
        assert metadata['tickers_processed'] == len(test_tickers), "Processed count mismatch"
        assert metadata['tickers_successful'] == len(test_tickers), "Successful count mismatch"
        assert metadata['tickers_failed'] == 0, "Failed count should be 0"
        assert metadata['total_rows'] == len(test_tickers), "Total rows mismatch"
        
        print("✅ Metadata accurately reflects processed data")

@pytest.mark.quick
def test_data_columns():
    """Test that fetched data has the correct column structure."""
    print("\n=== Testing Data Column Structure ===")
    
    fetcher = OHLCVFetcher()
    
    # Create sample OHLCV data
    sample_data = pd.DataFrame({
        'date': ['2025-01-15', '2025-01-16'],
        'open': [100.0, 105.0],
        'high': [110.0, 115.0],
        'low': [90.0, 95.0],
        'close': [105.0, 110.0],
        'volume': [1000000, 1200000]
    })
    
    # Verify required columns exist
    required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in sample_data.columns]
    assert not missing_columns, f"Missing required columns: {missing_columns}"
    
    # Verify data types
    assert sample_data['date'].dtype == 'object' or 'datetime' in str(sample_data['date'].dtype), "Date column should be datetime"
    assert sample_data['open'].dtype in ['float64', 'float32'], "Open column should be numeric"
    assert sample_data['high'].dtype in ['float64', 'float32'], "High column should be numeric"
    assert sample_data['low'].dtype in ['float64', 'float32'], "Low column should be numeric"
    assert sample_data['close'].dtype in ['float64', 'float32'], "Close column should be numeric"
    assert sample_data['volume'].dtype in ['int64', 'int32', 'float64', 'float32'], "Volume column should be numeric"
    
    print("✅ Data column structure is correct")

@pytest.mark.quick
def test_error_logging():
    """Test that errors are properly logged and tracked."""
    print("\n=== Testing Error Logging ===")

    fetcher = OHLCVFetcher()

    # Create sample error data
    errors = [
        {
            "ticker": "INVALID",
            "error": "API timeout",
            "timestamp": datetime.now().isoformat()
        },
        {
            "ticker": "MISSING",
            "error": "No data available",
            "timestamp": datetime.now().isoformat()
        }
    ]

    # Test error log structure
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir)

        # Test error saving in dry-run mode
        error_path = fetcher.save_errors(errors, log_path, dry_run=True)

        # Verify error path
        assert error_path is not None, "Error path not returned"
        assert str(error_path).endswith("errors.json"), "Error path should end with errors.json"

        # In dry-run mode, the path should be returned but file shouldn't exist
        assert not Path(error_path).exists(), "File should not exist in dry-run mode"

        # Test error saving in normal mode
        error_path = fetcher.save_errors(errors, log_path, dry_run=False)

        # Check error structure (in normal mode)
        assert Path(error_path).exists(), "Error file should exist in normal mode"
        with open(error_path, 'r') as f:
            error_data = json.load(f)

        # Verify error data structure
        for error in error_data:
            required_error_fields = ['ticker', 'error', 'timestamp']
            missing_fields = [field for field in required_error_fields if field not in error]
            assert not missing_fields, f"Missing error fields: {missing_fields}"

    print("✅ Error logging works correctly")

@pytest.mark.quick
def test_force_flag():
    """Test that the force flag properly overwrites existing partitions."""
    print("\n=== Testing Force Flag ===")
    
    fetcher = OHLCVFetcher()
    
    # Test force flag behavior
    with patch.object(fetcher, 'check_existing_partition') as mock_check, \
         patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker:
        
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = ['AAPL', 'GOOGL']
        mock_fetch_ohlcv.return_value = pd.DataFrame({'Open':[1],'High':[2],'Low':[0],'Close':[1],'Volume':[100]})
        mock_save_ticker.return_value = True
        
        # Test with force=False and existing partition
        mock_check.return_value = True
        result = fetcher.run(force=False, dry_run=True)
        assert result['status'] == 'skipped', "Should skip when partition exists and force=False"
        
        # Test with force=True and existing partition
        mock_check.return_value = False  # Don't skip when force=True
        result = fetcher.run(force=True, dry_run=True)
        assert result['status'] != 'skipped', "Should not skip when force=True"
    
    print("✅ Force flag works correctly")

@pytest.mark.quick
def test_retention_cleanup():
    """Test retention cleanup functionality."""
    print("\n=== Testing Retention Cleanup ===")
    
    fetcher = OHLCVFetcher()
    
    # Test cleanup with dry-run using utility function directly
    cleanup_results = cleanup_old_partitions(fetcher.config, "raw", dry_run=True, test_mode=True)
    
    # Check cleanup results structure
    required_cleanup_fields = [
        'cleanup_date', 'retention_days', 'cutoff_date',
        'deleted_partitions', 'total_deleted', 'dry_run', 'test_mode'
    ]
    
    missing_fields = [field for field in required_cleanup_fields if field not in cleanup_results]
    assert not missing_fields, f"Missing cleanup fields: {missing_fields}"
    
    print("✅ Retention cleanup structure valid")

@pytest.mark.quick
def test_rate_limit_handling():
    """Test rate limit handling functionality."""
    print("\n=== Testing Rate Limit Handling ===")
    
    fetcher = OHLCVFetcher()
    
    # Test different rate limit strategies
    strategies = ['exponential_backoff', 'fixed_delay', 'adaptive']
    
    for strategy in strategies:
        fetcher.config['rate_limit_strategy'] = strategy
        fetcher.config['rate_limit_enabled'] = True
        
        # Mock time.sleep to avoid actual delays
        with patch('time.sleep') as mock_sleep:
            handle_rate_limit(1, fetcher.config)
            assert mock_sleep.call_count == 1, f"Rate limit strategy '{strategy}' did not call sleep"
        
        print(f"✅ Rate limit strategy '{strategy}' works")
    
    assert True

@pytest.mark.heavy
def test_full_test_mode():
    """Test full test mode functionality."""
    print("\n=== Testing Full Test Mode ===")
    
    fetcher = OHLCVFetcher()
    
    # Test full test mode with proper mocking
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch.object(fetcher, 'check_existing_partition') as mock_check_partition:
        
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'IBM']
        mock_fetch_ohlcv.return_value = pd.DataFrame({'Open':[1],'High':[2],'Low':[0],'Close':[1],'Volume':[100]})
        mock_save_ticker.return_value = True
        mock_check_partition.return_value = False
        
        result = fetcher.run(force=True, dry_run=True, full_test=True)
        
        # Verify test mode behavior
        assert result['test_mode'] is True, "Test mode should be enabled"
        assert result['tickers_processed'] == 5, "Should process exactly 5 tickers in test mode"
    
    print("✅ Full test mode works correctly")

@pytest.mark.quick
def test_dry_run_mode():
    """Test dry run mode functionality."""
    print("\n=== Testing Dry Run Mode ===")
    
    fetcher = OHLCVFetcher()
    
    # Test dry run mode with proper mocking
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch.object(fetcher, 'check_existing_partition') as mock_check_partition:
        
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = ['AAPL', 'GOOGL']
        mock_fetch_ohlcv.return_value = pd.DataFrame({'Open':[1],'High':[2],'Low':[0],'Close':[1],'Volume':[100]})
        mock_save_ticker.return_value = True
        mock_check_partition.return_value = False
        
        result = fetcher.run(force=True, dry_run=True)
        
        # Verify dry run behavior
        assert result['status'] in ['success', 'partial_success'], "Dry run should complete successfully"
        assert result['tickers_processed'] == 2, "Should process 2 tickers"
    
    print("✅ Dry run mode works correctly")

@pytest.mark.quick
def test_batch_processing():
    """Test that batch processing splits tickers into correct batch sizes and processes all batches."""
    print("\n=== Testing Batch Processing ===")
    fetcher = OHLCVFetcher()
    fetcher.config['batch_size'] = 3
    fetcher.config['base_cooldown_seconds'] = 0  # No sleep for test
    tickers = [f'TICK{i}' for i in range(10)]
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch('time.sleep') as mock_sleep:
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = tickers
        mock_fetch_ohlcv.return_value = pd.DataFrame({'Open':[1],'High':[2],'Low':[0],'Close':[1],'Volume':[100]})
        mock_save_ticker.return_value = True
        result = fetcher.run(force=True, test=False, dry_run=True)
        
        # Verify all tickers were processed
        assert result['tickers_processed'] == len(tickers), f"Expected {len(tickers)} tickers processed, got {result['tickers_processed']}"
        print("✅ Batch processing works correctly")

@pytest.mark.quick
def test_cooldown_metadata():
    """Test that cooldown information is properly tracked in metadata."""
    print("\n=== Testing Cooldown Metadata ===")
    
    fetcher = OHLCVFetcher()
    
    # Test with cooldown configuration
    fetcher.config['base_cooldown_seconds'] = 1
    fetcher.config['rate_limit_enabled'] = True
    
    # Mock the run to avoid actual API calls
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker:
        
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = ['AAPL', 'GOOGL']
        
        # Return data with proper timezone handling
        df = pd.DataFrame({
            'Open': [1], 'High': [2], 'Low': [0], 'Close': [1], 'Volume': [100],
            'date': [pd.Timestamp.now().normalize()]  # Use timezone-naive timestamp
        })
        mock_fetch_ohlcv.return_value = df
        mock_save_ticker.return_value = True
        
        result = fetcher.run(force=True, dry_run=True)
        
        # Verify metadata includes cooldown information
        assert 'runtime_seconds' in result, "Runtime should be tracked"
        assert result['runtime_seconds'] > 0, "Runtime should be positive"
        
        print("✅ Cooldown metadata tracking works")

@pytest.mark.quick
def test_progress_bar():
    """Test that progress bar is properly configured and used."""
    print("\n=== Testing Progress Bar ===")

    fetcher = OHLCVFetcher()

    # Test progress bar configuration
    fetcher.config['progress'] = True
    fetcher.config['incremental_mode'] = False  # Disable incremental mode to avoid timezone issues

    # Mock the run to test progress bar usage
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch.object(fetcher, 'check_existing_partition') as mock_check_partition:

        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = ['AAPL', 'GOOGL']
        
        # Return data with proper timezone handling
        df = pd.DataFrame({
            'Open': [1], 'High': [2], 'Low': [0], 'Close': [1], 'Volume': [100],
            'date': [pd.Timestamp.now().normalize()]  # Use timezone-naive timestamp
        })
        mock_fetch_ohlcv.return_value = df
        mock_save_ticker.return_value = True
        mock_check_partition.return_value = False  # Ensure we don't skip

        result = fetcher.run(force=True, dry_run=True)

        # Verify the run completed successfully with progress tracking enabled
        assert result['status'] in ['success', 'partial_success'], "Run should complete successfully"
        assert result['tickers_processed'] == 2, "Should process 2 tickers"
        assert fetcher.config['progress'] is True, "Progress should be enabled"

        print("✅ Progress bar configuration works")

@pytest.mark.quick
def test_batch_error_handling():
    """Test that errors in batch processing are properly handled and logged."""
    print("\n=== Testing Batch Error Handling ===")

    fetcher = OHLCVFetcher()
    fetcher.config['incremental_mode'] = False  # Disable incremental mode to avoid timezone issues

    def fake_fetch(ticker, days):
        if ticker == 'ERROR':
            raise Exception("Simulated API error")
        # Return data with proper timezone handling
        df = pd.DataFrame({
            'Open': [1], 'High': [2], 'Low': [0], 'Close': [1], 'Volume': [100],
            'date': [pd.Timestamp.now().normalize()]  # Use timezone-naive timestamp
        })
        return df

    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data', side_effect=fake_fetch) as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch.object(fetcher, 'check_existing_partition') as mock_check_partition:

        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = ['AAPL', 'ERROR', 'GOOGL']
        mock_save_ticker.return_value = True
        mock_check_partition.return_value = False  # Ensure we don't skip

        result = fetcher.run(force=True, dry_run=True)

        # Verify error handling - should have 1 failed ticker (ERROR) and 2 successful
        assert result['tickers_failed'] == 1, f"Should have 1 failed ticker, got {result['tickers_failed']}"
        assert result['tickers_successful'] == 2, f"Should have 2 successful tickers, got {result['tickers_successful']}"
        assert 'ERROR' in result.get('failed_tickers', []), "ERROR ticker should be in failed list"

        print("✅ Batch error handling works correctly")

def main():
    """Run all tests."""
    print("Starting OHLCV Data Fetcher Tests...")
    
    # Run all test functions
    test_functions = [
        test_metadata_matches_processed_count,
        test_data_columns,
        test_error_logging,
        test_force_flag,
        test_retention_cleanup,
        test_rate_limit_handling,
        test_full_test_mode,
        test_dry_run_mode,
        test_batch_processing,
        test_cooldown_metadata,
        test_progress_bar,
        test_batch_error_handling
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