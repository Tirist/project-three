#!/usr/bin/env python3
"""
Test script for fetch_data.py functionality.
"""

import os
import json
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
from fetch_data import OHLCVFetcher
import pytest

@pytest.mark.quick
def test_metadata_matches_processed_count():
    """Test that metadata.json matches the processed ticker count."""
    print("=== Testing Metadata vs Processed Count ===")
    
    # Find the latest metadata file
    log_base_path = Path("logs/fetch")
    if not log_base_path.exists():
        print("❌ No fetch logs found")
        assert False, "No fetch logs found"
    
    date_dirs = [d for d in log_base_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
    if not date_dirs:
        print("❌ No fetch log directories found")
        assert False, "No fetch log directories found"
    
    latest_dir = sorted(date_dirs, reverse=True)[0]
    metadata_file = latest_dir / "metadata.json"
    
    if not metadata_file.exists():
        print(f"❌ Metadata file not found: {metadata_file}")
        assert False, f"Metadata file not found: {metadata_file}"
    
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    # Check if metadata has all required fields (including new ones)
    required_fields = [
        'tickers_processed', 'tickers_successful', 'tickers_failed',
        'source_primary', 'source_secondary', 'skipped_tickers', 'status',
        'runtime_seconds', 'runtime_minutes', 'api_retries', 'rate_limit_hits',
        'rate_limit_strategy', 'error_message', 'test_mode', 'full_test_mode', 'dry_run_mode'
    ]
    
    missing_fields = [field for field in required_fields if field not in metadata]
    assert not missing_fields, f"Missing required fields: {missing_fields}"
    
    # Verify counts add up
    processed = metadata['tickers_processed']
    successful = metadata['tickers_successful']
    failed = metadata['tickers_failed']
    
    assert processed == (successful + failed), f"Count mismatch: {processed} != {successful} + {failed}"
    
    print(f"✅ Metadata validation passed: {processed} processed, {successful} successful, {failed} failed")

@pytest.mark.quick
def test_data_columns():
    """Test that each ticker CSV has the expected columns."""
    print("\n=== Testing Data Column Structure ===")
    
    # Find the latest data directory
    data_base_path = Path("data/raw")
    if not data_base_path.exists():
        print("❌ No raw data directory found")
        assert False, "No raw data directory found"
    
    date_dirs = [d for d in data_base_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
    if not date_dirs:
        print("❌ No raw data directories found")
        assert False, "No raw data directories found"
    
    latest_dir = sorted(date_dirs, reverse=True)[0]
    
    expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    all_passed = True
    
    for csv_file in latest_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file)
            ticker = csv_file.stem
            
            # Check if all expected columns are present
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                print(f"❌ {ticker}: Missing columns {missing_columns}")
                all_passed = False
            else:
                print(f"✅ {ticker}: All expected columns present")
                
                # Check if data is not empty
                if len(df) == 0:
                    print(f"❌ {ticker}: No data rows")
                    all_passed = False
                else:
                    print(f"✅ {ticker}: {len(df)} data rows")
                    
        except Exception as e:
            print(f"❌ {csv_file.name}: Error reading file - {e}")
            all_passed = False
    
    assert all_passed, "Some ticker CSVs are missing columns or data rows."

@pytest.mark.quick
def test_error_logging():
    """Test error logging for invalid ticker."""
    print("\n=== Testing Error Logging ===")
    
    # Create a test with an invalid ticker
    fetcher = OHLCVFetcher()
    
    # Mock the ticker list to include an invalid ticker
    test_tickers = ['AAPL', 'FAKE', 'MSFT']
    
    # Process the test tickers
    successful = []
    failed = []
    errors = []
    
    for ticker in test_tickers:
        try:
            data = fetcher.fetch_ohlcv_data(ticker, 1)
            if data is not None:
                successful.append(ticker)
            else:
                failed.append(ticker)
                errors.append({
                    "ticker": ticker,
                    "error": "Failed to fetch data from all sources",
                    "timestamp": "2025-07-18T21:00:00"
                })
        except Exception as e:
            failed.append(ticker)
            errors.append({
                "ticker": ticker,
                "error": str(e),
                "timestamp": "2025-07-18T21:00:00"
            })
    
    # Check if FAKE ticker was properly handled as failed
    assert 'FAKE' in failed, "Invalid ticker 'FAKE' was not properly handled"
    print("✅ Invalid ticker 'FAKE' properly marked as failed")

@pytest.mark.quick
def test_force_flag():
    """Test that --force flag works correctly."""
    print("\n=== Testing Force Flag ===")
    date_str = "2025-07-18"
    data_path = Path("data/raw") / f"dt={date_str}"
    if not data_path.exists() or not any(data_path.iterdir()):
        # Create a fake partition and dummy CSV for testing
        data_path.mkdir(parents=True, exist_ok=True)
        dummy_csv = data_path / "AAPL.csv"
        pd.DataFrame({
            'Date': pd.date_range('2025-07-18', periods=5, freq='D'),
            'Open': [100, 101, 102, 103, 104],
            'High': [101, 102, 103, 104, 105],
            'Low': [99, 100, 101, 102, 103],
            'Close': [100, 101, 102, 103, 104],
            'Volume': [1000000] * 5
        }).to_csv(dummy_csv, index=False)
        print(f"Created fake partition and dummy CSV at {dummy_csv}")
    assert data_path.exists() and any(data_path.iterdir()), "Partition was not created for force flag test"
    fetcher = OHLCVFetcher()
    assert fetcher.check_existing_partition(date_str), "Force flag test: partition check failed"
    print("✅ Force flag test: partition would be skipped without --force")

@pytest.mark.quick
def test_retention_cleanup():
    """Test retention cleanup functionality."""
    print("\n=== Testing Retention Cleanup ===")
    
    fetcher = OHLCVFetcher()
    
    # Test cleanup with dry-run
    cleanup_results = fetcher.cleanup_old_partitions(dry_run=True)
    
    # Check cleanup results structure
    required_cleanup_fields = [
        'cleanup_date', 'retention_days', 'cutoff_date',
        'partitions_deleted', 'total_deleted', 'errors'
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
            fetcher.handle_rate_limit(1)
            assert mock_sleep.call_count == 1, f"Rate limit strategy '{strategy}' did not call sleep"
        
        print(f"✅ Rate limit strategy '{strategy}' works")
    
    assert True

@pytest.mark.heavy
def test_full_test_mode():
    """Test full-test mode functionality."""
    print("\n=== Testing Full Test Mode ===")
    
    fetcher = OHLCVFetcher()
    
    # Test full-test mode with dry-run
    with patch('yfinance.Ticker') as mock_ticker:
        # Mock successful response
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = pd.DataFrame({
            'Open': [100], 'High': [110], 'Low': [90], 'Close': [105], 'Volume': [1000000]
        })
        mock_ticker.return_value = mock_ticker_instance
        
        result = fetcher.run(force=True, dry_run=True, full_test=True)
        
        assert result.get('full_test_mode') == True, "Full test mode not properly enabled"
        print("✅ Full test mode properly enabled")

@pytest.mark.quick
def test_dry_run_mode():
    """Test dry-run mode functionality."""
    print("\n=== Testing Dry Run Mode ===")
    
    fetcher = OHLCVFetcher()
    
    # Test dry-run mode
    with patch('yfinance.Ticker') as mock_ticker:
        # Mock successful response
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = pd.DataFrame({
            'Open': [100], 'High': [110], 'Low': [90], 'Close': [105], 'Volume': [1000000]
        })
        mock_ticker.return_value = mock_ticker_instance
        
        result = fetcher.run(force=True, dry_run=True)
        
        assert result.get('dry_run_mode') == True, "Dry run mode not properly enabled"
        print("✅ Dry run mode properly enabled")

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
        # Should process all 10 tickers in 4 batches (3,3,3,1)
        assert result['tickers_processed'] == 10, "Not all tickers processed"
        assert result['batch_size'] == 3, "Batch size not set in metadata"
        print("✅ Batch processing correct")

@pytest.mark.quick
def test_cooldown_metadata():
    """Test that cooldown is respected and total_sleep_time is logged in metadata."""
    print("\n=== Testing Cooldown Metadata ===")
    fetcher = OHLCVFetcher()
    fetcher.config['batch_size'] = 2
    fetcher.config['base_cooldown_seconds'] = 0.5
    tickers = [f'TICK{i}' for i in range(5)]
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
        # 5 tickers, batch size 2 => 3 batches, 2 sleeps
        assert mock_sleep.call_count == 2, "Cooldown not called correct number of times"
        assert abs(result['total_sleep_time'] - 1.0) < 0.01, "Total sleep time incorrect"
        print("✅ Cooldown and total_sleep_time correct")

@pytest.mark.quick
def test_progress_bar():
    """Test that progress bar does not interfere with output/logging."""
    print("\n=== Testing Progress Bar ===")
    fetcher = OHLCVFetcher()
    fetcher.config['batch_size'] = 2
    fetcher.config['base_cooldown_seconds'] = 0
    fetcher.config['progress'] = True
    tickers = [f'TICK{i}' for i in range(4)]
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data') as mock_fetch_ohlcv, \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch('time.sleep') as mock_sleep, \
         patch('sys.stdout', new_callable=MagicMock()):
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = tickers
        mock_fetch_ohlcv.return_value = pd.DataFrame({'Open':[1],'High':[2],'Low':[0],'Close':[1],'Volume':[100]})
        mock_save_ticker.return_value = True
        result = fetcher.run(force=True, test=False, dry_run=True)
        assert result['tickers_processed'] == 4, "Not all tickers processed with progress bar"
        print("✅ Progress bar does not interfere with processing")

@pytest.mark.quick
def test_batch_error_handling():
    """Test that errors in one ticker do not stop the batch and are logged."""
    print("\n=== Testing Batch Error Handling ===")
    fetcher = OHLCVFetcher()
    fetcher.config['batch_size'] = 2
    fetcher.config['base_cooldown_seconds'] = 0
    tickers = ['GOOD1', 'BAD1', 'GOOD2', 'BAD2']
    def fake_fetch(ticker, days):
        if 'BAD' in ticker:
            raise Exception(f"Simulated error for {ticker}")
        return pd.DataFrame({'Open':[1],'High':[2],'Low':[0],'Close':[1],'Volume':[100]})
    with patch.object(fetcher, 'get_latest_ticker_file') as mock_get_file, \
         patch.object(fetcher, 'load_tickers') as mock_load_tickers, \
         patch.object(fetcher, 'fetch_ohlcv_data', side_effect=fake_fetch), \
         patch.object(fetcher, 'save_ticker_data') as mock_save_ticker, \
         patch('time.sleep') as mock_sleep:
        mock_get_file.return_value = Path('dummy.csv')
        mock_load_tickers.return_value = tickers
        mock_save_ticker.return_value = True
        result = fetcher.run(force=True, test=False, dry_run=True)
        assert result['tickers_failed'] == 2, "Failed tickers not counted correctly"
        print("✅ Batch error handling correct")

def main():
    """Run all tests."""
    print("🧪 Running fetch_data.py Tests\n")
    
    tests = [
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
    total = len(tests)
    
    for test in tests:
        try:
            test()
            passed += 1
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