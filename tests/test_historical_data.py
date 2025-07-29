#!/usr/bin/env python3
"""
Tests for historical data functionality and incremental pipeline.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil
import json

from pipeline.fetch_data import OHLCVFetcher
from pipeline.process_features import FeatureProcessor


class TestHistoricalData:
    """Test historical data functionality."""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary data directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_historical_data(self):
        """Create sample historical data for testing."""
        # Create 2 years of sample data
        start_date = datetime.now() - timedelta(days=730)
        dates = pd.date_range(start=start_date, end=datetime.now(), freq='D')
        
        data = []
        for date in dates:
            data.append({
                'date': date,
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 102.0,
                'volume': 1000000,
                'dividends': 0.0,
                'stock_splits': 1.0,
                'ticker': 'AAPL'
            })
        
        return pd.DataFrame(data)
    
    def test_historical_data_path_creation(self, temp_data_dir):
        """Test historical data path creation."""
        config = {
            "base_data_path": str(temp_data_dir),
            "historical_data_path": "raw/historical"
        }
        
        fetcher = OHLCVFetcher()
        fetcher.config = config
        
        path = fetcher.get_historical_data_path("AAPL")
        expected_path = temp_data_dir / "raw/historical/ticker=AAPL"
        
        assert str(path) == str(expected_path)
    
    def test_historical_data_save_and_load(self, temp_data_dir, sample_historical_data):
        """Test saving and loading historical data."""
        config = {
            "base_data_path": str(temp_data_dir),
            "historical_data_path": "raw/historical"
        }
        
        fetcher = OHLCVFetcher()
        fetcher.config = config
        
        # Save historical data
        success = fetcher.save_historical_data("AAPL", sample_historical_data)
        assert success is True
        
        # Load historical data
        loaded_data = fetcher.load_historical_data("AAPL")
        assert loaded_data is not None
        assert len(loaded_data) == len(sample_historical_data)
        assert list(loaded_data.columns) == list(sample_historical_data.columns)
    
    def test_historical_completeness_check(self, temp_data_dir, sample_historical_data):
        """Test historical data completeness validation."""
        config = {
            "base_data_path": str(temp_data_dir),
            "historical_data_path": "raw/historical",
            "min_historical_days": 730
        }
        
        fetcher = OHLCVFetcher()
        fetcher.config = config
        
        # Save 2 years of data
        fetcher.save_historical_data("AAPL", sample_historical_data)
        
        # Check completeness
        is_complete, days_available = fetcher.check_historical_completeness("AAPL")
        assert is_complete is True
        assert days_available >= 730
    
    def test_incremental_data_fetch(self, temp_data_dir, sample_historical_data):
        """Test incremental data fetching logic."""
        config = {
            "base_data_path": str(temp_data_dir),
            "historical_data_path": "raw/historical"
        }
        
        fetcher = OHLCVFetcher()
        fetcher.config = config
        
        # Save historical data up to 5 days ago
        historical_data = sample_historical_data.iloc[:-5]
        fetcher.save_historical_data("AAPL", historical_data)
        
        # Mock the fetch_ohlcv_data method to return new data
        def mock_fetch(ticker, days):
            if days <= 0:
                return None
            # Return 5 days of new data
            new_dates = pd.date_range(
                start=historical_data['date'].max() + timedelta(days=1),
                periods=5,
                freq='D'
            )
            new_data = []
            for date in new_dates:
                new_data.append({
                    'date': date,
                    'open': 100.0,
                    'high': 105.0,
                    'low': 95.0,
                    'close': 102.0,
                    'volume': 1000000,
                    'dividends': 0.0,
                    'stock_splits': 1.0,
                    'ticker': 'AAPL'
                })
            return pd.DataFrame(new_data)
        
        fetcher.fetch_ohlcv_data = mock_fetch
        
        # Test incremental fetch
        new_data = fetcher.fetch_incremental_data("AAPL", days_back=30)
        assert new_data is not None
        assert len(new_data) == 5
    
    def test_data_merging(self, temp_data_dir, sample_historical_data):
        """Test merging new data with historical data."""
        config = {
            "base_data_path": str(temp_data_dir),
            "historical_data_path": "raw/historical"
        }
        
        fetcher = OHLCVFetcher()
        fetcher.config = config
        
        # Save historical data
        historical_data = sample_historical_data.iloc[:-5]
        fetcher.save_historical_data("AAPL", historical_data)
        
        # Create new data
        new_dates = pd.date_range(
            start=historical_data['date'].max() + timedelta(days=1),
            periods=5,
            freq='D'
        )
        new_data = []
        for date in new_dates:
            new_data.append({
                'date': date,
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 102.0,
                'volume': 1000000,
                'dividends': 0.0,
                'stock_splits': 1.0,
                'ticker': 'AAPL'
            })
        new_df = pd.DataFrame(new_data)
        
        # Merge data
        merged_data = fetcher.merge_with_historical("AAPL", new_df)
        assert merged_data is not None
        assert len(merged_data) == len(historical_data) + len(new_df)
        
        # Check no duplicates
        assert len(merged_data) == len(merged_data.drop_duplicates(subset=['date']))
    
    def test_feature_processing_with_historical(self, temp_data_dir, sample_historical_data):
        """Test feature processing with historical data."""
        config = {
            "raw_data_path": str(temp_data_dir / "raw"),
            "processed_data_path": str(temp_data_dir / "processed"),
            "historical_data_path": str(temp_data_dir / "raw/historical"),
            "incremental_mode": True
        }
        
        processor = FeatureProcessor()
        processor.config = config
        processor.raw_path = Path(config["raw_data_path"])
        processor.processed_path = Path(config["processed_data_path"])
        processor.historical_path = Path(config["historical_data_path"])
        
        # Save historical data
        historical_dir = processor.historical_path / "ticker=AAPL"
        historical_dir.mkdir(parents=True, exist_ok=True)
        
        # Group by year and save
        sample_historical_data['year'] = sample_historical_data['date'].dt.year
        for year, year_data in sample_historical_data.groupby('year'):
            year_dir = historical_dir / f"year={year}"
            year_dir.mkdir(exist_ok=True)
            year_data.to_parquet(year_dir / "data.parquet", index=False)
        
        # Create current data (last 30 days)
        current_data = sample_historical_data.tail(30).copy()
        
        # Process with historical data
        processed_data, rows_dropped = processor.process_ticker_with_historical("AAPL", current_data)
        
        assert processed_data is not None
        assert len(processed_data) > 0
        
        # Check that technical indicators are calculated
        expected_columns = [
            'sma_50', 'sma_200', 'ema_26', 'macd', 'macd_signal', 
            'macd_histogram', 'rsi_14', 'bb_middle', 'bb_upper', 'bb_lower'
        ]
        
        for col in expected_columns:
            assert col in processed_data.columns, f"Missing column: {col}"
        
        # Check that SMA_200 has valid values (not all NaN)
        sma_200_values = processed_data['sma_200'].dropna()
        assert len(sma_200_values) > 0, "SMA_200 should have valid values with historical data"


def test_historical_data_smoke():
    """Smoke test for historical data functionality."""
    # This test validates the overall historical data pipeline
    # It should be run after the bootstrap process completes
    
    # Check if historical data directory exists
    historical_path = Path("data/raw/historical")
    if not historical_path.exists():
        pytest.skip("Historical data not found. Run bootstrap first.")
    
    # Check for bootstrap summary
    summary_file = historical_path / "bootstrap_summary.json"
    if not summary_file.exists():
        pytest.skip("Bootstrap summary not found. Run bootstrap first.")
    
    # Load bootstrap summary
    with open(summary_file, 'r') as f:
        summary = json.load(f)
    
    bootstrap_info = summary.get("bootstrap_summary", {})
    
    # Validate bootstrap results
    assert bootstrap_info.get("successful_tickers", 0) > 0, "No successful tickers in bootstrap"
    assert bootstrap_info.get("total_rows", 0) > 0, "No data rows in bootstrap"
    
    # Check success rate
    success_rate = float(bootstrap_info.get("success_rate", "0%").rstrip('%'))
    assert success_rate > 80, f"Bootstrap success rate too low: {success_rate}%"
    
    # Check that we have at least 400 tickers (80% of 500)
    successful_tickers = bootstrap_info.get("successful_tickers", 0)
    assert successful_tickers >= 400, f"Too few successful tickers: {successful_tickers}"
    
    # Check that each successful ticker has sufficient data
    min_days = 730  # 2 years
    ticker_dirs = list(historical_path.glob("ticker=*"))
    
    for ticker_dir in ticker_dirs[:10]:  # Check first 10 tickers
        ticker = ticker_dir.name.replace("ticker=", "")
        
        # Load all data for this ticker
        all_data = []
        for year_dir in ticker_dir.glob("year=*"):
            data_file = year_dir / "data.parquet"
            if data_file.exists():
                year_data = pd.read_parquet(data_file)
                all_data.append(year_data)
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            days_available = len(combined_data)
            
            assert days_available >= min_days, f"Ticker {ticker} has insufficient data: {days_available} days"
            
            # Check data quality
            assert 'date' in combined_data.columns, f"Ticker {ticker} missing date column"
            assert 'close' in combined_data.columns, f"Ticker {ticker} missing close column"
            assert 'volume' in combined_data.columns, f"Ticker {ticker} missing volume column"
            
            # Check for reasonable data ranges
            close_prices = combined_data['close'].dropna()
            assert len(close_prices) > 0, f"Ticker {ticker} has no valid close prices"
            assert close_prices.min() > 0, f"Ticker {ticker} has invalid close prices"
            
            volumes = combined_data['volume'].dropna()
            assert len(volumes) > 0, f"Ticker {ticker} has no valid volumes"
            assert volumes.min() >= 0, f"Ticker {ticker} has invalid volumes"


if __name__ == "__main__":
    pytest.main([__file__]) 