#!/usr/bin/env python3
"""
Test script for the modular stock pipeline.

This script tests the modular pipeline with different configurations
to ensure it works correctly in various environments.
"""

import os
import sys
import subprocess
from pathlib import Path

def test_modular_pipeline():
    """Test the modular pipeline with different configurations."""
    
    print("=== Testing Modular Stock Pipeline ===\n")
    
    # Test 1: Basic test with default settings
    print("Test 1: Basic test with default settings")
    print("Running: python pipeline/stock_pipeline_modular.py")
    
    result = subprocess.run([
        sys.executable, 'pipeline/stock_pipeline_modular.py'
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Test 1 PASSED - Default settings work")
    else:
        print("❌ Test 1 FAILED")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
    
    print()
    
    # Test 2: Test mode with limited tickers
    print("Test 2: Test mode with limited tickers")
    print("Running with TEST_MODE=true and limited tickers")
    
    env = os.environ.copy()
    env['TEST_MODE'] = 'true'
    env['TICKER_SYMBOLS'] = 'AAPL,MSFT'
    env['DATA_DAYS'] = '7'
    
    result = subprocess.run([
        sys.executable, 'pipeline/stock_pipeline_modular.py'
    ], env=env, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Test 2 PASSED - Test mode works")
    else:
        print("❌ Test 2 FAILED")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
    
    print()
    
    # Test 3: Custom output path
    print("Test 3: Custom output path")
    print("Running with custom OUTPUT_PATH")
    
    env = os.environ.copy()
    env['OUTPUT_PATH'] = 'test_output'
    env['TICKER_SYMBOLS'] = 'AAPL'
    env['DATA_DAYS'] = '5'
    
    result = subprocess.run([
        sys.executable, 'pipeline/stock_pipeline_modular.py'
    ], env=env, capture_output=True, text=True)
    
    if result.returncode == 0:
        # Check if output files were created
        test_output = Path('test_output')
        if test_output.exists():
            print("✅ Test 3 PASSED - Custom output path works")
            # Clean up test output
            import shutil
            shutil.rmtree(test_output)
        else:
            print("❌ Test 3 FAILED - Output files not created")
    else:
        print("❌ Test 3 FAILED")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
    
    print()
    
    # Test 4: Check output files
    print("Test 4: Checking output files")
    
    # Look for the most recent output
    data_path = Path('data/processed')
    if data_path.exists():
        date_dirs = [d for d in data_path.iterdir() if d.is_dir()]
        if date_dirs:
            latest_dir = max(date_dirs, key=lambda x: x.name)
            parquet_file = latest_dir / 'stock_data.parquet'
            csv_file = latest_dir / 'stock_data.csv'
            metadata_file = latest_dir / 'metadata.json'
            
            if parquet_file.exists() and csv_file.exists() and metadata_file.exists():
                print("✅ Test 4 PASSED - All output files created")
                print(f"   Parquet: {parquet_file}")
                print(f"   CSV: {csv_file}")
                print(f"   Metadata: {metadata_file}")
            else:
                print("❌ Test 4 FAILED - Missing output files")
        else:
            print("❌ Test 4 FAILED - No date directories found")
    else:
        print("❌ Test 4 FAILED - Output directory not found")
    
    print("\n=== Test Summary ===")
    print("The modular pipeline has been tested with various configurations.")
    print("Check the output above for any failures.")

if __name__ == "__main__":
    test_modular_pipeline() 