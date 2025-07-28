#!/usr/bin/env python3
"""
run_all_tests.py

Runs the full pytest suite for the project.
Prints a summary and exits nonzero if any test fails.
Accepts --test (quick tests only) and --full-test (all tests).
"""
import subprocess
import sys
import argparse
import os
from pathlib import Path
from datetime import datetime

# Ensure process_features.py is called in test mode before running tests
print("[SETUP] Ensuring mock features and metadata exist for today's partition...")
today_str = datetime.now().strftime('dt=%Y-%m-%d')
features_path = Path('data/processed') / today_str / 'features.parquet'
metadata_path = Path('logs/features') / today_str / 'metadata.json'
if not (features_path.exists() and metadata_path.exists()):
    print(f"[SETUP] Running process_features.py --test-mode to generate mock outputs for {today_str}")
    result = subprocess.run([sys.executable, 'process_features.py', '--test-mode'], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if not (features_path.exists() and metadata_path.exists()):
        print(f"[ERROR] Could not generate mock features.parquet and metadata.json for {today_str}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Run all tests with pytest")
    parser.add_argument('--full-test', action='store_true', help='Run full test suite including heavy tests')
    args = parser.parse_args()

    print("=== Running All Tests (pytest) ===")
    
    if args.full_test:
        print("[INFO] Running full test suite including heavy tests. This may take several minutes...")
        # Use pytest-xdist for parallel execution to avoid runtime bottlenecks with sequential tests
        cmd = [sys.executable, '-m', 'pytest', '-v', '-n', 'auto', '--dist=loadscope']
    else:
        print("[INFO] Running quick test suite (skipping test_full_test_mode). To run all tests, use --full-test.")
        print("[INFO] Skipping: test_full_test_mode")
        # Use pytest-xdist for parallel execution to avoid runtime bottlenecks with sequential tests
        cmd = [sys.executable, '-m', 'pytest', '-v', '-n', 'auto', '--dist=loadscope', '-m', 'quick']

    try:
        result = subprocess.run(cmd, check=True)
        print("\n✅ All tests passed!")
        sys.exit(0)
    except subprocess.CalledProcessError:
        print("\n❌ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 