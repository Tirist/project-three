#!/usr/bin/env python3
"""
validate_features.py

Validate the processed features parquet file and output a summary report.

Usage:
    python3 scripts/validate_features.py [--parquet-path PATH] [--sample-size N]
"""
import os
import sys
import argparse
import pandas as pd
import numpy as np

EXPECTED_COLUMNS = [
    'ticker', 'date', 'open', 'high', 'low', 'close', 'volume',
    'SMA_5', 'SMA_10', 'SMA_20', 'SMA_50', 'SMA_200',
    'EMA_12', 'EMA_26', 'RSI_14', 'MACD', 'MACD_Signal', 'MACD_Histogram',
    'BB_Upper', 'BB_Lower', 'BB_Middle', 'BB_Width', 'BB_%B',
    'Volume_SMA', 'Momentum_1d', 'Momentum_5d', 'Momentum_10d',
    'ATR_14', 'Stoch_%K', 'Stoch_%D'
]

DEFAULT_PARQUET = 'data/processed/dt=2025-07-21/features.parquet'
DEFAULT_SAMPLE_SIZE = 10
DEFAULT_SAMPLE_PATH = 'data/processed/dt=2025-07-21/sample_validation.csv'
EXPECTED_TICKERS = 503


def print_section(title):
    print(f"\n{'='*10} {title} {'='*10}")

def main():
    parser = argparse.ArgumentParser(description="Validate features parquet file and output a summary report.")
    parser.add_argument('--parquet-path', default=DEFAULT_PARQUET, help='Path to features.parquet')
    parser.add_argument('--sample-size', type=int, default=DEFAULT_SAMPLE_SIZE, help='Number of random rows for sample CSV')
    args = parser.parse_args()

    if not os.path.exists(args.parquet_path):
        print(f"[ERROR] Parquet file not found: {args.parquet_path}")
        sys.exit(1)

    print_section("Loading Parquet File")
    df = pd.read_parquet(args.parquet_path)
    print(f"Loaded: {args.parquet_path}")
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]:,} columns")

    print_section("Schema (Columns and Dtypes)")
    print(df.dtypes)

    print_section("First 10 Rows")
    print(df.head(10))

    print_section("Validation Checks")
    # Unique tickers
    unique_tickers = df['ticker'].nunique() if 'ticker' in df.columns else 0
    print(f"Unique tickers: {unique_tickers} (expected: {EXPECTED_TICKERS})")
    # Missing/NaN values
    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if nan_cols.empty:
        print("No missing values detected.")
    else:
        print("Columns with missing values:")
        for col, count in nan_cols.items():
            print(f"  {col}: {count}")
    # Expected columns
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        print(f"Missing columns: {missing_cols}")
    else:
        print("All expected columns present.")

    print_section("Summary Statistics (Numeric Columns)")
    stats = df.describe().T[['min', 'max', 'mean']]
    print(stats)

    print_section("Exporting Sample Rows")
    sample_size = min(args.sample_size, len(df))
    sample = df.sample(n=sample_size, random_state=42)
    sample_path = DEFAULT_SAMPLE_PATH
    os.makedirs(os.path.dirname(sample_path), exist_ok=True)
    sample.to_csv(sample_path, index=False)
    print(f"Sample of {sample_size} rows exported to: {sample_path}")

    print_section("Validation Summary")
    print(f"Total rows: {df.shape[0]}")
    print(f"Total columns: {df.shape[1]}")
    print(f"Unique tickers: {unique_tickers}")
    print(f"Columns missing: {missing_cols if missing_cols else 'None'}")
    print(f"Columns with NaNs: {list(nan_cols.index) if not nan_cols.empty else 'None'}")
    print(f"Sample file: {sample_path}")
    print("\nValidation complete.\n")

if __name__ == "__main__":
    main() 