# Stock Evaluation Pipeline

A comprehensive pipeline for fetching, processing, and analyzing stock market data with partitioned storage and metadata logging.

## Project Structure

```
Project Three/
├── config/
│   └── settings.yaml          # Configuration file
├── data/                      # Partitioned data storage
│   ├── tickers/
│   │   └── dt=YYYY-MM-DD/
│   │       └── tickers.csv
│   └── raw/
│       └── dt=YYYY-MM-DD/
│           ├── AAPL.csv
│           ├── MSFT.csv
│           └── ...
├── logs/                      # Metadata and execution logs
│   ├── tickers/
│   │   └── dt=YYYY-MM-DD/
│   │       └── metadata.json
│   └── fetch/
│       └── dt=YYYY-MM-DD/
│           ├── metadata.json
│           └── errors.json
├── tests/
│   └── test_fetch_data.py     # Test script for fetch_data.py
├── fetch_tickers.py           # Ticker fetching script
├── fetch_data.py              # OHLCV data fetching script
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Installation

1. **Clone the repository** (if applicable)
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Fetch S&P 500 Tickers

The `fetch_tickers.py` script fetches the current S&P 500 ticker list from Wikipedia and saves it to a partitioned folder structure.

**Basic usage:**
```bash
python3 fetch_tickers.py
```

**Force re-fetch (even if partition exists):**
```bash
python3 fetch_tickers.py --force
```

**Dry-run mode (simulate without writing files):**
```bash
python3 fetch_tickers.py --dry-run
```

**Full test mode (validate entire ticker universe):**
```bash
python3 fetch_tickers.py --full-test
```

**Use custom config file:**
```bash
python3 fetch_tickers.py --config path/to/config.yaml
```

**Combine flags:**
```bash
python3 fetch_tickers.py --force --dry-run --full-test
```

**Debug rate limit simulation:**
```bash
python3 fetch_tickers.py --debug-rate-limit
python3 fetch_data.py --debug-rate-limit
```

### Fetch OHLCV Data

The `fetch_data.py` script reads the latest ticker list and fetches OHLCV (Open, High, Low, Close, Volume) data for each ticker.

**Basic usage:**
```bash
python3 fetch_data.py
```

**Test mode (small subset of tickers):**
```bash
python3 fetch_data.py --test
```

**Full test mode (all tickers, 2 years of data):**
```bash
python3 fetch_data.py --full-test
```

**Dry-run mode (simulate without writing files):**
```bash
python3 fetch_data.py --dry-run
```

**Force re-fetch (overwrite existing data):**
```bash
python3 fetch_data.py --force
```

**Combine flags:**
```bash
python3 fetch_data.py --test --dry-run --force
python3 fetch_data.py --full-test --dry-run
```

### Output Structure

The scripts create the following structure:

**Ticker Data:**
```
data/tickers/dt=2025-07-18/
└── tickers.csv               # CSV with symbol and company_name columns

logs/tickers/dt=2025-07-18/
└── metadata.json            # Execution metadata and lineage info
```

**OHLCV Data:**
```
data/raw/dt=2025-07-18/
├── AAPL.csv                  # OHLCV data for Apple
├── MSFT.csv                  # OHLCV data for Microsoft
└── ...                       # One file per ticker

logs/fetch/dt=2025-07-18/
├── metadata.json            # Execution metadata and summary
└── errors.json              # Error details for failed tickers
```

### Configuration

Edit `config/settings.yaml` to customize:

- Data source settings
- File paths
- Validation parameters
- Retry logic

## Features

### ✅ Implemented
- **Daily ticker ingestion** from Wikipedia S&P 500 list
- **OHLCV data fetching** with yfinance and Alpha Vantage fallback
- **Partitioned storage** by date (`dt=YYYY-MM-DD`)
- **Metadata logging** in JSON format with extended fields
- **Error handling** with retry logic and detailed error logging
- **Ticker symbol cleaning** (e.g., BRK.B → BRK-B)
- **Duplicate prevention** (skip if partition exists)
- **Force re-fetch** option
- **Test mode** for development and testing
- **Full test mode** for comprehensive validation
- **Dry-run mode** for simulation
- **Retention cleanup** with configurable retention periods
- **Rate limiting** with multiple strategies (exponential backoff, fixed delay, adaptive)
- **Ticker change tracking** with diff logs
- **Comprehensive logging** to both console and file
- **Configuration management** with YAML
- **Automated testing** with comprehensive test suite

### 🔄 Stretch Goals (Optional)
- [ ] Validate ticker count against expected range
- [ ] Add more data sources beyond Wikipedia
- [ ] Implement data quality checks
- [ ] Add email notifications for failures

## Metadata Schema

### Ticker Metadata
```json
{
  "run_date": "2025-07-18",
  "source_primary": "wikipedia_sp500",
  "source_secondary": null,
  "tickers_fetched": 503,
  "tickers_added": 2,
  "tickers_removed": 1,
  "skipped_tickers": 0,
  "status": "success",
  "runtime_seconds": 1.29,
  "runtime_minutes": 0.02,
  "api_retries": 0,
  "rate_limit_hits": 0,
  "rate_limit_strategy": "exponential_backoff",
  "error_message": null,
  "full_test_mode": false,
  "dry_run_mode": false
}
```

### OHLCV Metadata
```json
{
  "run_date": "2025-07-18",
  "source_primary": "yfinance",
  "source_secondary": "alpha_vantage",
  "tickers_processed": 3,
  "tickers_successful": 3,
  "tickers_failed": 0,
  "skipped_tickers": 0,
  "status": "success",
  "runtime_seconds": 0.73,
  "runtime_minutes": 0.01,
  "api_retries": 0,
  "rate_limit_hits": 0,
  "rate_limit_strategy": "exponential_backoff",
  "error_message": null,
  "test_mode": true,
  "full_test_mode": false,
  "dry_run_mode": false
}
```

### Ticker Diff Log
```json
{
  "run_date": "2025-07-18",
  "timestamp": "2025-07-18T21:00:00",
  "tickers_added": ["NEW", "IPO"],
  "tickers_removed": ["DELISTED"],
  "total_added": 2,
  "total_removed": 1,
  "net_change": 1
}
```

### Cleanup Log
```json
{
  "cleanup_date": "2025-07-18",
  "retention_days": 30,
  "cutoff_date": "2025-06-18",
  "partitions_deleted": [
    "data/tickers/dt=2025-06-15",
    "data/raw/dt=2025-06-15"
  ],
  "total_deleted": 2,
  "errors": []
}
```

## Error Handling

The script handles various error scenarios:

- **Network failures**: Automatic retry with exponential backoff
- **Missing config**: Falls back to default settings
- **Invalid data**: Logs warnings but continues execution
- **File system errors**: Creates directories as needed
- **Parsing errors**: Detailed error logging

## Logging

Logs are written to:
- **Console**: Real-time execution status
- **File**: `fetch_tickers.log` for historical tracking
- **Metadata**: JSON files for lineage and debugging

## Testing

Run the automated test suites:

```bash
# Test fetch_data.py functionality
PYTHONPATH=. python3 tests/test_fetch_data.py

# Test fetch_tickers.py functionality
PYTHONPATH=. python3 tests/test_fetch_tickers.py
```

The test suites validate:
- **Metadata consistency** and extended field validation
- **Data column structure** and integrity
- **Error handling** for invalid tickers and API failures
- **Force flag functionality** and duplicate prevention
- **Retention cleanup** with configurable periods
- **Rate limiting** with multiple strategies
- **Full test mode** and dry-run functionality
- **Ticker change tracking** and diff logging
- **Mock API failures** and graceful error handling

## Performance and Rate Limiting

### Rate Limit Strategies
The pipeline supports three rate limiting strategies:

1. **Exponential Backoff** (default): Doubles delay time with each retry
2. **Fixed Delay**: Uses constant delay between retries
3. **Adaptive**: Scales delay based on attempt number

### Configuration
```yaml
rate_limit_enabled: true
rate_limit_strategy: "exponential_backoff"
max_rate_limit_hits: 10
base_cooldown_seconds: 1
max_cooldown_seconds: 60
```

### Performance Guidelines
- **Test Mode**: 3 tickers, ~1 second runtime
- **Production Mode**: 503 tickers, ~5-10 minutes runtime
- **Full Test Mode**: 503 tickers, 2 years data, ~30-60 minutes runtime
- **Rate Limits**: yfinance ~1000 requests/hour, Alpha Vantage ~5 requests/minute

### Monitoring
Monitor these metrics in metadata:
- `api_retries`: Number of retry attempts
- `rate_limit_hits`: Rate limit events encountered
- `runtime_minutes`: Total execution time
- `tickers_successful/failed`: Success rate

### Process Features (Technical Indicators)

The `process_features.py` script reads all OHLCV data, computes technical indicators, and saves a merged features dataset.

**Basic usage:**
```bash
python3 process_features.py
```

**Test mode (small subset of data):**
```bash
python3 process_features.py --test
```

**Dry-run mode (simulate without writing files):**
```bash
python3 process_features.py --dry-run
```

**Force re-process (overwrite existing features partition):**
```bash
python3 process_features.py --force
```

**Combine flags:**
```bash
python3 process_features.py --test --dry-run --force
```

**Export a sample of features:**
```bash
python3 process_features.py --sample
```

**Drop incomplete tickers:**
```bash
python3 process_features.py --drop-incomplete
```

### Technical Indicators Generated

The following indicators are computed for each ticker:

- **SMA_5, SMA_10, SMA_20, SMA_50, SMA_200**: Simple Moving Averages (5, 10, 20, 50, 200 days)
- **EMA_12, EMA_26**: Exponential Moving Averages (12, 26 days)
- **RSI_14**: Relative Strength Index (14-day)
- **MACD, MACD_Signal, MACD_Histogram**: Moving Average Convergence Divergence (12, 26, 9)
- **Bollinger Bands**: 20-day SMA ± 2 stddev (BB_Upper, BB_Lower, BB_Middle, BB_Width, BB_%B)
- **Volume_SMA_20**: 20-day average volume
- **Momentum_1d, Momentum_5d, Momentum_10d**: Price momentum
- **ATR_14**: Average True Range (14-day)
- **Stoch_%K, Stoch_%D**: Stochastic Oscillator

**Example features.parquet schema:**
```
[date, ticker, open, high, low, close, volume, SMA_5, SMA_10, SMA_20, SMA_50, SMA_200, EMA_12, EMA_26, RSI_14, MACD, MACD_Signal, MACD_Histogram, BB_Upper, BB_Lower, BB_Middle, BB_Width, BB_%B, Volume_SMA, Momentum_1d, Momentum_5d, Momentum_10d, ATR_14, Stoch_%K, Stoch_%D]
```

### Features Metadata Example
```json
{
  "run_date": "2025-07-18",
  "tickers_processed": 3,
  "features_generated": ["SMA_5", "SMA_10", "EMA_12", "EMA_26", "RSI_14", "MACD", "BB_Upper", "BB_Lower"],
  "batch_size": 10,
  "cooldown_seconds": 2,
  "rate_limit_hits": 1,
  "total_sleep_time": 30,
  "status": "success"
}
```

### Sample errors.json
```json
[
  {
    "ticker": "FAKE",
    "error": "Failed to fetch data from all sources",
    "timestamp": "2025-07-18T21:00:00"
  }
]
```

### Sample cleanup.json
```json
{
  "cleanup_date": "2025-07-18",
  "retention_days": 30,
  "cutoff_date": "2025-06-18",
  "partitions_deleted": [
    "data/processed/dt=2025-06-15"
  ],
  "total_deleted": 1,
  "errors": []
}
```

### Batch Processing & Cooldown

- **--batch-size**: Number of tickers per batch (default: 25)
- **--cooldown**: Seconds to sleep between batches (default: 1-2)
- **--progress**: Show real-time progress bar (tqdm)
- **--debug-rate-limit**: Simulate/log rate limit events and cooldowns (for testing)
- **--sample**: Export a sample CSV with 5 random tickers × 7 days (process_features.py)
- **--test**: Use a small subset of tickers/data for fast validation
- **--dry-run**: Simulate all operations, no files written
- **--drop-incomplete**: Drop tickers with fewer than 500 rows and drop initial NaN rows per ticker (for clean ML datasets)

**API Rate Limits:**
- **yfinance**: ~1000 requests/hour (subject to change)
- **Alpha Vantage**: 5 requests/minute (free tier)

**Recommended settings for S&P 500:**
- `--batch-size 25 --cooldown 2` (safe for most free API quotas)
- Monitor `rate_limit_hits` and `total_sleep_time` in metadata for tuning

## One-Click Pipeline Execution

The recommended way to run the full pipeline is with the orchestrator script:

**Quick validation (test mode):**
```bash
python3 run_pipeline.py --test
```

**Full pipeline with parallel fetch and data cleaning:**
```bash
python3 run_pipeline.py --full --parallel 8 --drop-incomplete
```

**Test-only mode (run all tests):**
```bash
python3 run_all_tests.py
```

This will:
- Run ticker ingestion, OHLCV fetching, and feature engineering in sequence
- Pass all relevant flags to each stage
- Run the full test suite at the end
- Print a final summary of results

> For most users, use `run_pipeline.py` as your entry point for data updates and validation.

### End-to-End Validation

**Run a full test pipeline:**
```bash
python3 fetch_tickers.py --test --force
python3 fetch_data.py --test --batch-size 10 --cooldown 2
python3 process_features.py --test
```

**Expected outputs:**
```
data/tickers/dt=YYYY-MM-DD/tickers.csv
data/raw/dt=YYYY-MM-DD/AAPL.csv
data/processed/dt=YYYY-MM-DD/features.parquet
```

### Running All Tests

Run the full test suite:
```bash
pytest tests/
```

**Test coverage:**
- **Batching:** Confirms correct batch sizes and all tickers processed
- **Cooldown:** Verifies total sleep time is logged and respected
- **Progress Bar:** Ensures progress bar does not interfere with logs
- **Error Handling:** Simulates failures and checks errors.json
- **Feature Calculations:** Validates technical indicator correctness and schema

## Next Steps

This is the second component of the stock evaluation pipeline. Future components will include:

1. **Technical Analysis** (`analyze_data.py`)
2. **Portfolio Optimization** (`optimize_portfolio.py`)
3. **Reporting and Visualization** (`generate_reports.py`)

---

For a full end-to-end checklist and sample output validation, see [docs/VALIDATION.md](docs/VALIDATION.md).

## Contributing

1. Follow the existing code style
2. Add comprehensive docstrings
3. Include error handling
4. Update configuration as needed
5. Test with various scenarios

## License

[Add your license information here] 