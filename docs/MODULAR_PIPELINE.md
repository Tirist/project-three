# Modular Stock Pipeline

A simplified, modularized version of the stock evaluation pipeline designed for cloud environments (serverless, containers, etc.).

## Overview

The modular pipeline breaks down the stock data processing into three main functions:

1. **`fetch_data()`** - Fetches raw stock data using yfinance
2. **`clean_data()`** - Applies filtering and technical indicator calculations
3. **`store_data()`** - Saves processed data to local storage

## Features

- ✅ **Environment Variable Configuration** - All settings via environment variables
- ✅ **Cloud-Ready** - Designed for serverless and containerized environments
- ✅ **Lightweight** - Minimal dependencies, focused functionality
- ✅ **Progress Logging** - Clear progress indicators through each step
- ✅ **Error Handling** - Graceful error handling with fallbacks
- ✅ **Test Mode** - Quick testing with limited data

## Quick Start

### Basic Usage

```bash
# Run with default settings (S&P 500 tickers, 30 days)
python pipeline/stock_pipeline_modular.py
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TICKER_SYMBOLS` | `AAPL,MSFT,GOOGL,AMZN,TSLA` | Comma-separated list of ticker symbols |
| `DATA_DAYS` | `30` | Number of days of data to fetch |
| `OUTPUT_PATH` | `data/processed` | Output directory path |
| `TEST_MODE` | `false` | Set to `true` for test mode (limited data) |
| `API_KEY` | `None` | API key for data sources (optional) |
| `USE_YFINANCE` | `true` | Use yfinance for data fetching |
| `USE_ALPHA_VANTAGE` | `false` | Use Alpha Vantage API |

### Examples

```bash
# Test mode with limited tickers
export TEST_MODE=true
export TICKER_SYMBOLS="AAPL,MSFT"
export DATA_DAYS=7
python pipeline/stock_pipeline_modular.py

# Custom output path
export OUTPUT_PATH="/tmp/stock_data"
python pipeline/stock_pipeline_modular.py

# Full S&P 500 with 60 days of data
export DATA_DAYS=60
python pipeline/stock_pipeline_modular.py
```

## Cloud Deployment

### Docker

```bash
# Build the container
docker build -f Dockerfile.modular -t stock-pipeline .

# Run with default settings
docker run stock-pipeline

# Run with custom environment variables
docker run -e TICKER_SYMBOLS="AAPL,MSFT" -e TEST_MODE="true" stock-pipeline

# Run with volume mount for data persistence
docker run -v $(pwd)/data:/app/data stock-pipeline
```

### AWS Lambda

```python
# lambda_function.py
import os
import sys
from pipeline.stock_pipeline_modular import main

def lambda_handler(event, context):
    # Set environment variables from event
    if 'ticker_symbols' in event:
        os.environ['TICKER_SYMBOLS'] = event['ticker_symbols']
    if 'data_days' in event:
        os.environ['DATA_DAYS'] = str(event['data_days'])
    if 'test_mode' in event:
        os.environ['TEST_MODE'] = str(event['test_mode']).lower()
    
    # Run the pipeline
    success = main()
    
    return {
        'statusCode': 200 if success else 500,
        'body': 'Pipeline completed successfully' if success else 'Pipeline failed'
    }
```

### Google Cloud Functions

```python
# main.py
import os
import sys
from pipeline.stock_pipeline_modular import main

def stock_pipeline(request):
    # Set environment variables from request
    request_json = request.get_json()
    if request_json:
        if 'ticker_symbols' in request_json:
            os.environ['TICKER_SYMBOLS'] = request_json['ticker_symbols']
        if 'data_days' in request_json:
            os.environ['DATA_DAYS'] = str(request_json['data_days'])
    
    # Run the pipeline
    success = main()
    
    return f'Pipeline {"completed" if success else "failed"}', 200 if success else 500
```

## Output Format

The pipeline generates the following files in the output directory:

```
data/processed/YYYY-MM-DD/
├── stock_data.parquet    # Main data file (Parquet format)
├── stock_data.csv        # Data file (CSV format)
└── metadata.json         # Processing metadata
```

### Data Schema

The output data includes:

- **Basic OHLCV**: `open`, `high`, `low`, `close`, `volume`
- **Technical Indicators**: `sma_20`, `sma_50`, `ema_12`, `ema_26`, `macd`, `rsi`
- **Price Changes**: `price_change`, `price_change_pct`
- **Volume Indicators**: `volume_sma`, `volume_ratio`
- **Metadata**: `ticker`, `date`

### Metadata

The `metadata.json` file contains:

```json
{
  "generated_at": "2024-01-15T10:30:00",
  "ticker_count": 5,
  "total_rows": 150,
  "date_range": {
    "start": "2023-12-15T00:00:00",
    "end": "2024-01-15T00:00:00"
  },
  "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
  "columns": ["date", "ticker", "open", "high", "low", "close", "volume", ...],
  "test_mode": false
}
```

## Testing

Run the test script to verify the pipeline works correctly:

```bash
python test_modular_pipeline.py
```

This will test:
- Default settings
- Test mode with limited data
- Custom output paths
- Output file generation

## Error Handling

The pipeline includes comprehensive error handling:

- **Network failures**: Retries and fallbacks for data fetching
- **Missing data**: Skips tickers with no data, continues processing
- **Invalid data**: Removes rows with missing values
- **Insufficient data**: Warns when technical indicators can't be calculated

## Performance

Typical performance characteristics:

- **Test mode** (5 tickers, 7 days): ~10-30 seconds
- **Production mode** (S&P 500, 30 days): ~5-15 minutes
- **Memory usage**: ~100-500MB depending on data size
- **Output size**: ~1-50MB depending on ticker count and date range

## Limitations

- **Data source**: Currently only supports yfinance (Alpha Vantage support planned)
- **Historical data**: Limited to available data from yfinance
- **Rate limits**: Subject to yfinance API rate limits
- **Storage**: Local file system only (cloud storage integration planned)

## Migration from Original Pipeline

The modular pipeline is designed to be a drop-in replacement for the original pipeline in cloud environments:

1. **Same data format**: Output files are compatible
2. **Same technical indicators**: All major indicators included
3. **Environment variables**: Easy configuration without code changes
4. **Lightweight**: Reduced dependencies and complexity

## Support

For issues or questions:

1. Check the logs for detailed error messages
2. Verify environment variable configuration
3. Test with `TEST_MODE=true` for debugging
4. Review the metadata.json file for processing details 