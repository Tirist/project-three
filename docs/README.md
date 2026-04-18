# Project Three - Stock Evaluation Pipeline

A comprehensive, production-ready data pipeline for stock market analysis with automated orchestration, integrity monitoring, and frontend dashboard preparation.

## 🏗️ Project Structure

```
project_root/
├── pipeline/                    # Core pipeline components
│   ├── fetch_tickers.py        # S&P 500 ticker fetching
│   ├── fetch_data.py           # Historical price data collection
│   ├── process_features.py     # Feature engineering and processing
│   ├── run_pipeline.py         # Main pipeline orchestrator
│   └── utils/                  # Shared utilities
│       ├── common.py           # Common functions and classes
│       ├── logger.py           # Standardized logging
│       ├── progress.py         # Progress tracking utilities
│       └── integrity_monitor.py # Pipeline monitoring
├── reports/                    # Reporting and API
│   ├── generate_integrity_report.py
│   ├── api.py                  # Frontend API endpoints
│   └── integrity_reports/      # Generated reports
├── tests/                      # Test suite
│   ├── test_fetch_data.py
│   ├── test_fetch_tickers.py
│   ├── test_process_features.py
│   └── run_all_tests.py
├── ops/                        # Portable prod entrypoints (env, launchd/systemd samples)
│   ├── run_prod_data.sh        # Daily full-universe data build (--full --skip-tests)
│   └── env.example
├── scripts/                    # Automation scripts
│   ├── run_daily_tests.py      # Daily smoke tests
│   ├── run_weekly_tests.py     # Weekly full tests
│   ├── cleanup_old_reports.py  # Data retention management
│   └── setup_cron.sh          # Cron job setup
├── tools/                      # Maintenance and utilities
│   └── maintenance/           # Pipeline maintenance tools
│       └── fixes/             # Recovery and fix scripts
│           ├── populate_historical.py  # Historical data recovery
│           └── README.md       # Fix documentation
├── data/                       # Data storage
│   ├── raw/                    # Raw CSV data (partitioned by date)
│   ├── processed/              # Processed parquet files
│   └── test/                   # Test data (isolated)
├── logs/                       # Logging and metadata
│   ├── fetch/                  # Data fetching logs
│   ├── features/               # Feature processing logs
│   ├── tickers/                # Ticker fetching logs
│   ├── integrity/              # Integrity monitoring logs
│   └── structured/             # JSON logs for frontend
├── config/                     # Configuration files
│   ├── settings.yaml           # Main pipeline settings
│   └── test_schedules.yaml     # Test scheduling configuration
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── pytest.ini                 # Test configuration
└── .gitignore                 # Git ignore rules
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)
- Access to financial data APIs (Alpha Vantage, Yahoo Finance)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd project-three

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up automated scheduling
bash scripts/setup_cron.sh
```

### Manual pipeline run (operator guide)

**Golden path — full S&P universe under `data/`, with pytest (local confidence):**

```bash
python pipeline/run_pipeline.py --full --parallel 8
```

**Scheduled-style run — same data paths, skip pytest (matches cron / `ops/run_prod_data.sh`):**

```bash
python pipeline/run_pipeline.py --full --parallel 8 --skip-tests
# or: bash ops/run_prod_data.sh
```

**Recovery wipe then full fetch (deletes all of `data/processed` and `logs/features` first):**

```bash
python pipeline/run_pipeline.py --prod
```

**Routine `--prod` without deleting prior processed partitions:**

```bash
python pipeline/run_pipeline.py --prod --prod-no-clean
```

**Strict 500-row combined-history gate (modeling only; requires deep historical bootstrap):**

```bash
python pipeline/run_pipeline.py --full --drop-incomplete
```

**Test run (limited tickers, `data/test/` only):**

```bash
python pipeline/run_pipeline.py --test
```

**Daily integrity (smoke — NOT the full S&P job; uses `--daily-integrity` and test dirs):**

```bash
python pipeline/run_pipeline.py --daily-integrity
```

**Weekly integrity (full tests + full pipeline):**

```bash
python pipeline/run_pipeline.py --weekly-integrity
```

Output quality for `--full` / `--prod` is enforced using `output_validation` in [`config/settings.yaml`](../config/settings.yaml). Portable scheduling samples live under [`ops/`](../ops/).

## 📊 Automated Schedule

After `bash scripts/setup_cron.sh`, the default schedule is:

- **4:00 AM Daily**: Production **data** build — [`ops/run_prod_data.sh`](../ops/run_prod_data.sh) → `run_pipeline.py --full --skip-tests` (full tickers, `data/raw` / `data/processed`, log `logs/cron_prod_data.log`)
- **8:00 AM Sundays**: Weekly integrity — [`scripts/run_weekly_tests.py`](../scripts/run_weekly_tests.py) → `--weekly-integrity` (full pytest; log `logs/cron_weekly.log`)
- **2:00 AM Daily**: Test data cleanup (`cleanup_old_reports.py --test-only`)
- **3:00 AM Sundays**: Production data cleanup (30-day retention)
- **Every 15 minutes**: Integrity monitoring

Optional daily smoke (`scripts/run_daily_tests.py`, subset tickers) is **commented out** in the installed crontab; uncomment there if you want it.

### Cron Job Verification
```bash
# Check current cron jobs
crontab -l

# Verify cron configuration
python pipeline/utils/integrity_monitor.py --check-cron

# Test cron setup
bash scripts/test_cron_setup.sh
```

## 🧪 Testing

### Current Test Status
- ✅ **21/21 tests passing** (8m 40s runtime)
- ✅ **Comprehensive validation** of data quality and pipeline integrity
- ✅ **Column format support** for both standard and Alpha Vantage data formats
- ✅ **Metadata validation** with complete tracking fields
- ✅ **Error handling** and recovery mechanisms

### Run All Tests
```bash
python tests/run_all_tests.py
```

### Run Specific Test Categories
```bash
# Quick tests only
pytest tests/ -m quick

# Full test suite
pytest tests/ -m heavy

# Specific test file
pytest tests/test_fetch_data.py
```

### Test Coverage
```bash
pytest tests/ --cov=pipeline --cov-report=html
```

## 🔧 Configuration

### Pipeline Settings (`config/settings.yaml`)
```yaml
# Data sources
data_sources:
  primary: "alpha_vantage"
  secondary: "yfinance"
  
# API configuration
api:
  alpha_vantage_key: "your_key_here"
  rate_limit_delay: 12  # seconds between requests
  
# Processing settings
processing:
  parallel_workers: 4
  batch_size: 50
  retention_days: 30
```

### Test Schedules (`config/test_schedules.yaml`)
```yaml
daily_tests:
  enabled: true
  timeout_minutes: 30
  parallel_workers: 2
  
weekly_tests:
  enabled: true
  timeout_minutes: 120
  parallel_workers: 8
  
notifications:
  enabled: false
  webhook_url: "https://hooks.slack.com/..."
```

## 📈 Monitoring & Reporting

### Integrity Reports
```bash
# Generate latest report
python reports/generate_integrity_report.py --type daily

# Generate weekly report
python reports/generate_integrity_report.py --type weekly --date 2025-07-27
```

### Pipeline Status
```bash
# Check pipeline status
python pipeline/utils/integrity_monitor.py --monitor-pipeline

# Verify cron configuration
python pipeline/utils/integrity_monitor.py --check-cron

# Retry failed runs
python pipeline/utils/integrity_monitor.py --retry-failed
```

## 🌐 Frontend API

### Start API Server
```bash
python reports/api.py --port 8080
```

### Available Endpoints
- `GET /api/status` - Overall pipeline status
- `GET /api/reports/latest` - Latest integrity report
- `GET /api/reports/daily` - Daily reports
- `GET /api/reports/weekly` - Weekly reports
- `GET /api/data/freshness` - Data freshness information
- `GET /api/pipeline/runs` - Recent pipeline runs

### Example API Response
```json
{
  "timestamp": "2025-07-27T14:30:00",
  "status": "success",
  "data": {
    "pipeline_status": {
      "running": false,
      "last_run": "2025-07-27T04:00:00",
      "next_scheduled": "4:00 AM daily (production)"
    },
    "data_status": {
      "raw_data": {
        "latest_partition": "2025-07-27",
        "partition_count": 7,
        "partitions": ["2025-07-21", "2025-07-25", "2025-07-27"]
      }
    }
  }
}
```

## 🔍 Data Structure

### Raw Data (`data/raw/dt=YYYY-MM-DD/`)
- CSV files for each S&P 500 ticker
- **Column formats supported**:
  - Standard: `date, open, high, low, close, volume`
  - Alpha Vantage: `date, 1. open, 2. high, 3. low, 4. close, 5. volume`
- Partitioned by date for efficient querying

### Processed Data (`data/processed/dt=YYYY-MM-DD/`)
- `features.parquet` - Consolidated feature dataset
- Technical indicators and derived features
- Optimized for analysis and modeling

### Logs (`logs/`)
- Structured JSON logs for frontend consumption
- **Enhanced metadata** with comprehensive tracking:
  - `run_date`, `tickers_successful`, `features_generated`
  - `runtime_seconds`, `status`, `error_message`
  - `skipped_tickers`, `rows_dropped_due_to_nans`
- Performance metrics and error tracking

## 🛠️ Development

### Adding New Features
1. Create feature in appropriate pipeline module
2. Add tests in `tests/` directory
3. Update configuration if needed
4. Update documentation

### Code Style
- Follow PEP 8 guidelines
- Use type hints
- Add docstrings for all functions
- Run tests before committing

### Common Utilities
The `pipeline/utils/` module provides:
- `PipelineConfig`: Centralized configuration management
- `DataManager`: Data directory and file operations
- `LogManager`: Standardized logging and metadata
- `ProgressTracker`: Progress bar utilities
- `IntegrityMonitor`: Pipeline monitoring and reporting

## 📋 Maintenance

### Data Cleanup
```bash
# Clean test data only
python scripts/cleanup_old_reports.py --test-only

# Clean production data (30-day retention)
python scripts/cleanup_old_reports.py --pipeline-data --retention-days=30

# Clean everything
python scripts/cleanup_old_reports.py --all
```

### Log Management
```bash
# Manual log rotation
bash scripts/rotate_logs.sh

# View recent logs
tail -f logs/cron_daily.log
```

### Troubleshooting
1. Check cron jobs: `crontab -l`
2. Verify Python environment: `which python`
3. Test pipeline manually: `python pipeline/run_pipeline.py --test`
4. Check integrity: `python pipeline/utils/integrity_monitor.py --check-cron`
5. Run test suite: `python tests/run_all_tests.py`

### Recovery and Maintenance
```bash
# Recover missing historical data
python tools/maintenance/fixes/populate_historical.py

# Check available maintenance scripts
ls tools/maintenance/fixes/

# View maintenance documentation
cat tools/maintenance/fixes/README.md
```

## 🎯 Recent Improvements

### Pipeline Bug Fixes (July 2025)
- ✅ **Fixed missing `--skip-process` argument** in run_pipeline.py argument parser
- ✅ **Improved NaN handling** in process_features.py to preserve data with technical indicators
- ✅ **Added missing dependencies** (fastparquet, tqdm) to requirements.txt
- ✅ **Created maintenance scripts** for historical data recovery and pipeline fixes
- ✅ **Enhanced data validation** and error recovery mechanisms

### Project Consolidation (July 2025)
- ✅ **Reorganized project structure** for better maintainability
- ✅ **Enhanced testing suite** with 21/21 tests passing
- ✅ **Improved metadata generation** with comprehensive tracking
- ✅ **Fixed column format validation** for multiple data sources
- ✅ **Configured production cron jobs** for automated daily runs
- ✅ **Enhanced error handling** and recovery mechanisms

### Key Features
- **Multi-format data support**: Handles both standard and Alpha Vantage CSV formats
- **Comprehensive metadata**: Complete tracking of pipeline execution and data quality
- **Production automation**: Fully automated daily pipeline runs with monitoring
- **Robust testing**: Comprehensive test suite with data validation
- **Integrity monitoring**: Continuous pipeline health monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run full test suite: `python tests/run_all_tests.py`
5. Submit pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review logs in `logs/` directory
3. Run integrity checks: `python pipeline/utils/integrity_monitor.py --check-cron`
4. Run test suite: `python tests/run_all_tests.py`
5. Create an issue with detailed information

---

**Last Updated**: July 27, 2025  
**Version**: 1.0.0  
**Status**: ✅ Production Ready - All tests passing, cron jobs configured  
**Test Status**: 21/21 tests passing (8m 40s runtime)  
**Cron Status**: Configured for daily 4:00 AM production runs 