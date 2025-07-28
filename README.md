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
├── scripts/                    # Automation scripts
│   ├── run_daily_tests.py      # Daily smoke tests
│   ├── run_weekly_tests.py     # Weekly full tests
│   ├── cleanup_old_reports.py  # Data retention management
│   └── setup_cron.sh          # Cron job setup
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

### Manual Pipeline Run
```bash
# Full production run (all 503 S&P 500 tickers)
python pipeline/run_pipeline.py --prod

# Test run (5 tickers, isolated test data)
python pipeline/run_pipeline.py --test

# Daily integrity test
python pipeline/run_pipeline.py --daily-integrity

# Weekly integrity test
python pipeline/run_pipeline.py --weekly-integrity
```

## 📊 Automated Schedule

The pipeline runs automatically with the following schedule:

- **4:00 AM Daily**: Full production run (503 tickers)
- **2:00 AM Daily**: Test data cleanup
- **3:00 AM Sundays**: Production data cleanup (30-day retention)
- **Every 15 minutes**: Integrity monitoring

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

## 🧪 Testing

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
- Columns: Date, Open, High, Low, Close, Volume
- Partitioned by date for efficient querying

### Processed Data (`data/processed/dt=YYYY-MM-DD/`)
- `features.parquet` - Consolidated feature dataset
- Technical indicators and derived features
- Optimized for analysis and modeling

### Logs (`logs/`)
- Structured JSON logs for frontend consumption
- Metadata for each pipeline stage
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

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run full test suite
5. Submit pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review logs in `logs/` directory
3. Run integrity checks
4. Create an issue with detailed information

---

**Last Updated**: July 27, 2025  
**Version**: 1.0.0  
**Status**: Production Ready 