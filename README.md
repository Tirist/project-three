# Project Three - Stock Market Data Pipeline

A comprehensive, production-ready data pipeline for automated stock market analysis with real-time monitoring, integrity checks, and dashboard integration.

## 🎯 Overview

This project provides a complete solution for:
- **Automated Data Collection**: Fetching S&P 500 ticker data and historical price information
- **Feature Engineering**: Calculating technical indicators (SMA, EMA, MACD, RSI, Bollinger Bands)
- **Pipeline Orchestration**: Automated scheduling with cron jobs and integrity monitoring
- **Quality Assurance**: Comprehensive testing suite and data validation
- **Dashboard Integration**: API endpoints and reporting for frontend applications

## 🚀 Quick Start

```bash
# Clone and setup
git clone <repository-url>
cd project-three
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run pipeline
python pipeline/run_pipeline.py --test  # Test mode (5 tickers)
python pipeline/run_pipeline.py --prod  # Production mode (503 tickers)

# Check status
python check_status.py
```

## 📊 Key Features

- **Automated Scheduling**: Daily production runs and weekly integrity checks
- **Incremental Processing**: Efficient handling of historical and current data
- **Comprehensive Testing**: 37+ test cases covering all pipeline components
- **Monitoring & Alerts**: Real-time pipeline status and failure notifications
- **Data Quality**: Integrity reports and validation checks
- **Scalable Architecture**: Partitioned data storage and parallel processing

## 📁 Project Structure

```
├── pipeline/          # Core pipeline components
├── reports/           # API endpoints and reporting
├── tests/            # Comprehensive test suite
├── scripts/          # Automation and cron jobs
├── tools/            # Diagnostics and maintenance
├── data/             # Partitioned data storage
├── logs/             # Pipeline logs and metadata
├── config/           # Configuration files
└── docs/             # Full documentation
```

## 📚 Documentation

For detailed documentation, guides, and API references, see:
- **[📖 Full Documentation](docs/README.md)** - Complete project guide
- **[🔧 User Guides](docs/guides/)** - Step-by-step tutorials
- **[🔌 API Reference](docs/api/)** - API documentation
- **[🛠️ Troubleshooting](docs/troubleshooting/)** - Common issues and solutions

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test suite
python -m pytest tests/test_process_features.py -v
```

## 🔧 Maintenance

- **Status Check**: `python check_status.py`
- **Diagnostics**: `python scripts/run_diagnostics.py`
- **Cleanup**: `python scripts/cleanup_old_reports.py`

## 📈 Pipeline Status

The pipeline runs automatically:
- **Daily (4:00 AM)**: Full production run with integrity checks
- **Weekly**: Comprehensive analysis and reporting
- **Continuous**: Real-time monitoring and alerts

## 🤝 Contributing

1. Follow the existing code structure and patterns
2. Add tests for new features
3. Update documentation as needed
4. Run the full test suite before submitting

## 📄 License

[Add your license information here]

---

**For detailed setup instructions, configuration options, and troubleshooting, see the [full documentation](docs/README.md).** 