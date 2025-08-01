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

### Option 1: Docker (Recommended)

```bash
# Clone and setup
git clone <repository-url>
cd project-three

# Build and run with Docker
make quickstart

# Or manually:
./scripts/docker-build.sh production latest
./scripts/docker-run.sh pipeline test
```

### Option 2: Local Development

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
python scripts/check_status.py
```

### Option 3: Cloud Storage

```bash
# Use AWS S3
python pipeline/run_pipeline.py --test --storage-provider s3

# Use Google Cloud Storage
python pipeline/run_pipeline.py --test --storage-provider gcs

# Use custom cloud configuration
python pipeline/run_pipeline.py --test --storage-provider s3 --storage-config config/my_cloud.yaml
```

## 📊 Key Features

- **Automated Scheduling**: Daily production runs and weekly integrity checks
- **Incremental Processing**: Efficient handling of historical and current data
- **Comprehensive Testing**: 37+ test cases covering all pipeline components
- **Monitoring & Alerts**: Real-time pipeline status and failure notifications
- **Data Quality**: Integrity reports and validation checks
- **Scalable Architecture**: Partitioned data storage and parallel processing
- **Cloud Storage Support**: Configurable storage backends (local, S3, GCS, Azure)

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
├── docs/             # Full documentation
├── Dockerfile        # Multi-stage Docker configuration
├── docker-compose.yml # Docker Compose services
├── Makefile          # Development shortcuts
└── scripts/          # Docker build and run scripts
```

## 📚 Documentation

For detailed documentation, guides, and API references, see:
- **[📖 Full Documentation](docs/README.md)** - Complete project guide
- **[🐳 Docker Guide](docs/DOCKER_GUIDE.md)** - Containerization and deployment
- **[☁️ Cloud Storage](docs/CLOUD_STORAGE.md)** - Cloud storage configuration and setup
- **[⚙️ Environment Setup](docs/ENVIRONMENT_SETUP.md)** - Development environment configuration
- **[🔄 Refactoring Summary](docs/REFACTORING_SUMMARY.md)** - Recent code refactoring changes
- **[✅ Data Validation](docs/VALIDATION.md)** - Data validation and quality checks
- **[📈 Historical Data Guide](docs/guides/HISTORICAL_DATA_GUIDE.md)** - Working with historical data
- **[🔧 User Guides](docs/guides/)** - Additional step-by-step tutorials
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

- **Status Check**: `python scripts/check_status.py`
- **Diagnostics**: `python scripts/run_diagnostics.py`
- **Cleanup**: `python scripts/cleanup_old_reports.py`

## 📈 Pipeline Status

The pipeline runs automatically:
- **Daily (5:30 PM ET)**: Full production run with integrity checks (after market close)
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