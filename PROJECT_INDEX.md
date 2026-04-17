# Project Three - Stock Pipeline

## 📁 Project Structure

### 🛠️ Tools
- **`tools/diagnostics/`** - Diagnostic and analysis tools
  - `investigate_api_issues.py` - Test API connectivity
  - `archive/2026-04-cleanup/tools/diagnostics/` - Archived one-off diagnostics (`fix_test_suite.py`, `evaluate_bootstrap_failures.py`)

- **`tools/monitoring/`** - Monitoring and dashboard tools
  - `generate_dashboard_report.py` - Generate dashboard reports

- **`tools/maintenance/`** - Maintenance and cleanup tools
  - `terminate_stuck_run.py` - Terminate stuck pipeline runs
  - `bootstrap_historical_data.py` - Bootstrap historical data
  - `bootstrap_yfinance.py` - Primary bootstrap path (recommended)
  - `archive/2026-04-cleanup/tools/maintenance/` - Archived one-off maintenance scripts (`demo_bootstrap.py`, `organize_project.py`)

### 📊 Reports
- **`reports/dashboard/`** - Dashboard reports and summaries
- **`reports/analysis/`** - Analysis reports
- **`reports/status/`** - Status and action reports

### 📁 Data
- **`data/raw/`** - Raw data files
- **`data/processed/`** - Processed data files
- **`data/historical/`** - Historical data files

### 📝 Logs
- **`logs/`** - Pipeline logs
- **`logs/analysis/`** - Analysis logs
- **`logs/monitoring/`** - Monitoring logs

### ⚙️ Configuration
- **`config/`** - Configuration files

### 🔄 Pipeline
- **`pipeline/`** - Main pipeline code

### 🧪 Tests
- **`tests/`** - Test files

### 📚 Documentation
- **`docs/`** - Main documentation
- **`docs/guides/`** - User guides
- **`docs/api/`** - API documentation
- **`docs/troubleshooting/`** - Troubleshooting guides

## 🚀 Quick Start

1. **Check Pipeline Status**: `python tools/monitoring/generate_dashboard_report.py`
2. **Run Diagnostics**: `python tools/diagnostics/investigate_api_issues.py`
3. **View Reports**: Check `reports/dashboard/` for latest reports

## 📋 Recent Actions

- ✅ Archived legacy one-off scripts and snapshot reports
- ✅ Preserved archive manifest for future restoration
- ✅ Kept active monitoring and maintenance paths live

## 🎯 Next Steps

1. Use `tools/maintenance/bootstrap_yfinance.py` for large bootstrap runs
2. Keep `archive/README.md` current as new files are retired
3. Continue docs cleanup as operational workflows evolve

---
*Last Updated: 2026-04-16*
