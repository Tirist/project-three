# Project Three - Stock Pipeline

## 📁 Project Structure

### 🛠️ Tools
- **`tools/diagnostics/`** - Diagnostic and analysis tools
  - `evaluate_bootstrap_failures.py` - Analyze bootstrap job failures
  - `investigate_api_issues.py` - Test API connectivity
  - `fix_test_suite.py` - Fix test suite issues

- **`tools/monitoring/`** - Monitoring and dashboard tools
  - `generate_dashboard_report.py` - Generate dashboard reports

- **`tools/maintenance/`** - Maintenance and cleanup tools
  - `terminate_stuck_run.py` - Terminate stuck pipeline runs
  - `fill_data_gaps.py` - Check and fill data gaps using pipeline
  - `organize_project.py` - Organize project structure

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

- ✅ Terminated stuck weekly run
- ✅ Fixed test suite issues
- ✅ Investigated API connectivity
- ✅ Organized project structure

## 🎯 Next Steps

1. Use pipeline for data collection (not bootstrap scripts)
2. Optimize pipeline performance
3. Implement monitoring system

---
*Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
