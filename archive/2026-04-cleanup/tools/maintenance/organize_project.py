#!/usr/bin/env python3
"""
Project Organization Script
Organize the project structure by consolidating files into appropriate directories.
"""

import shutil
import os
from pathlib import Path
from datetime import datetime

def create_directory_structure():
    """Create the organized directory structure."""
    
    # Define the new directory structure
    directories = [
        "tools/",                    # Utility scripts and tools
        "tools/diagnostics/",        # Diagnostic and analysis tools
        "tools/monitoring/",         # Monitoring and dashboard tools
        "tools/maintenance/",        # Maintenance and cleanup tools
        "reports/",                  # Generated reports (already exists)
        "reports/dashboard/",        # Dashboard reports
        "reports/analysis/",         # Analysis reports
        "reports/status/",           # Status and action reports
        "data/",                     # Data directory (already exists)
        "data/raw/",                 # Raw data (already exists)
        "data/processed/",           # Processed data (already exists)
        "data/historical/",          # Historical data
        "logs/",                     # Logs directory (already exists)
        "logs/analysis/",            # Analysis logs
        "logs/monitoring/",          # Monitoring logs
        "config/",                   # Config directory (already exists)
        "pipeline/",                 # Pipeline directory (already exists)
        "tests/",                    # Tests directory (already exists)
        "scripts/",                  # Scripts directory (already exists)
        "docs/",                     # Documentation (already exists)
        "docs/guides/",              # User guides
        "docs/api/",                 # API documentation
        "docs/troubleshooting/",     # Troubleshooting guides
    ]
    
    # Create directories
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def move_files_to_organized_structure():
    """Move files to their appropriate locations in the organized structure."""
    
    # Define file movements
    file_movements = {
        # Diagnostic and analysis tools
        "evaluate_bootstrap_failures.py": "tools/diagnostics/",
        "investigate_api_issues.py": "tools/diagnostics/",
        "fix_test_suite.py": "tools/diagnostics/",
        
        # Monitoring and dashboard tools
        "generate_dashboard_report.py": "tools/monitoring/",
        "dashboard_report.json": "reports/dashboard/",
        "dashboard_api_summary.json": "reports/dashboard/",
        "DASHBOARD_SUMMARY.md": "reports/dashboard/",
        
        # Maintenance tools
        "terminate_stuck_run.py": "tools/maintenance/",
        
        # Status and action reports
        "IMMEDIATE_ACTION_PLAN.md": "reports/status/",
        "IMMEDIATE_ACTIONS_SUMMARY.md": "reports/status/",
        
        # Historical data tools
        "bootstrap_historical_data.py": "tools/maintenance/",
        "demo_bootstrap.py": "tools/maintenance/",
        "HISTORICAL_DATA_GUIDE.md": "docs/guides/",
        
        # Documentation
        "README.md": "docs/",
    }
    
    # Move files
    for file_path, destination in file_movements.items():
        source = Path(file_path)
        dest = Path(destination) / source.name
        
        if source.exists():
            try:
                # Create destination directory if it doesn't exist
                dest.parent.mkdir(parents=True, exist_ok=True)
                
                # Move the file
                shutil.move(str(source), str(dest))
                print(f"✅ Moved {file_path} → {destination}")
            except Exception as e:
                print(f"❌ Failed to move {file_path}: {e}")
        else:
            print(f"⚠️ File not found: {file_path}")

def create_project_index():
    """Create a project index file to help navigate the organized structure."""
    
    index_content = """# Project Three - Stock Pipeline

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
  - `bootstrap_historical_data.py` - Bootstrap historical data
  - `demo_bootstrap.py` - Demo bootstrap functionality

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

1. Fix bootstrap rate limiting issues
2. Optimize pipeline performance
3. Implement monitoring system

---
*Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    with open("PROJECT_INDEX.md", "w") as f:
        f.write(index_content)
    
    print("✅ Created PROJECT_INDEX.md")

def create_quick_access_scripts():
    """Create quick access scripts for common operations."""
    
    # Create a quick status check script
    status_script = """#!/usr/bin/env python3
\"\"\"
Quick Status Check
Check pipeline status and recent activity.
\"\"\"

import sys
from pathlib import Path

# Add tools to path
sys.path.insert(0, str(Path(__file__).parent / "tools" / "monitoring"))

try:
    from generate_dashboard_report import main as generate_report
    print("🚀 Generating dashboard report...")
    generate_report()
except ImportError:
    print("❌ Dashboard report generator not found")
    print("Run: python tools/monitoring/generate_dashboard_report.py")
"""
    
    with open("check_status.py", "w") as f:
        f.write(status_script)
    
    # Create a quick diagnostics script
    diagnostics_script = """#!/usr/bin/env python3
\"\"\"
Quick Diagnostics
Run basic diagnostics on the pipeline.
\"\"\"

import sys
from pathlib import Path

# Add tools to path
sys.path.insert(0, str(Path(__file__).parent / "tools" / "diagnostics"))

try:
    from investigate_api_issues import main as run_diagnostics
    print("🔍 Running API diagnostics...")
    run_diagnostics()
except ImportError:
    print("❌ Diagnostics tools not found")
    print("Run: python tools/diagnostics/investigate_api_issues.py")
"""
    
    with open("run_diagnostics.py", "w") as f:
        f.write(diagnostics_script)
    
    # Make scripts executable
    os.chmod("check_status.py", 0o755)
    os.chmod("run_diagnostics.py", 0o755)
    
    print("✅ Created quick access scripts: check_status.py, run_diagnostics.py")

def cleanup_temp_files():
    """Clean up temporary and cache files."""
    
    # Remove cache directories
    cache_dirs = [".pytest_cache", "__pycache__"]
    for cache_dir in cache_dirs:
        if Path(cache_dir).exists():
            shutil.rmtree(cache_dir)
            print(f"✅ Removed cache directory: {cache_dir}")
    
    # Remove .DS_Store files
    for ds_store in Path(".").glob("**/.DS_Store"):
        ds_store.unlink()
        print(f"✅ Removed: {ds_store}")

def main():
    """Main function to organize the project."""
    print("🚀 Organizing project structure...\n")
    
    # Create directory structure
    print("📁 Creating directory structure...")
    create_directory_structure()
    print()
    
    # Move files to organized structure
    print("📦 Moving files to organized structure...")
    move_files_to_organized_structure()
    print()
    
    # Create project index
    print("📋 Creating project index...")
    create_project_index()
    print()
    
    # Create quick access scripts
    print("⚡ Creating quick access scripts...")
    create_quick_access_scripts()
    print()
    
    # Clean up temp files
    print("🧹 Cleaning up temporary files...")
    cleanup_temp_files()
    print()
    
    print("🎉 Project organization completed!")
    print("\n📖 Check PROJECT_INDEX.md for the new structure")
    print("⚡ Use check_status.py and run_diagnostics.py for quick access")

if __name__ == "__main__":
    main()
