#!/usr/bin/env python3
"""
Quick Status Check
Check pipeline status and recent activity.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from tools.monitoring.generate_dashboard_report import main as generate_report
    print("🚀 Generating dashboard report...")
    generate_report()
except ImportError as e:
    print("❌ Dashboard report generator not found")
    print(f"Error: {e}")
    print("Run: python tools/monitoring/generate_dashboard_report.py")
