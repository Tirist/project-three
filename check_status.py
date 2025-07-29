#!/usr/bin/env python3
"""
Quick Status Check
Check pipeline status and recent activity.
"""

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
