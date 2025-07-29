#!/usr/bin/env python3
"""
Quick Diagnostics
Run basic diagnostics on the pipeline.
"""

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
