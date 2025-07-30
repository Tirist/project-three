#!/usr/bin/env python3
"""
Quick Diagnostics
Run basic diagnostics on the pipeline.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from tools.diagnostics.investigate_api_issues import main as run_diagnostics
    print("🔍 Running API diagnostics...")
    run_diagnostics()
except ImportError as e:
    print("❌ Diagnostics tools not found")
    print(f"Error: {e}")
    print("Run: python tools/diagnostics/investigate_api_issues.py")
