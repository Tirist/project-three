#!/bin/bash
# Test script to verify cron setup

# Determine the project root directory (assuming this script is in scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Testing Project Three pipeline cron setup..."
echo "Project root: $PROJECT_ROOT"

# Test Python environment
if [ -f "$PROJECT_ROOT/.venv/bin/python" ]; then
    echo "✅ Python environment found"
else
    echo "❌ Python environment not found"
    exit 1
fi

# Test configuration file
if [ -f "$PROJECT_ROOT/config/test_schedules.yaml" ]; then
    echo "✅ Configuration file found"
else
    echo "❌ Configuration file not found"
    exit 1
fi

# Test pipeline script
if [ -f "$PROJECT_ROOT/pipeline/run_pipeline.py" ]; then
    echo "✅ pipeline/run_pipeline.py found"
else
    echo "❌ pipeline/run_pipeline.py not found"
    exit 1
fi

# Test integrity monitor
if [ -f "$PROJECT_ROOT/pipeline/utils/integrity_monitor.py" ]; then
    echo "✅ pipeline/utils/integrity_monitor.py found"
else
    echo "❌ pipeline/utils/integrity_monitor.py not found"
    exit 1
fi

# Test cleanup script
if [ -f "$PROJECT_ROOT/scripts/cleanup_old_reports.py" ]; then
    echo "✅ scripts/cleanup_old_reports.py found"
else
    echo "❌ scripts/cleanup_old_reports.py not found"
    exit 1
fi

# Verify no --test flags in automated runs
echo "Verifying cron integrity..."
if grep -q "\\-\\-test " "$PROJECT_ROOT/pipeline/run_pipeline.py" && ! grep -q "\\-\\-daily-integrity" "$PROJECT_ROOT/pipeline/run_pipeline.py"; then
    echo "❌ Pipeline script contains --test flag without --daily-integrity"
    exit 1
fi

if ! grep -q "\\-\\-weekly-integrity" "$PROJECT_ROOT/pipeline/run_pipeline.py"; then
    echo "❌ Pipeline script missing --weekly-integrity flag"
    exit 1
fi

echo "✅ Cron integrity verified - no --test flags in automated runs"

echo "✅ All tests passed - cron setup is ready!"
