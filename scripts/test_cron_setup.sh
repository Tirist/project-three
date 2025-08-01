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

# Test scripts
for script in run_integrity_smoke_tests.py run_weekly_tests.py cleanup_old_reports.py; do
    if [ -f "$PROJECT_ROOT/scripts/$script" ]; then
        echo "✅ $script found"
    else
        echo "❌ $script not found"
        exit 1
    fi
done

# Verify no --test flags in automated runs
echo "Verifying cron integrity..."
if grep -q "\\-\\-test" "$PROJECT_ROOT/scripts/run_integrity_smoke_tests.py" && ! grep -q "\\-\\-daily-integrity" "$PROJECT_ROOT/scripts/run_integrity_smoke_tests.py"; then
    echo "❌ Daily script contains --test flag"
    exit 1
fi

if grep -q "\\-\\-test" "$PROJECT_ROOT/scripts/run_weekly_tests.py" && ! grep -q "\\-\\-weekly-integrity" "$PROJECT_ROOT/scripts/run_weekly_tests.py"; then
    echo "❌ Weekly script contains --test flag"
    exit 1
fi

echo "✅ Cron integrity verified - no --test flags in automated runs"

echo "✅ All tests passed - cron setup is ready!"
