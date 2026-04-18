#!/usr/bin/env bash
# Test script to verify cron-related paths and scripts (portable).

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_PATH="$PROJECT_DIR/.venv/bin/python"

echo "Testing Project Three pipeline cron setup..."
echo "PROJECT_DIR=$PROJECT_DIR"

if [ -x "$PYTHON_PATH" ]; then
  echo "✅ Python environment found"
else
  echo "❌ Python environment not found at $PYTHON_PATH"
  exit 1
fi

if [ -f "$PROJECT_DIR/config/test_schedules.yaml" ]; then
  echo "✅ Configuration file found"
else
  echo "❌ Configuration file not found"
  exit 1
fi

if [ -f "$PROJECT_DIR/ops/run_prod_data.sh" ]; then
  echo "✅ ops/run_prod_data.sh found"
else
  echo "❌ ops/run_prod_data.sh not found"
  exit 1
fi

for script in run_daily_tests.py run_weekly_tests.py cleanup_old_reports.py notify_util.py; do
  if [ -f "$PROJECT_DIR/scripts/$script" ]; then
    echo "✅ $script found"
  else
    echo "❌ $script not found"
    exit 1
  fi
done

echo "Verifying cron integrity..."
if grep -q "--test" "$PROJECT_DIR/scripts/run_daily_tests.py" && ! grep -q "--daily-integrity" "$PROJECT_DIR/scripts/run_daily_tests.py"; then
  echo "❌ Daily script contains unexpected --test flag"
  exit 1
fi

if grep -q "--test" "$PROJECT_DIR/scripts/run_weekly_tests.py" && ! grep -q "--weekly-integrity" "$PROJECT_DIR/scripts/run_weekly_tests.py"; then
  echo "❌ Weekly script contains unexpected --test flag"
  exit 1
fi

echo "✅ Cron integrity verified - no --test flags in automated runs"

echo "✅ All tests passed - cron setup is ready!"
