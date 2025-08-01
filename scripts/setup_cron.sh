#!/bin/bash

# Project Three Data Pipeline - Cron Job Setup
# This script sets up automated cron jobs for the data pipeline

echo "Setting up Project Three Data Pipeline cron jobs..."

# Get the current directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_PATH="$PROJECT_DIR/.venv/bin/python"

# Create the cron configuration
cat > /tmp/project_three_cron << EOF
# Project Three Data Pipeline Environment Variables
PYTHONPATH=$PROJECT_DIR
PIPELINE_MODE=prod
PYTHONUNBUFFERED=1

# Project Three Data Pipeline - Daily Production Run (FULL RUN - NO --test)
# Run daily at 4:00 AM - Fresh production data ready by 6:00 AM
0 4 * * * cd $PROJECT_DIR && $PYTHON_PATH -m pipeline.run_pipeline --daily-integrity >> logs/cron_daily.log 2>&1

# Project Three Data Pipeline - Cleanup Test Data Only
# Run daily at 2:00 AM - Only clears test data, preserves production data for 30 days
0 2 * * * cd $PROJECT_DIR && $PYTHON_PATH scripts/cleanup_old_reports.py --test-only >> logs/cron_cleanup.log 2>&1

# Project Three Data Pipeline - Full Cleanup (30-day retention)
# Run weekly on Sunday at 3:00 AM - Cleans old production data based on 30-day retention
0 3 * * 0 cd $PROJECT_DIR && $PYTHON_PATH scripts/cleanup_old_reports.py --pipeline-data --retention-days=30 >> logs/cron_cleanup.log 2>&1

# Project Three Data Pipeline - Integrity Monitoring
# Run every 15 minutes to check pipeline status
*/15 * * * * cd $PROJECT_DIR && $PYTHON_PATH pipeline/utils/integrity_monitor.py --monitor-pipeline >> logs/cron_monitor.log 2>&1
EOF

# Install the cron jobs
crontab /tmp/project_three_cron

# Clean up temporary file
rm /tmp/project_three_cron

echo "✅ Cron jobs installed successfully!"
echo ""
echo "Current cron jobs:"
crontab -l
echo ""
echo "To verify cron configuration:"
echo "python pipeline/utils/integrity_monitor.py --check-cron" 