#!/bin/bash
# Simple log rotation script for Project Three pipeline

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS_DIR="$PROJECT_DIR/logs"
DATE=$(date +%Y%m%d)

# Rotate cron logs
for log_file in "$LOGS_DIR"/cron_*.log; do
    if [ -f "$log_file" ]; then
        mv "$log_file" "$log_file.$DATE"
        gzip "$log_file.$DATE" 2>/dev/null || true
    fi
done

# Keep only last 30 days of rotated logs
find "$LOGS_DIR" -name "cron_*.log.*" -mtime +30 -delete 2>/dev/null || true

echo "Log rotation completed at $(date)"
