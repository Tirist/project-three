#!/bin/bash
# setup_cron.sh
# Sets up automated cron jobs for daily/weekly testing and cleanup
# Includes environment variables, log rotation, and proper error handling

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the absolute path to the project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_PATH="$PROJECT_DIR/.venv/bin/python"

echo -e "${GREEN}Setting up cron jobs for Project Three data pipeline...${NC}"
echo "Project directory: $PROJECT_DIR"
echo "Python path: $PYTHON_PATH"

# Check if virtual environment exists
if [ ! -f "$PYTHON_PATH" ]; then
    echo -e "${RED}Error: Python virtual environment not found at $PYTHON_PATH${NC}"
    echo "Please run: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p "$PROJECT_DIR/logs"

# Create a temporary file for the crontab
TEMP_CRON=$(mktemp)

# Export current crontab
crontab -l > "$TEMP_CRON" 2>/dev/null || echo "" > "$TEMP_CRON"

# Add environment variables and new cron jobs
cat >> "$TEMP_CRON" << EOF

# Project Three Data Pipeline Environment Variables
PYTHONPATH=$PROJECT_DIR
PIPELINE_MODE=prod
PYTHONUNBUFFERED=1

# Project Three Data Pipeline - Daily Integrity Tests
# Run daily at 6:00 AM
0 6 * * * cd $PROJECT_DIR && $PYTHON_PATH scripts/run_daily_tests.py >> logs/cron_daily.log 2>&1

# Project Three Data Pipeline - Weekly Integrity Tests  
# Run weekly on Sunday at 8:00 AM
0 8 * * 0 cd $PROJECT_DIR && $PYTHON_PATH scripts/run_weekly_tests.py >> logs/cron_weekly.log 2>&1

# Project Three Data Pipeline - Cleanup Old Reports
# Run daily at 2:00 AM
0 2 * * * cd $PROJECT_DIR && $PYTHON_PATH scripts/cleanup_old_reports.py >> logs/cron_cleanup.log 2>&1

EOF

# Install the new crontab
if crontab "$TEMP_CRON"; then
    echo -e "${GREEN}✅ Cron jobs installed successfully!${NC}"
else
    echo -e "${RED}❌ Failed to install cron jobs${NC}"
    rm "$TEMP_CRON"
    exit 1
fi

# Clean up temporary file
rm "$TEMP_CRON"

# Create logrotate configuration
LOGROTATE_CONF="/etc/logrotate.d/project-three-pipeline"
if [ -w "/etc/logrotate.d" ]; then
    cat > "$LOGROTATE_CONF" << EOF
$PROJECT_DIR/logs/cron_*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $(whoami) $(whoami)
    postrotate
        # Restart any services if needed
        echo "Log rotated for Project Three pipeline"
    endscript
}
EOF
    echo -e "${GREEN}✅ Logrotate configuration created at $LOGROTATE_CONF${NC}"
else
    echo -e "${YELLOW}⚠️  Cannot create logrotate config (requires sudo). Manual setup required.${NC}"
    echo "Create $LOGROTATE_CONF with the following content:"
    cat << EOF
$PROJECT_DIR/logs/cron_*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $(whoami) $(whoami)
}
EOF
fi

# Create a simple log rotation script as backup
ROTATE_SCRIPT="$PROJECT_DIR/scripts/rotate_logs.sh"
cat > "$ROTATE_SCRIPT" << 'EOF'
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
EOF

chmod +x "$ROTATE_SCRIPT"
echo -e "${GREEN}✅ Log rotation script created at $ROTATE_SCRIPT${NC}"

# Create a test script to verify setup
TEST_SCRIPT="$PROJECT_DIR/scripts/test_cron_setup.sh"
cat > "$TEST_SCRIPT" << EOF
#!/bin/bash
# Test script to verify cron setup

echo "Testing Project Three pipeline cron setup..."

# Test Python environment
if [ -f "$PYTHON_PATH" ]; then
    echo "✅ Python environment found"
else
    echo "❌ Python environment not found"
    exit 1
fi

# Test configuration file
if [ -f "$PROJECT_DIR/config/test_schedules.yaml" ]; then
    echo "✅ Configuration file found"
else
    echo "❌ Configuration file not found"
    exit 1
fi

# Test scripts
for script in run_daily_tests.py run_weekly_tests.py cleanup_old_reports.py; do
    if [ -f "$PROJECT_DIR/scripts/\$script" ]; then
        echo "✅ \$script found"
    else
        echo "❌ \$script not found"
        exit 1
    fi
done

echo "✅ All tests passed - cron setup is ready!"
EOF

chmod +x "$TEST_SCRIPT"
echo -e "${GREEN}✅ Test script created at $TEST_SCRIPT${NC}"

echo ""
echo -e "${GREEN}=== CRON SETUP COMPLETE ===${NC}"
echo ""
echo "Installed jobs:"
echo "  - Daily integrity tests: 6:00 AM daily"
echo "  - Weekly integrity tests: 8:00 AM Sundays"  
echo "  - Cleanup old reports: 2:00 AM daily"
echo ""
echo "Environment variables set:"
echo "  - PYTHONPATH=$PROJECT_DIR"
echo "  - PIPELINE_MODE=prod"
echo "  - PYTHONUNBUFFERED=1"
echo ""
echo "Log files will be written to:"
echo "  - logs/cron_daily.log"
echo "  - logs/cron_weekly.log"
echo "  - logs/cron_cleanup.log"
echo ""
echo "Log rotation:"
echo "  - Automatic via logrotate (if sudo access)"
echo "  - Manual via scripts/rotate_logs.sh"
echo ""
echo "Useful commands:"
echo "  - View current cron jobs: crontab -l"
echo "  - Edit cron jobs: crontab -e"
echo "  - Remove all cron jobs: crontab -r"
echo "  - Test setup: $TEST_SCRIPT"
echo "  - Manual log rotation: $ROTATE_SCRIPT"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Run: $TEST_SCRIPT"
echo "2. Check logs after first run"
echo "3. Configure notifications in config/test_schedules.yaml" 