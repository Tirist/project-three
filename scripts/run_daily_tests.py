#!/usr/bin/env python3
"""
run_daily_tests.py

Runs daily integrity checks with smoke tests (5-10 tickers).
Generates daily integrity reports in logs/integrity_reports/daily/.

Usage:
    python scripts/run_daily_tests.py
"""

import subprocess
import sys
import logging
import yaml
import signal
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/cron_daily.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class TimeoutError(Exception):
    """Custom timeout exception."""
    pass

def timeout_handler(signum, frame):
    """Handle timeout signal."""
    raise TimeoutError("Operation timed out")

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    config_path = Path("config/test_schedules.yaml")
    if not config_path.exists():
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def run_with_timeout(cmd: list, timeout_seconds: int, desc: str = "") -> tuple[bool, float, str]:
    """Run command with timeout and return success, runtime, and output."""
    start_time = time.time()
    
    try:
        # Set up timeout handler
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_seconds)
        
        logging.info(f"Running: {' '.join(cmd)} [{desc}]")
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=timeout_seconds
        )
        
        # Cancel timeout
        signal.alarm(0)
        
        runtime = time.time() - start_time
        
        if result.returncode == 0:
            logging.info(f"✅ {desc} completed successfully in {runtime:.2f}s")
            return True, runtime, result.stdout
        else:
            logging.error(f"❌ {desc} failed with return code {result.returncode}")
            logging.error(f"STDOUT: {result.stdout}")
            logging.error(f"STDERR: {result.stderr}")
            return False, runtime, result.stderr
            
    except TimeoutError:
        logging.error(f"❌ {desc} timed out after {timeout_seconds}s")
        return False, timeout_seconds, "TIMEOUT"
    except subprocess.TimeoutExpired:
        logging.error(f"❌ {desc} timed out after {timeout_seconds}s")
        return False, timeout_seconds, "TIMEOUT"
    except Exception as e:
        logging.error(f"❌ {desc} failed with exception: {e}")
        return False, time.time() - start_time, str(e)
    finally:
        signal.alarm(0)

def send_notification(message: str, config: Dict[str, Any]) -> None:
    """Send notification on failure."""
    if not config.get('notifications', {}).get('enabled', False):
        return
    
    # Simple webhook notification (can be extended for email/Slack)
    webhook_url = config.get('notifications', {}).get('webhook_url')
    if webhook_url:
        try:
            import requests
            payload = {"text": f"🚨 Daily Test Failure: {message}"}
            requests.post(webhook_url, json=payload, timeout=10)
            logging.info("Notification sent")
        except Exception as e:
            logging.warning(f"Failed to send notification: {e}")

def main() -> int:
    """Run daily integrity tests."""
    start_time = time.time()
    
    # Load configuration
    config = load_config()
    daily_config = config.get('daily_tests', {})
    
    if not daily_config.get('enabled', True):
        logging.info("Daily tests disabled in configuration")
        return 0
    
    print("=== DAILY INTEGRITY TEST RUN ===")
    print(f"Started at: {datetime.now().isoformat()}")
    
    # Create logs directory if it doesn't exist
    Path("logs").mkdir(exist_ok=True)
    
    # Get configuration values
    timeout_minutes = daily_config.get('timeout_minutes', 30)
    parallel_workers = daily_config.get('parallel_workers', 4)
    timeout_seconds = timeout_minutes * 60
    
    # Run pipeline with daily integrity mode
    cmd = [
        sys.executable, "pipeline/run_pipeline.py",
        "--daily-integrity",
        "--parallel", str(parallel_workers),
        "--clean"
    ]
    
    success, runtime, output = run_with_timeout(
        cmd, 
        timeout_seconds, 
        "Daily Integrity Pipeline"
    )
    
    total_time = time.time() - start_time
    
    # Log results
    if success:
        logging.info(f"✅ Daily integrity test completed successfully in {total_time:.2f}s")
        print("✅ Daily integrity test completed successfully")
        print(output)
        return 0
    else:
        error_msg = f"Daily integrity test failed after {total_time:.2f}s"
        logging.error(error_msg)
        print("❌ Daily integrity test failed")
        print(f"Error: {output}")
        
        # Send notification on failure
        send_notification(error_msg, config)
        
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logging.info("Daily test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error in daily test: {e}")
        sys.exit(1) 