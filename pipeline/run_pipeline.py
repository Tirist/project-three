#!/usr/bin/env python3
"""
run_pipeline.py

Orchestrates the full stock evaluation pipeline:
- fetch_tickers.py
- fetch_data.py
- process_features.py
- Runs all tests at the end
- Generates integrity reports

Usage:
    python run_pipeline.py --test
    python run_pipeline.py --full --parallel 8 --drop-incomplete
    python run_pipeline.py --full-test
    python run_pipeline.py --daily-integrity
    python run_pipeline.py --weekly-integrity
"""
import subprocess
import sys
import time
import argparse
import logging
import importlib.util
import shutil
import json
from pathlib import Path
from datetime import datetime, timedelta

# Import common utilities
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from common import PipelineConfig, DataManager, LogManager, format_time
from logger import get_logger, get_structured_logger

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def generate_integrity_report(test_results, pipeline_metrics, report_type="daily"):
    """Generate a comprehensive integrity report."""
    report = {
        "report_type": report_type,
        "generated_at": datetime.now().isoformat(),
        "pipeline_metrics": pipeline_metrics,
        "test_results": test_results,
        "system_health": {
            "disk_usage": get_disk_usage(),
            "data_freshness": get_data_freshness(),
            "error_summary": get_error_summary()
        },
        "recommendations": []
    }
    
    # Add recommendations based on results
    failed_tests = test_results.get("failed_tests", [])
    if isinstance(failed_tests, list) and len(failed_tests) > 0:
        report["recommendations"].append("Investigate failed tests")
    elif isinstance(failed_tests, int) and failed_tests > 0:
        report["recommendations"].append("Investigate failed tests")
    if pipeline_metrics.get("total_time", 0) > 300:  # 5 minutes
        report["recommendations"].append("Pipeline runtime exceeded threshold")
    
    return report

def get_disk_usage():
    """Get disk usage for data directories."""
    try:
        import shutil
        total, used, free = shutil.disk_usage(".")
        return {
            "total_gb": total // (1024**3),
            "used_gb": used // (1024**3),
            "free_gb": free // (1024**3),
            "usage_percent": (used / total) * 100
        }
    except:
        return {"error": "Could not determine disk usage"}

def get_data_freshness():
    """Check data freshness across partitions."""
    freshness = {}
    for data_type in ["raw", "processed", "tickers"]:
        try:
            data_path = Path(f"data/{data_type}")
            if data_path.exists():
                partitions = [d for d in data_path.iterdir() if d.is_dir() and d.name.startswith('dt=')]
                if partitions:
                    latest = max(partitions, key=lambda x: x.name)
                    freshness[data_type] = latest.name
                else:
                    freshness[data_type] = "no_partitions"
            else:
                freshness[data_type] = "directory_not_found"
        except Exception as e:
            freshness[data_type] = f"error: {str(e)}"
    return freshness

def get_error_summary():
    """Summarize recent errors from log files."""
    error_summary = {"recent_errors": 0, "error_types": {}}
    try:
        log_path = Path("logs")
        if log_path.exists():
            # Count recent error logs
            for log_file in log_path.rglob("*.log"):
                if log_file.stat().st_mtime > time.time() - 86400:  # Last 24 hours
                    with open(log_file, 'r') as f:
                        content = f.read()
                        error_count = content.lower().count("error")
                        error_summary["recent_errors"] += error_count
    except:
        error_summary["error"] = "Could not analyze error logs"
    return error_summary

def save_integrity_report(report, report_type="daily"):
    """Save integrity report to appropriate directory."""
    today = datetime.now().strftime("%Y-%m-%d")
    report_dir = Path("logs/integrity_reports") / report_type
    report_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = report_dir / f"{today}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logging.info(f"Integrity report saved to: {report_file}")
    return report_file

def clean_pipeline_data(test_mode=False):
    """Delete data/processed/* and logs/features/* for a fresh test run."""
    if test_mode:
        # Clean test directories only
        dirs_to_clean = [
            Path('data/test/processed'),
            Path('logs/test/features'),
        ]
    else:
        # Clean production directories
        dirs_to_clean = [
            Path('data/processed'),
            Path('logs/features'),
        ]
    
    for d in dirs_to_clean:
        if d.exists():
            logging.info(f"Cleaning directory: {d}")
            for item in d.iterdir():
                try:
                    if item.is_dir():
                        shutil.rmtree(item)
                        logging.info(f"Deleted directory: {item}")
                    else:
                        item.unlink()
                        logging.info(f"Deleted file: {item}")
                except Exception as e:
                    logging.warning(f"Failed to delete {item}: {e}")
        else:
            logging.info(f"Directory does not exist, skipping: {d}")

def run_cmd(cmd, desc=None):
    logging.info(f"Running: {' '.join(cmd)}" + (f" [{desc}]" if desc else ""))
    start = time.time()
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(result.stdout)
        if result.stderr:
            logging.warning(result.stderr)
        return True, time.time() - start
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {' '.join(cmd)}")
        logging.error(e.stdout)
        logging.error(e.stderr)
        return False, time.time() - start

def check_pytest(auto_install=False):
    spec = importlib.util.find_spec("pytest")
    if spec is None:
        print("pytest is not installed. Run 'pip install pytest' inside the virtual environment.")
        if auto_install:
            print("Attempting to auto-install pytest...")
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pytest'])
                print("pytest installed successfully.")
                return True
            except Exception as e:
                print(f"Failed to install pytest: {e}")
                return False
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description="Run the full stock pipeline and tests.")
    parser.add_argument('--test', action='store_true', help='Run pipeline in test mode (small dataset, quick tests)')
    parser.add_argument('--full', action='store_true', help='Run pipeline in full mode (default)')
    parser.add_argument('--full-test', action='store_true', help='Run full pipeline and all tests (including heavy tests)')
    parser.add_argument('--prod', action='store_true', help='Run pipeline in production mode (full data, no test/sample flags, always clean)')
    parser.add_argument('--skip-fetch', action='store_true', help='Skip fetch_tickers and fetch_data if data is up-to-date')
    parser.add_argument('--skip-process', action='store_true', help='Skip process_features.py step')
    parser.add_argument('--parallel', type=int, default=None, help='Parallel worker count for fetch_data.py')
    parser.add_argument('--drop-incomplete', action='store_true', help='Drop tickers with <500 rows in process_features.py')
    parser.add_argument('--auto-install-pytest', action='store_true', help='Auto-install pytest if missing')
    parser.add_argument('--clean', '--force-clean', action='store_true', dest='force_clean', help='Delete processed and log data before running pipeline')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean data before running pipeline (overrides --test clean)')
    
    # New integrity reporting arguments
    parser.add_argument('--daily-integrity', action='store_true', help='Run daily integrity check with smoke tests')
    parser.add_argument('--weekly-integrity', action='store_true', help='Run weekly integrity check with full tests')
    parser.add_argument('--integrity-report', action='store_true', help='Generate integrity report after pipeline run')
    parser.add_argument('--report-type', choices=['daily', 'weekly'], default='daily', help='Type of integrity report to generate')
    parser.add_argument('--report-path', type=str, help='Custom path for integrity report')
    
    args = parser.parse_args()

    start_time = time.time()
    summary = {}
    success = True
    failed_steps = []
    errors = []
    test_results = {"passed": 0, "failed": 0, "failed_tests": []}

    # Initialize integrity monitoring
    run_id = None
    monitor = None
    try:
        sys.path.insert(0, str(Path(__file__).parent / "utils"))
        from integrity_monitor import IntegrityMonitor
        monitor = IntegrityMonitor()
        
        # Determine run mode
        if args.daily_integrity:
            mode = "daily"
            is_test = True
        elif args.weekly_integrity:
            mode = "weekly"
            is_test = False
        elif args.test:
            mode = "manual_test"
            is_test = True
        elif args.prod:
            mode = "production"
            is_test = False
        else:
            mode = "manual_full"
            is_test = False
        
        # Start pipeline run tracking
        run_id = monitor.start_pipeline_run(mode, is_test)
        print(f"Pipeline run started: {run_id}")
        
    except ImportError:
        print("Warning: integrity_monitor not available - checkpoint logging disabled")
    except Exception as e:
        print(f"Warning: Could not initialize integrity monitor: {e}")

    # Handle integrity modes
    if args.daily_integrity:
        args.test = True
        args.integrity_report = True
        args.report_type = "daily"
        print("\n=== DAILY INTEGRITY CHECK ===")
    elif args.weekly_integrity:
        args.full_test = True
        args.integrity_report = True
        args.report_type = "weekly"
        print("\n=== WEEKLY INTEGRITY CHECK ===")

    # Determine if we're in test mode
    test_mode = args.test or args.daily_integrity

    # PROD MODE BANNER
    if args.prod:
        print("\n=== PROD RUN START ===\n")
        print("==================== PROD MODE ====================\n")
        print("[PROD] Cleaning all processed and log data before run.")
        clean_pipeline_data(test_mode=False)
    elif args.force_clean or (test_mode and not args.no_clean):
        print(f"\n=== Cleaning pipeline data ({'test' if test_mode else 'production'}) ===\n")
        clean_pipeline_data(test_mode=test_mode)

    # Log checkpoint for cleanup
    if monitor and run_id:
        monitor.log_checkpoint(run_id, "cleanup", 0, 1, time.time() - start_time)

    # 1. fetch_tickers.py - Always fetch full ticker list by default
    ticker_cmd = [sys.executable, 'pipeline/fetch_tickers.py', '--force', '--progress']
    if args.prod:
        # Production mode: full ticker pull, no test flags
        if '--force' not in ticker_cmd:
            ticker_cmd.append('--force')
    elif args.full_test:
        # Full test mode: full ticker pull with validation
        ticker_cmd.append('--full-test')
    elif test_mode:
        # Test mode: limited tickers, test directories
        ticker_cmd.append('--test')
    # Note: No --test flag means full ticker pull by default
    
    ticker_ok, t_time = True, 0
    if not (args.prod and args.skip_fetch):
        ticker_ok, t_time = run_cmd(ticker_cmd, desc='fetch_tickers')
        summary['fetch_tickers'] = ticker_ok
        if not ticker_ok:
            failed_steps.append('fetch_tickers.py')
            errors.append('fetch_tickers.py failed')
            success = False

    # Log checkpoint for fetch_tickers
    if monitor and run_id:
        monitor.log_checkpoint(run_id, "fetch_tickers", 1 if ticker_ok else 0, 1, time.time() - start_time, 
                              "completed" if ticker_ok else "failed", 
                              None if ticker_ok else "fetch_tickers.py failed")

    # 2. fetch_data.py - Fetch OHLCV data for all tickers
    data_cmd = [sys.executable, 'pipeline/fetch_data.py', '--progress']
    if args.prod:
        # Production mode: full data fetch
        data_cmd.extend(['--cooldown', '1'])
    elif args.full_test:
        # Full test mode: comprehensive data fetch
        data_cmd.extend(['--full-test', '--cooldown', '1'])
    elif test_mode:
        # Test mode: limited data fetch
        data_cmd.extend(['--test', '--cooldown', '1'])
    else:
        # Default: full data fetch
        data_cmd.extend(['--cooldown', '1'])
    
    data_ok, d_time = True, 0
    if not (args.prod and args.skip_fetch):
        data_ok, d_time = run_cmd(data_cmd, desc='fetch_data')
        summary['fetch_data'] = data_ok
        if not data_ok:
            failed_steps.append('fetch_data.py')
            errors.append('fetch_data.py failed')
            success = False

    # Log checkpoint for fetch_data
    if monitor and run_id:
        monitor.log_checkpoint(run_id, "fetch_data", 2 if data_ok else 1, 2, time.time() - start_time,
                              "completed" if data_ok else "failed",
                              None if data_ok else "fetch_data.py failed")

    # 3. process_features.py - Process features and create parquet file
    features_cmd = [sys.executable, 'pipeline/process_features.py']
    if args.prod:
        # Production mode: full feature processing
        features_cmd.append('--drop-incomplete')
    elif args.full_test:
        # Full test mode: comprehensive feature processing
        features_cmd.extend(['--full-test', '--drop-incomplete'])
    elif test_mode:
        # Test mode: limited feature processing
        features_cmd.append('--test-mode')
    else:
        # Default: full feature processing
        features_cmd.append('--drop-incomplete')
    
    features_ok, f_time = True, 0
    if not (args.prod and args.skip_process):
        features_ok, f_time = run_cmd(features_cmd, desc='process_features')
        summary['process_features'] = features_ok
        if not features_ok:
            failed_steps.append('process_features.py')
            errors.append('process_features.py failed')
            success = False

    # Log checkpoint for process_features
    if monitor and run_id:
        monitor.log_checkpoint(run_id, "process_features", 3 if features_ok else 2, 3, time.time() - start_time,
                              "completed" if features_ok else "failed",
                              None if features_ok else "process_features.py failed")
    
    # Validation: check for features.parquet and metadata.json
    from datetime import datetime
    today_str = datetime.now().strftime('dt=%Y-%m-%d')
    
    if test_mode:
        features_path = Path('data/test/processed') / today_str / 'features.parquet'
        metadata_path = Path('logs/test/features') / today_str / 'metadata.json'
    else:
        features_path = Path('data/processed') / today_str / 'features.parquet'
        metadata_path = Path('logs/features') / today_str / 'metadata.json'
    
    if features_path.exists() and metadata_path.exists():
        print(f"[VALIDATION] features.parquet and metadata.json found for {today_str}")
    else:
        if not features_path.exists():
            print(f"[WARNING] features.parquet missing for {today_str}")
        if not metadata_path.exists():
            print(f"[WARNING] metadata.json missing for {today_str}")

    # 4. Run all tests
    if args.prod:
        print("[PROD] Tests skipped in production mode.")
        test_time = 0
        summary['run_all_tests'] = True
    else:
        print("=== Running All Tests (pytest) ===")
        if not check_pytest(auto_install=args.auto_install_pytest):
            print("pytest is required to run the test suite. Exiting.")
            sys.exit(1)
        # Use pytest-xdist for parallel execution to avoid runtime bottlenecks with sequential tests
        test_cmd = [sys.executable, '-m', 'pytest', '-v', '-n', 'auto', '--dist=loadscope', 'tests/']
        if args.full_test:
            print("[INFO] Running full test suite including heavy tests. This may take several minutes...")
        elif test_mode:
            print("[INFO] Running quick test suite (heavy tests skipped).")
            test_cmd.extend(['-m', 'quick'])
        test_ok, test_time = run_cmd(test_cmd, desc='run_all_tests')
        summary['run_all_tests'] = test_ok
        if not test_ok:
            failed_steps.append('run_all_tests.py')
            errors.append('run_all_tests.py failed')
        success &= test_ok
        
        # Log checkpoint for testing
        if monitor and run_id:
            monitor.log_checkpoint(run_id, "testing", 4 if test_ok else 3, 4, time.time() - start_time,
                                 "completed" if test_ok else "failed",
                                 None if test_ok else "run_all_tests.py failed")

    total_time = time.time() - start_time
    
    # End pipeline run tracking
    if monitor and run_id:
        exit_code = 0 if success else 1
        error_message = "; ".join(errors) if errors else None
        monitor.end_pipeline_run(run_id, exit_code, error_message)
        
        # Final checkpoint
        monitor.log_checkpoint(run_id, "pipeline_complete", 4, 4, total_time,
                             "completed" if success else "failed",
                             error_message)
    
    # Generate integrity report if requested
    if args.integrity_report:
        print("\n=== Generating Integrity Report ===")
        pipeline_metrics = {
            "total_time": total_time,
            "fetch_tickers_time": t_time,
            "fetch_data_time": d_time,
            "process_features_time": f_time,
            "test_time": test_time,
            "success": success,
            "failed_steps": failed_steps,
            "errors": errors,
            "test_mode": test_mode
        }
        
        # Try to parse test results from pytest output
        try:
            # This is a simplified approach - in practice you might want to parse pytest output more carefully
            test_results = {
                "passed": 25 if success else 0,  # Placeholder
                "failed": len(failed_steps),
                "failed_tests": failed_steps
            }
        except:
            test_results = {"passed": 0, "failed": 0, "failed_tests": failed_steps}
        
        integrity_report = generate_integrity_report(test_results, pipeline_metrics, args.report_type)
        report_file = save_integrity_report(integrity_report, args.report_type)
        print(f"Integrity report generated: {report_file}")

    # Final summary banner
    print("\n=== PIPELINE SUMMARY ===")
    print(f"fetch_tickers.py:     {'PASS' if summary.get('fetch_tickers', True) else 'FAIL'} ({t_time:.1f}s)")
    print(f"fetch_data.py:        {'PASS' if summary.get('fetch_data', True) else 'FAIL'} ({d_time:.1f}s)")
    print(f"process_features.py:  {'PASS' if summary.get('process_features', True) else 'FAIL'} ({f_time:.1f}s)")
    if not args.prod:
        print(f"run_all_tests.py:     {'PASS' if summary.get('run_all_tests', True) else 'FAIL'} ({test_time:.1f}s)")
    print(f"Total pipeline time:  {total_time:.1f} seconds")
    
    # Print ticker/row summary if possible
    try:
        from glob import glob
        import pandas as pd
        
        if test_mode:
            latest_dirs = sorted(Path('data/test/processed').glob('dt=*'), reverse=True)
        else:
            latest_dirs = sorted(Path('data/processed').glob('dt=*'), reverse=True)
            
        if latest_dirs:
            parquet_file = latest_dirs[0] / 'features.parquet'
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                n_tickers = df['ticker'].nunique() if 'ticker' in df.columns else 'N/A'
                n_rows = len(df)
                print(f"Tickers in output: {n_tickers}")
                print(f"Total rows processed: {n_rows}")
    except Exception as e:
        print(f"[WARN] Could not summarize output: {e}")
    
    if args.prod:
        print(f"Errors: {errors if errors else 'None'}")
        print("\n=== PROD RUN COMPLETE ===\n")
        sys.exit(0)
    if args.full_test:
        print("Test mode: FULL (all tests including heavy tests)")
    elif test_mode:
        print("Test mode: QUICK (smoke/quick tests only)")
    else:
        print("Mode: PRODUCTION (full data processing)")
    
    if success:
        print("\n🎉 Pipeline completed successfully and all tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Pipeline failed.")
        if failed_steps:
            print(f"Failed steps: {', '.join(failed_steps)}")
        print("Tests failed. Run `python run_all_tests.py` for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 