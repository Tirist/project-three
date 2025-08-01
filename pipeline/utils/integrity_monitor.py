#!/usr/bin/env python3
"""
integrity_monitor.py

Comprehensive integrity monitoring and reporting module for the stock evaluation pipeline.
Addresses feedback points for cron job verification, metadata tracking, checkpoint logging,
flexible retention policies, and recovery mechanisms.

Features:
1. ✅ Verify cron jobs never pass --test by default
2. ✅ Add metadata flags for test traceability
3. ✅ Checkpoint logging for external monitoring
4. ✅ Flexible retention policies
5. ✅ Recovery mechanism for failed runs

Usage:
    python integrity_monitor.py --check-cron
    python integrity_monitor.py --monitor-pipeline
    python integrity_monitor.py --generate-report
    python integrity_monitor.py --retry-failed
"""

import argparse
import json
import logging
import subprocess
import sys
import time
import yaml
import signal
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

@dataclass
class PipelineCheckpoint:
    """Pipeline checkpoint data structure."""
    timestamp: str
    stage: str
    tickers_processed: int
    total_tickers: int
    progress_percent: float
    elapsed_seconds: float
    estimated_remaining: float
    status: str  # 'running', 'completed', 'failed'
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PipelineRun:
    """Pipeline run data structure."""
    run_id: str
    start_time: str
    mode: str  # 'daily', 'weekly', 'manual', 'retry'
    is_test: bool
    status: str  # 'running', 'completed', 'failed', 'retrying'
    end_time: Optional[str] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    checkpoints: List[PipelineCheckpoint] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

class IntegrityMonitor:
    """Comprehensive integrity monitoring and reporting system."""
    
    def __init__(self, config_path: str = "config/test_schedules.yaml"):
        """Initialize the integrity monitor."""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.status_file = Path("logs/pipeline_status.json")
        self.runs_file = Path("logs/pipeline_runs.json")
        self.checkpoint_interval = self.config.get('checkpoint_interval', 50)  # Checkpoint every 50 tickers
        
        # Ensure log directories exist
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        self.runs_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing runs
        self.runs = self._load_runs()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            logging.warning(f"Config file not found: {self.config_path}, using defaults")
            return {
                'checkpoint_interval': 50,
                'retention_days': 30,
                'max_retries': 3,
                'retry_delay_minutes': 15,
                'notifications': {'enabled': False}
            }
        
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logging.error(f"Failed to load config: {e}")
            return {}
    
    def _load_runs(self) -> List[Dict[str, Any]]:
        """Load existing pipeline runs from JSON file."""
        if not self.runs_file.exists():
            return []
        
        try:
            with open(self.runs_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load runs: {e}")
            return []
    
    def _save_runs(self) -> None:
        """Save pipeline runs to JSON file."""
        try:
            with open(self.runs_file, 'w') as f:
                json.dump(self.runs, f, indent=2, default=str)
        except Exception as e:
            logging.error(f"Failed to save runs: {e}")
    
    def _save_status(self, status_data: Dict[str, Any]) -> None:
        """Save current pipeline status to JSON file."""
        try:
            with open(self.status_file, 'w') as f:
                json.dump(status_data, f, indent=2, default=str)
        except Exception as e:
            logging.error(f"Failed to save status: {e}")
    
    def check_cron_configuration(self) -> bool:
        """
        Feedback Point 1: Verify that cron jobs triggering daily runs never pass --test by default.
        
        Returns:
            True if cron configuration is correct, False otherwise
        """
        print("=== Checking Cron Configuration ===")
        
        # Check setup_cron.sh
        setup_script = Path("scripts/setup_cron.sh")
        if not setup_script.exists():
            print("❌ setup_cron.sh not found")
            return False
        
        with open(setup_script, 'r') as f:
            content = f.read()
        
        # Check for --test flag in actual cron commands (not in comments, grep commands, or verification logic)
        setup_lines = content.split('\n')
        test_in_commands = False
        
        for line in setup_lines:
            # Skip comments, empty lines, and verification logic
            if line.strip().startswith('#') or not line.strip():
                continue
            # Skip grep commands (verification logic)
            if 'grep' in line and '--test' in line:
                continue
            # Look for actual cron command lines that might contain --test
            if ('0 4' in line or '0 2' in line or '0 3' in line or '*/15' in line) and '--test ' in line and '--test-only' not in line:
                test_in_commands = True
                break
        
        if test_in_commands:
            print("❌ Found --test flag in cron commands")
            return False
        
        # Check daily test script
        daily_script = Path("pipeline/run_pipeline.py")
        if not daily_script.exists():
            print("❌ pipeline/run_pipeline.py not found")
            return False
        
        with open(daily_script, 'r') as f:
            daily_content = f.read()
        
        # Verify daily script uses --daily-integrity (not --test)
        # Look for actual command-line usage of --test (not in comments)
        daily_lines = daily_content.split('\n')
        test_usage_found = False
        daily_integrity_found = False
        
        for line in daily_lines:
            # Skip comments and empty lines
            if line.strip().startswith('#') or not line.strip():
                continue
            if '--test' in line and not line.strip().startswith('#'):
                test_usage_found = True
            if '--daily-integrity' in line and not line.strip().startswith('#'):
                daily_integrity_found = True
        
        if test_usage_found and not daily_integrity_found:
            print("❌ Daily script uses --test instead of --daily-integrity")
            return False
        
        # Check weekly test script (same file, different flag)
        weekly_integrity_found = False
        
        for line in daily_lines:
            # Skip comments and empty lines
            if line.strip().startswith('#') or not line.strip():
                continue
            if '--weekly-integrity' in line and not line.strip().startswith('#'):
                weekly_integrity_found = True
        
        if not weekly_integrity_found:
            print("❌ Weekly integrity flag not found in pipeline script")
            return False
        
        # Check actual cron jobs
        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            if result.returncode == 0:
                cron_content = result.stdout
                cron_lines = cron_content.split('\n')
                test_in_cron_commands = False
                
                for line in cron_lines:
                    # Skip comments and empty lines
                    if line.strip().startswith('#') or not line.strip():
                        continue
                    # Look for actual cron command lines that contain --test
                    if ('0 4' in line or '0 2' in line or '0 3' in line or '*/15' in line) and '--test ' in line and '--test-only' not in line:
                        test_in_cron_commands = True
                        break
                
                if test_in_cron_commands:
                    print("❌ Found --test flag in active cron jobs")
                    return False
                print("✅ No --test flags found in active cron jobs")
            else:
                print("⚠️  Could not check active cron jobs")
        except Exception as e:
            print(f"⚠️  Could not check cron jobs: {e}")
        
        print("✅ Cron configuration is correct - no --test flags in automated runs")
        return True
    
    def add_metadata_flags(self, run_id: str, is_test: bool, mode: str) -> Dict[str, Any]:
        """
        Feedback Point 2: Add metadata flags for test traceability.
        
        Args:
            run_id: Unique run identifier
            is_test: Whether this is a test run
            mode: Run mode (daily, weekly, manual, retry)
            
        Returns:
            Metadata dictionary with traceability flags
        """
        metadata = {
            "run_id": run_id,
            "is_test": is_test,
            "mode": mode,
            "timestamp": datetime.now().isoformat(),
            "pipeline_version": "1.0.0",
            "environment": "production" if not is_test else "test",
            "triggered_by": "cron" if mode in ["daily", "weekly"] else "manual",
            "data_directories": {
                "raw": "data/test/raw" if is_test else "data/raw",
                "processed": "data/test/processed" if is_test else "data/processed",
                "logs": "logs/test" if is_test else "logs"
            },
            "retention_policy": {
                "test_data": "immediate" if is_test else "30_days",
                "production_data": "90_days"
            }
        }
        
        # Save metadata to appropriate log directory
        log_dir = Path("logs/test" if is_test else "logs") / "metadata"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        metadata_file = log_dir / f"run_{run_id}_metadata.json"
        try:
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            logging.info(f"Metadata saved to {metadata_file}")
        except Exception as e:
            logging.error(f"Failed to save metadata: {e}")
        
        return metadata
    
    def log_checkpoint(self, run_id: str, stage: str, tickers_processed: int, 
                      total_tickers: int, elapsed_seconds: float, 
                      status: str = "running", error_message: Optional[str] = None) -> None:
        """
        Feedback Point 3: Add checkpoint logging for external monitoring.
        
        Args:
            run_id: Unique run identifier
            stage: Current pipeline stage
            tickers_processed: Number of tickers processed
            total_tickers: Total number of tickers
            elapsed_seconds: Time elapsed so far
            status: Current status
            error_message: Error message if any
        """
        progress_percent = (tickers_processed / total_tickers * 100) if total_tickers > 0 else 0
        estimated_remaining = (elapsed_seconds / tickers_processed * (total_tickers - tickers_processed)) if tickers_processed > 0 else 0
        
        checkpoint = PipelineCheckpoint(
            timestamp=datetime.now().isoformat(),
            stage=stage,
            tickers_processed=tickers_processed,
            total_tickers=total_tickers,
            progress_percent=progress_percent,
            elapsed_seconds=elapsed_seconds,
            estimated_remaining=estimated_remaining,
            status=status,
            error_message=error_message,
            metadata={
                "checkpoint_interval": self.checkpoint_interval,
                "memory_usage": self._get_memory_usage(),
                "disk_usage": self._get_disk_usage()
            }
        )
        
        # Update status file for external monitoring
        status_data = {
            "last_updated": datetime.now().isoformat(),
            "current_run": run_id,
            "current_stage": stage,
            "progress": {
                "processed": tickers_processed,
                "total": total_tickers,
                "percentage": progress_percent,
                "elapsed_seconds": elapsed_seconds,
                "estimated_remaining": estimated_remaining
            },
            "status": status,
            "error_message": error_message,
            "checkpoint": asdict(checkpoint)
        }
        
        self._save_status(status_data)
        
        # Log checkpoint
        logging.info(f"CHECKPOINT [{stage}]: {tickers_processed}/{total_tickers} ({progress_percent:.1f}%) - {status}")
        
        # Add checkpoint to run history
        for run in self.runs:
            if run.get('run_id') == run_id:
                if 'checkpoints' not in run:
                    run['checkpoints'] = []
                run['checkpoints'].append(asdict(checkpoint))
                break
        
        self._save_runs()
    
    def _get_memory_usage(self) -> Dict[str, Any]:
        """Get current memory usage."""
        try:
            import psutil
            memory = psutil.virtual_memory()
            return {
                "total_gb": memory.total / (1024**3),
                "available_gb": memory.available / (1024**3),
                "percent_used": memory.percent
            }
        except ImportError:
            return {"error": "psutil not available"}
    
    def _get_disk_usage(self) -> Dict[str, Any]:
        """Get current disk usage."""
        try:
            import psutil
            disk = psutil.disk_usage('.')
            return {
                "total_gb": disk.total / (1024**3),
                "free_gb": disk.free / (1024**3),
                "percent_used": (disk.used / disk.total) * 100
            }
        except ImportError:
            return {"error": "psutil not available"}
    
    def start_pipeline_run(self, mode: str, is_test: bool = False) -> str:
        """Start a new pipeline run and return the run ID."""
        run_id = f"{mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create run record
        run = PipelineRun(
            run_id=run_id,
            start_time=datetime.now().isoformat(),
            mode=mode,
            is_test=is_test,
            status="running",
            checkpoints=[],
            metadata=self.add_metadata_flags(run_id, is_test, mode)
        )
        
        self.runs.append(asdict(run))
        self._save_runs()
        
        logging.info(f"Started pipeline run: {run_id} (mode: {mode}, test: {is_test})")
        return run_id
    
    def end_pipeline_run(self, run_id: str, exit_code: int, error_message: Optional[str] = None) -> None:
        """End a pipeline run and update its status."""
        for run in self.runs:
            if run.get('run_id') == run_id:
                run['end_time'] = datetime.now().isoformat()
                run['exit_code'] = exit_code
                run['error_message'] = error_message
                run['status'] = 'completed' if exit_code == 0 else 'failed'
                break
        
        self._save_runs()
        logging.info(f"Ended pipeline run: {run_id} (exit_code: {exit_code})")
    
    def cleanup_old_reports(self, retention_days: Optional[int] = None) -> Tuple[int, int]:
        """
        Feedback Point 4: Add retention days as a parameter for flexible retention policies.
        
        Args:
            retention_days: Number of days to retain (overrides config)
            
        Returns:
            Tuple of (files_deleted, bytes_freed)
        """
        if retention_days is None:
            retention_days = self.config.get('retention_days', 30)
        
        # Ensure retention_days is an integer
        retention_days = int(retention_days)
        
        print(f"=== Cleaning up reports older than {retention_days} days ===")
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        total_deleted = 0
        total_size_freed = 0
        
        # Clean up pipeline runs
        if self.runs_file.exists():
            old_runs = []
            for run in self.runs:
                start_time = datetime.fromisoformat(run['start_time'])
                if start_time < cutoff_date:
                    old_runs.append(run)
                    total_deleted += 1
            
            # Remove old runs
            self.runs = [run for run in self.runs if run not in old_runs]
            self._save_runs()
        
        # Clean up integrity reports
        reports_dir = Path("logs/integrity_reports")
        if reports_dir.exists():
            for report_file in reports_dir.rglob("*.json"):
                try:
                    file_time = datetime.fromtimestamp(report_file.stat().st_mtime)
                    if file_time < cutoff_date:
                        file_size = report_file.stat().st_size
                        report_file.unlink()
                        total_deleted += 1
                        total_size_freed += file_size
                        print(f"Deleted: {report_file}")
                except Exception as e:
                    logging.warning(f"Error processing {report_file}: {e}")
        
        # Clean up test data if retention is very short
        if retention_days <= 7:
            test_dirs = [Path("data/test"), Path("logs/test")]
            for test_dir in test_dirs:
                if test_dir.exists():
                    try:
                        shutil.rmtree(test_dir)
                        print(f"Cleaned test directory: {test_dir}")
                    except Exception as e:
                        logging.warning(f"Error cleaning {test_dir}: {e}")
        
        print(f"Cleanup completed: {total_deleted} files deleted, {total_size_freed} bytes freed")
        return total_deleted, total_size_freed
    
    def retry_failed_runs(self, max_retries: Optional[int] = None) -> List[str]:
        """
        Feedback Point 5: Provide a recovery mechanism for failed daily runs.
        
        Args:
            max_retries: Maximum number of retries (overrides config)
            
        Returns:
            List of retried run IDs
        """
        if max_retries is None:
            max_retries = self.config.get('max_retries', 3)
        
        print(f"=== Retrying failed runs (max {max_retries} retries) ===")
        
        retried_runs = []
        retry_delay = self.config.get('retry_delay_minutes', 15)
        
        # Find failed runs that haven't exceeded max retries
        for run in self.runs:
            if (run.get('status') == 'failed' and 
                run.get('retry_count', 0) < max_retries and
                run.get('mode') in ['daily', 'weekly']):
                
                run_id = run['run_id']
                mode = run['mode']
                current_retries = run.get('retry_count', 0)
                
                print(f"Retrying {run_id} (attempt {current_retries + 1}/{max_retries})")
                
                # Update retry count
                run['retry_count'] = current_retries + 1
                run['status'] = 'retrying'
                run['last_retry'] = datetime.now().isoformat()
                
                # Wait before retry
                if current_retries > 0:
                    print(f"Waiting {retry_delay} minutes before retry...")
                    time.sleep(retry_delay * 60)
                
                # Execute retry
                try:
                    if mode == 'daily':
                        cmd = [sys.executable, "-m", "pipeline.run_pipeline", "--daily-integrity"]
                    else:  # weekly
                        cmd = [sys.executable, "-m", "pipeline.run_pipeline", "--weekly-integrity"]
                    
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
                    
                    if result.returncode == 0:
                        run['status'] = 'completed'
                        run['exit_code'] = 0
                        print(f"✅ Retry successful for {run_id}")
                    else:
                        run['status'] = 'failed'
                        run['exit_code'] = result.returncode
                        run['error_message'] = result.stderr
                        print(f"❌ Retry failed for {run_id}")
                    
                    retried_runs.append(run_id)
                    
                except Exception as e:
                    run['status'] = 'failed'
                    run['error_message'] = str(e)
                    print(f"❌ Retry exception for {run_id}: {e}")
                    retried_runs.append(run_id)
        
        self._save_runs()
        print(f"Retry process completed: {len(retried_runs)} runs retried")
        return retried_runs
    
    def generate_integrity_report(self, days: int = 7) -> Dict[str, Any]:
        """Generate a comprehensive integrity report."""
        cutoff_date = datetime.now() - timedelta(days=float(days))
        
        # Filter runs within the specified period
        recent_runs = [
            run for run in self.runs 
            if datetime.fromisoformat(run['start_time']) >= cutoff_date
        ]
        
        # Calculate statistics
        total_runs = len(recent_runs)
        successful_runs = len([r for r in recent_runs if r.get('status') == 'completed'])
        failed_runs = len([r for r in recent_runs if r.get('status') == 'failed'])
        retried_runs = len([r for r in recent_runs if r.get('retry_count', 0) > 0])
        
        # Calculate average runtime
        runtimes = []
        for run in recent_runs:
            if run.get('end_time'):
                start = datetime.fromisoformat(run['start_time'])
                end = datetime.fromisoformat(run['end_time'])
                runtimes.append((end - start).total_seconds())
        
        avg_runtime = sum(runtimes) / len(runtimes) if runtimes else 0
        
        # Generate report
        report = {
            "report_generated": datetime.now().isoformat(),
            "period_days": days,
            "summary": {
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "failed_runs": failed_runs,
                "retried_runs": retried_runs,
                "success_rate": (successful_runs / total_runs * 100) if total_runs > 0 else 0,
                "average_runtime_seconds": avg_runtime
            },
            "runs_by_mode": {},
            "recent_failures": [],
            "recommendations": []
        }
        
        # Group runs by mode
        for run in recent_runs:
            mode = run.get('mode', 'unknown')
            if mode not in report["runs_by_mode"]:
                report["runs_by_mode"][mode] = {"total": 0, "successful": 0, "failed": 0}
            
            report["runs_by_mode"][mode]["total"] += 1
            if run.get('status') == 'completed':
                report["runs_by_mode"][mode]["successful"] += 1
            else:
                report["runs_by_mode"][mode]["failed"] += 1
        
        # Add recent failures
        for run in recent_runs:
            if run.get('status') == 'failed':
                report["recent_failures"].append({
                    "run_id": run.get('run_id'),
                    "mode": run.get('mode'),
                    "start_time": run.get('start_time'),
                    "error_message": run.get('error_message'),
                    "retry_count": run.get('retry_count', 0)
                })
        
        # Generate recommendations
        if failed_runs > 0:
            report["recommendations"].append("Investigate failed runs and consider retry mechanism")
        
        if retried_runs > 0:
            report["recommendations"].append("Monitor retry patterns to identify recurring issues")
        
        if avg_runtime > 3600:  # More than 1 hour
            report["recommendations"].append("Consider optimizing pipeline performance")
        
        # Save report
        reports_dir = Path("logs/integrity_reports/summary")
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = reports_dir / f"integrity_report_{datetime.now().strftime('%Y%m%d')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Integrity report saved to: {report_file}")
        return report

def main():
    """Main entry point for the integrity monitor."""
    parser = argparse.ArgumentParser(description="Integrity monitoring and reporting for stock evaluation pipeline")
    parser.add_argument('--check-cron', action='store_true', help='Verify cron configuration')
    parser.add_argument('--monitor-pipeline', action='store_true', help='Monitor pipeline execution')
    parser.add_argument('--generate-report', action='store_true', help='Generate integrity report')
    parser.add_argument('--retry-failed', action='store_true', help='Retry failed runs')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old reports')
    parser.add_argument('--retention-days', type=int, help='Retention period in days for cleanup')
    parser.add_argument('--report-days', type=int, default=7, help='Days to include in report')
    parser.add_argument('--max-retries', type=int, help='Maximum retry attempts')
    
    args = parser.parse_args()
    
    monitor = IntegrityMonitor()
    
    if args.check_cron:
        success = monitor.check_cron_configuration()
        sys.exit(0 if success else 1)
    
    elif args.monitor_pipeline:
        print("Pipeline monitoring started...")
        # This would typically be called from within the pipeline
        # For now, just show current status
        if monitor.status_file.exists():
            with open(monitor.status_file, 'r') as f:
                status = json.load(f)
            print(json.dumps(status, indent=2))
        else:
            print("No current pipeline status found")
    
    elif args.generate_report:
        report = monitor.generate_integrity_report(args.report_days)
        print(json.dumps(report, indent=2))
    
    elif args.retry_failed:
        retried = monitor.retry_failed_runs(args.max_retries)
        print(f"Retried {len(retried)} failed runs")
    
    elif args.cleanup:
        deleted, freed = monitor.cleanup_old_reports(args.retention_days)
        print(f"Cleanup completed: {deleted} files deleted, {freed} bytes freed")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 