#!/usr/bin/env python3
"""
cleanup_old_reports.py

Cleans up old integrity reports to maintain storage efficiency.
Removes reports older than specified retention periods.

Usage:
    python scripts/cleanup_old_reports.py
    python scripts/cleanup_old_reports.py --dry-run
    python scripts/cleanup_old_reports.py --test
    python scripts/cleanup_old_reports.py --retention-days=7
    python scripts/cleanup_old_reports.py --test-only --retention-days=1
"""

import argparse
import sys
import yaml
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    config_path = Path("config/test_schedules.yaml")
    if not config_path.exists():
        logging.warning(f"Configuration file not found: {config_path}, using defaults")
        return {
            'integrity_reports': {
                'retention_periods': {
                    'daily': 30,
                    'weekly': 90,
                    'summary': 365
                }
            }
        }
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def cleanup_reports(dry_run=False, test_mode=False, retention_days=None) -> tuple[int, int]:
    """Clean up old integrity reports."""
    mode_str = "TEST" if test_mode else "PRODUCTION"
    print(f"=== Cleaning Up Old Integrity Reports ({mode_str}) ===")
    
    # Load configuration
    config = load_config()
    
    # Use provided retention_days or fall back to config
    if retention_days is None:
        retention_periods = config.get('integrity_reports', {}).get('retention_periods', {
            'daily': 30,
            'weekly': 90,
            'summary': 365
        })
        # Use the shortest retention period as default
        retention_days = min(retention_periods.values())
    
    print(f"Using retention period: {retention_days} days")
    
    # Determine base directory based on test mode
    if test_mode:
        reports_dir = Path("logs/test/integrity_reports")
    else:
        reports_dir = Path("logs/integrity_reports")
    
    if not reports_dir.exists():
        print(f"No integrity reports directory found: {reports_dir}")
        return 0, 0
    
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    total_deleted = 0
    total_size_freed = 0
    
    for report_type in ['daily', 'weekly', 'summary']:
        type_dir = reports_dir / report_type
        if not type_dir.exists():
            continue
        
        print(f"\nChecking {report_type} reports (retention: {retention_days} days)")
        
        for report_file in type_dir.glob("*.json"):
            try:
                # Extract date from filename (YYYY-MM-DD.json)
                date_str = report_file.stem
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    file_size = report_file.stat().st_size
                    if dry_run:
                        print(f"  [DRY RUN] Would delete: {report_file} ({file_size} bytes)")
                    else:
                        report_file.unlink()
                        print(f"  Deleted: {report_file} ({file_size} bytes)")
                        total_deleted += 1
                        total_size_freed += file_size
            except ValueError:
                print(f"  Warning: Could not parse date from filename: {report_file}")
            except Exception as e:
                print(f"  Error processing {report_file}: {e}")
    
    if dry_run:
        print(f"\n[DRY RUN] Would delete {total_deleted} files, freeing {total_size_freed} bytes")
    else:
        print(f"\nDeleted {total_deleted} files, freed {total_size_freed} bytes")
    
    return total_deleted, total_size_freed

def cleanup_test_data(dry_run=False, retention_days=None) -> tuple[int, int]:
    """Clean up test data directories."""
    print("=== Cleaning Up Test Data ===")
    
    # For test data, use shorter retention if specified
    if retention_days is None:
        retention_days = 7  # Default 7 days for test data
    
    print(f"Using retention period: {retention_days} days for test data")
    
    test_dirs = [
        Path("data/test"),
        Path("logs/test")
    ]
    
    total_deleted = 0
    total_size_freed = 0
    
    for test_dir in test_dirs:
        if not test_dir.exists():
            continue
        
        print(f"\nCleaning {test_dir}")
        
        # Remove all contents of test directories
        for item in test_dir.rglob("*"):
            if item.is_file():
                try:
                    file_size = item.stat().st_size
                    if dry_run:
                        print(f"  [DRY RUN] Would delete: {item} ({file_size} bytes)")
                    else:
                        item.unlink()
                        print(f"  Deleted: {item} ({file_size} bytes)")
                        total_deleted += 1
                        total_size_freed += file_size
                except Exception as e:
                    print(f"  Error deleting {item}: {e}")
            elif item.is_dir():
                try:
                    if dry_run:
                        print(f"  [DRY RUN] Would delete directory: {item}")
                    else:
                        import shutil
                        shutil.rmtree(item)
                        print(f"  Deleted directory: {item}")
                except Exception as e:
                    print(f"  Error deleting directory {item}: {e}")
    
    if dry_run:
        print(f"\n[DRY RUN] Would delete {total_deleted} files, freeing {total_size_freed} bytes")
    else:
        print(f"\nDeleted {total_deleted} files, freed {total_size_freed} bytes")
    
    return total_deleted, total_size_freed

def cleanup_pipeline_data(dry_run=False, retention_days=None) -> tuple[int, int]:
    """Clean up old pipeline data (raw, processed, logs)."""
    print("=== Cleaning Up Pipeline Data ===")
    
    if retention_days is None:
        retention_days = 30  # Default 30 days for pipeline data
    
    print(f"Using retention period: {retention_days} days for pipeline data")
    
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    total_deleted = 0
    total_size_freed = 0
    
    # Clean up data directories
    data_dirs = [
        Path("data/raw"),
        Path("data/processed"),
        Path("data/tickers"),
        Path("logs/fetch"),
        Path("logs/features"),
        Path("logs/tickers"),
        Path("logs/cleanup")
    ]
    
    for data_dir in data_dirs:
        if not data_dir.exists():
            continue
        
        print(f"\nCleaning {data_dir}")
        
        for partition_dir in data_dir.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                try:
                    partition_date_str = partition_dir.name[3:]  # Remove "dt=" prefix
                    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
                    
                    if partition_date < cutoff_date:
                        if dry_run:
                            print(f"  [DRY RUN] Would delete old partition: {partition_dir}")
                        else:
                            import shutil
                            shutil.rmtree(partition_dir)
                            print(f"  Deleted old partition: {partition_dir}")
                            total_deleted += 1
                except ValueError:
                    print(f"  Warning: Could not parse date from partition name: {partition_dir.name}")
                except Exception as e:
                    print(f"  Error processing {partition_dir}: {e}")
    
    if dry_run:
        print(f"\n[DRY RUN] Would delete {total_deleted} partitions, freeing {total_size_freed} bytes")
    else:
        print(f"\nDeleted {total_deleted} partitions, freed {total_size_freed} bytes")
    
    return total_deleted, total_size_freed

def generate_cleanup_report(deleted_count: int, size_freed: int, config: Dict[str, Any], 
                          test_mode: bool = False, retention_days: Optional[int] = None) -> None:
    """Generate a cleanup report."""
    retention_periods = config.get('integrity_reports', {}).get('retention_periods', {})
    
    report = {
        "cleanup_date": datetime.now().isoformat(),
        "files_deleted": deleted_count,
        "bytes_freed": size_freed,
        "retention_policy": retention_periods,
        "retention_days_used": retention_days,
        "test_mode": test_mode
    }
    
    # Save cleanup report
    if test_mode:
        cleanup_log_path = Path("logs/test/cleanup")
    else:
        cleanup_log_path = Path("logs/cleanup")
    
    cleanup_log_path.mkdir(parents=True, exist_ok=True)
    cleanup_file = cleanup_log_path / f"cleanup_{datetime.now().strftime('%Y-%m-%d')}.json"
    
    try:
        with open(cleanup_file, 'w') as f:
            yaml.dump(report, f, default_flow_style=False, indent=2)
        print(f"Cleanup report saved to: {cleanup_file}")
    except Exception as e:
        print(f"Warning: Could not save cleanup report: {e}")

def main():
    parser = argparse.ArgumentParser(description="Clean up old integrity reports and test data")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    parser.add_argument('--test', action='store_true', help='Clean up test data directories')
    parser.add_argument('--test-only', action='store_true', help='Only clean up test data, skip integrity reports')
    parser.add_argument('--pipeline-data', action='store_true', help='Clean up old pipeline data (raw, processed, logs)')
    parser.add_argument('--retention-days', type=int, help='Retention period in days (overrides config)')
    parser.add_argument('--all', action='store_true', help='Clean up everything (reports, test data, pipeline data)')
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("Running in DRY RUN mode - no files will be deleted")
    
    try:
        config = load_config()
        total_deleted = 0
        total_size_freed = 0
        
        # Clean up integrity reports (unless test-only mode)
        if not args.test_only:
            deleted_count, size_freed = cleanup_reports(args.dry_run, args.test, args.retention_days)
            total_deleted += deleted_count
            total_size_freed += size_freed
        
        # Clean up test data if requested
        if args.test or args.test_only or args.all:
            deleted_count, size_freed = cleanup_test_data(args.dry_run, args.retention_days)
            total_deleted += deleted_count
            total_size_freed += size_freed
        
        # Clean up pipeline data if requested
        if args.pipeline_data or args.all:
            deleted_count, size_freed = cleanup_pipeline_data(args.dry_run, args.retention_days)
            total_deleted += deleted_count
            total_size_freed += size_freed
        
        generate_cleanup_report(total_deleted, total_size_freed, config, args.test, args.retention_days)
        print("✅ Cleanup completed successfully")
        return 0
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 