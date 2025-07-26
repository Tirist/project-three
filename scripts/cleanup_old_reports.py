#!/usr/bin/env python3
"""
cleanup_old_reports.py

Cleans up old integrity reports to maintain storage efficiency.
Removes reports older than specified retention periods.

Usage:
    python scripts/cleanup_old_reports.py
    python scripts/cleanup_old_reports.py --dry-run
"""

import argparse
import sys
import yaml
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

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

def cleanup_reports(dry_run=False) -> tuple[int, int]:
    """Clean up old integrity reports."""
    print("=== Cleaning Up Old Integrity Reports ===")
    
    # Load configuration
    config = load_config()
    retention_periods = config.get('integrity_reports', {}).get('retention_periods', {
        'daily': 30,
        'weekly': 90,
        'summary': 365
    })
    
    reports_dir = Path("logs/integrity_reports")
    if not reports_dir.exists():
        print("No integrity reports directory found")
        return 0, 0
    
    total_deleted = 0
    total_size_freed = 0
    
    for report_type, retention_days in retention_periods.items():
        type_dir = reports_dir / report_type
        if not type_dir.exists():
            continue
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
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

def generate_cleanup_report(deleted_count: int, size_freed: int, config: Dict[str, Any]) -> None:
    """Generate a cleanup report."""
    retention_periods = config.get('integrity_reports', {}).get('retention_periods', {})
    
    report = {
        "cleanup_date": datetime.now().isoformat(),
        "files_deleted": deleted_count,
        "bytes_freed": size_freed,
        "retention_policy": retention_periods
    }
    
    # Save cleanup report
    cleanup_dir = Path("logs/integrity_reports/summary")
    cleanup_dir.mkdir(parents=True, exist_ok=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    report_file = cleanup_dir / f"cleanup_{today}.json"
    
    try:
        with open(report_file, 'w') as f:
            yaml.dump(report, f, default_flow_style=False, indent=2)
        print(f"Cleanup report saved to: {report_file}")
    except Exception as e:
        print(f"Warning: Could not save cleanup report: {e}")

def main():
    parser = argparse.ArgumentParser(description="Clean up old integrity reports")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("Running in DRY RUN mode - no files will be deleted")
    
    try:
        config = load_config()
        deleted_count, size_freed = cleanup_reports(args.dry_run)
        generate_cleanup_report(deleted_count, size_freed, config)
        print("✅ Cleanup completed successfully")
        return 0
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 