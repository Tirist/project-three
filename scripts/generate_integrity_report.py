#!/usr/bin/env python3
"""
generate_integrity_report.py

Generates integrity reports from existing pipeline data without running the full pipeline.
Useful for generating reports from historical data or for manual analysis.

Usage:
    python scripts/generate_integrity_report.py --type daily
    python scripts/generate_integrity_report.py --type weekly --date 2025-07-25
    python scripts/generate_integrity_report.py --type daily --format markdown
"""

import argparse
import json
import sys
import yaml
import subprocess
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import pandas as pd

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    config_path = Path("config/test_schedules.yaml")
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Failed to load configuration: {e}")
        return {}

def parse_pytest_results() -> Dict[str, Any]:
    """Parse pytest results from recent test runs."""
    test_results = {
        "total_tests": 0,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "failed_tests": [],
        "last_run": None
    }
    
    # Look for recent pytest output in logs
    log_files = [
        Path("logs/cron_daily.log"),
        Path("logs/cron_weekly.log"),
        Path("logs/features") / datetime.now().strftime("dt=%Y-%m-%d") / "test_results.log"
    ]
    
    for log_file in log_files:
        if log_file.exists():
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                    
                # Extract pytest summary
                pytest_pattern = r"=+ (.*?) in .*? =+\n(.*?)(?=\n=+|$)"
                matches = re.findall(pytest_pattern, content, re.DOTALL)
                
                for match in matches:
                    if "test session" in match[0]:
                        summary = match[1]
                        
                        # Parse test counts
                        passed_match = re.search(r"(\d+) passed", summary)
                        failed_match = re.search(r"(\d+) failed", summary)
                        skipped_match = re.search(r"(\d+) skipped", summary)
                        
                        if passed_match:
                            test_results["passed"] = int(passed_match.group(1))
                        if failed_match:
                            test_results["failed"] = int(failed_match.group(1))
                        if skipped_match:
                            test_results["skipped"] = int(skipped_match.group(1))
                        
                        test_results["total_tests"] = (
                            test_results["passed"] + 
                            test_results["failed"] + 
                            test_results["skipped"]
                        )
                        
                        # Extract failed test names
                        failed_tests = re.findall(r"tests/.*?\.py::([^\s]+)", summary)
                        test_results["failed_tests"] = failed_tests
                        
                        # Get last run time
                        test_results["last_run"] = datetime.fromtimestamp(
                            log_file.stat().st_mtime
                        ).isoformat()
                        
                        break
                        
            except Exception as e:
                print(f"Warning: Could not parse {log_file}: {e}")
    
    return test_results

def analyze_pipeline_data(date_str=None) -> Dict[str, Any]:
    """Analyze existing pipeline data for integrity report."""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    dt_str = f"dt={date_str}"
    
    analysis = {
        "date": date_str,
        "data_availability": {},
        "data_quality": {},
        "missing_data_percentage": 0,
        "pipeline_metrics": {}
    }
    
    # Check data availability
    total_expected_files = 0
    total_missing_files = 0
    
    for data_type in ["raw", "processed", "tickers"]:
        data_path = Path(f"data/{data_type}/{dt_str}")
        if data_path.exists():
            if data_type == "processed":
                parquet_file = data_path / "features.parquet"
                if parquet_file.exists():
                    try:
                        df = pd.read_parquet(parquet_file)
                        row_count = len(df)
                        ticker_count = df['ticker'].nunique() if 'ticker' in df.columns else 0
                        
                        analysis["data_availability"][data_type] = {
                            "exists": True,
                            "file_count": len(list(data_path.glob("*.parquet"))),
                            "row_count": row_count,
                            "ticker_count": ticker_count,
                            "file_size_mb": parquet_file.stat().st_size / (1024 * 1024)
                        }
                        
                        # Data quality checks
                        analysis["data_quality"][data_type] = {
                            "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
                            "duplicate_rows": len(df[df.duplicated()]),
                            "columns": list(df.columns)
                        }
                        
                        total_expected_files += 1
                        
                    except Exception as e:
                        analysis["data_availability"][data_type] = {"exists": True, "error": str(e)}
                        total_missing_files += 1
                else:
                    analysis["data_availability"][data_type] = {"exists": True, "no_features": True}
                    total_missing_files += 1
            else:
                csv_files = list(data_path.glob("*.csv"))
                analysis["data_availability"][data_type] = {
                    "exists": True,
                    "file_count": len(csv_files),
                    "total_size_mb": sum(f.stat().st_size for f in csv_files) / (1024 * 1024)
                }
                total_expected_files += 1
        else:
            analysis["data_availability"][data_type] = {"exists": False}
            total_missing_files += 1
    
    # Calculate missing data percentage
    if total_expected_files > 0:
        analysis["missing_data_percentage"] = (total_missing_files / total_expected_files) * 100
    
    # Check metadata
    metadata_path = Path(f"logs/features/{dt_str}/metadata.json")
    if metadata_path.exists():
        try:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            analysis["metadata"] = metadata
            
            # Extract pipeline metrics
            analysis["pipeline_metrics"] = {
                "runtime_seconds": metadata.get("runtime_seconds", 0),
                "runtime_minutes": metadata.get("runtime_minutes", 0),
                "tickers_processed": metadata.get("tickers_processed", 0),
                "tickers_successful": metadata.get("tickers_successful", 0),
                "tickers_failed": metadata.get("tickers_failed", 0),
                "features_generated": metadata.get("features_generated", False),
                "status": metadata.get("status", "unknown")
            }
        except Exception as e:
            analysis["metadata"] = {"error": str(e)}
    else:
        analysis["metadata"] = {"exists": False}
    
    return analysis

def generate_report(analysis: Dict[str, Any], test_results: Dict[str, Any], 
                   report_type: str = "daily", config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Generate integrity report from analysis."""
    if config is None:
        config = {}
    
    thresholds = config.get('integrity_reports', {}).get('thresholds', {})
    
    report = {
        "report_type": report_type,
        "generated_at": datetime.now().isoformat(),
        "analysis_date": analysis["date"],
        "data_availability": analysis["data_availability"],
        "data_quality": analysis["data_quality"],
        "missing_data_percentage": analysis["missing_data_percentage"],
        "pipeline_metrics": analysis["pipeline_metrics"],
        "test_results": test_results,
        "metadata": analysis["metadata"],
        "recommendations": [],
        "status": "healthy"
    }
    
    # Add recommendations based on analysis
    if not analysis["data_availability"].get("processed", {}).get("exists", False):
        report["recommendations"].append("No processed data found - pipeline may have failed")
        report["status"] = "critical"
    
    if analysis["missing_data_percentage"] > 50:
        report["recommendations"].append(f"High missing data percentage: {analysis['missing_data_percentage']:.1f}%")
        report["status"] = "warning"
    
    if analysis["pipeline_metrics"].get("runtime_minutes", 0) > thresholds.get("max_pipeline_time_minutes", 5):
        report["recommendations"].append("Pipeline runtime exceeded threshold")
        report["status"] = "warning"
    
    if test_results.get("failed", 0) > 0:
        report["recommendations"].append(f"{test_results['failed']} tests failed - investigate immediately")
        report["status"] = "critical"
    
    if analysis["data_quality"].get("processed", {}).get("null_percentage", 0) > 10:
        report["recommendations"].append("High null percentage in processed data")
        report["status"] = "warning"
    
    return report

def save_report(report: Dict[str, Any], report_type: str = "daily", 
                custom_path: str = None, output_format: str = "json") -> Path:
    """Save integrity report in specified format."""
    if custom_path:
        report_file = Path(custom_path)
    else:
        today = datetime.now().strftime("%Y-%m-%d")
        report_dir = Path("logs/integrity_reports") / report_type
        report_dir.mkdir(parents=True, exist_ok=True)
        
        if output_format == "markdown":
            report_file = report_dir / f"{today}.md"
        else:
            report_file = report_dir / f"{today}.json"
    
    if output_format == "markdown":
        markdown_content = generate_markdown_report(report)
        with open(report_file, 'w') as f:
            f.write(markdown_content)
    else:
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
    
    print(f"Integrity report saved to: {report_file}")
    return report_file

def generate_markdown_report(report: Dict[str, Any]) -> str:
    """Generate Markdown format report."""
    md = f"""# {report['report_type'].title()} Integrity Report

**Generated:** {report['generated_at']}  
**Analysis Date:** {report['analysis_date']}  
**Status:** {report['status'].upper()}

## Executive Summary

- **Data Availability:** {report['missing_data_percentage']:.1f}% missing data
- **Pipeline Status:** {report['pipeline_metrics'].get('status', 'unknown')}
- **Test Results:** {report['test_results'].get('passed', 0)} passed, {report['test_results'].get('failed', 0)} failed
- **Runtime:** {report['pipeline_metrics'].get('runtime_minutes', 0):.2f} minutes

## Data Availability

"""
    
    for data_type, info in report['data_availability'].items():
        if info.get('exists', False):
            if data_type == 'processed':
                md += f"- **{data_type.title()}:** ✅ {info.get('row_count', 0)} rows, {info.get('ticker_count', 0)} tickers\n"
            else:
                md += f"- **{data_type.title()}:** ✅ {info.get('file_count', 0)} files\n"
        else:
            md += f"- **{data_type.title()}:** ❌ Missing\n"
    
    md += f"""
## Test Results

- **Total Tests:** {report['test_results'].get('total_tests', 0)}
- **Passed:** {report['test_results'].get('passed', 0)}
- **Failed:** {report['test_results'].get('failed', 0)}
- **Skipped:** {report['test_results'].get('skipped', 0)}

"""
    
    if report['test_results'].get('failed_tests'):
        md += "**Failed Tests:**\n"
        for test in report['test_results']['failed_tests']:
            md += f"- {test}\n"
        md += "\n"
    
    md += f"""
## Pipeline Metrics

- **Runtime:** {report['pipeline_metrics'].get('runtime_seconds', 0):.2f} seconds
- **Tickers Processed:** {report['pipeline_metrics'].get('tickers_processed', 0)}
- **Tickers Successful:** {report['pipeline_metrics'].get('tickers_successful', 0)}
- **Tickers Failed:** {report['pipeline_metrics'].get('tickers_failed', 0)}

## Data Quality

"""
    
    for data_type, quality in report['data_quality'].items():
        md += f"### {data_type.title()}\n"
        md += f"- **Null Percentage:** {quality.get('null_percentage', 0):.2f}%\n"
        md += f"- **Duplicate Rows:** {quality.get('duplicate_rows', 0)}\n"
        md += f"- **Columns:** {len(quality.get('columns', []))}\n\n"
    
    if report['recommendations']:
        md += "## Recommendations\n\n"
        for rec in report['recommendations']:
            md += f"- {rec}\n"
        md += "\n"
    
    return md

def main():
    parser = argparse.ArgumentParser(description="Generate integrity report from existing data")
    parser.add_argument('--type', choices=['daily', 'weekly'], default='daily', help='Type of report to generate')
    parser.add_argument('--date', type=str, help='Date to analyze (YYYY-MM-DD format)')
    parser.add_argument('--output', type=str, help='Custom output path for report')
    parser.add_argument('--format', choices=['json', 'markdown'], default='json', help='Output format')
    
    args = parser.parse_args()
    
    print(f"=== Generating {args.type.upper()} Integrity Report ===")
    
    # Load configuration
    config = load_config()
    
    # Analyze pipeline data
    analysis = analyze_pipeline_data(args.date)
    
    # Parse test results
    test_results = parse_pytest_results()
    
    # Generate report
    report = generate_report(analysis, test_results, args.type, config)
    
    # Save report
    report_file = save_report(report, args.type, args.output, args.format)
    
    print("✅ Integrity report generated successfully")
    print(f"Status: {report['status'].upper()}")
    print(f"Missing data: {report['missing_data_percentage']:.1f}%")
    print(f"Tests passed: {report['test_results'].get('passed', 0)}/{report['test_results'].get('total_tests', 0)}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 