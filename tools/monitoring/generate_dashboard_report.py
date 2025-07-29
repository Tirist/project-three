#!/usr/bin/env python3
"""
Dashboard Report Generator for Stock Pipeline
Analyzes logs and metadata to generate a comprehensive dashboard report.
"""

import json
import os
import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

class DashboardReportGenerator:
    def __init__(self, logs_dir: str = "logs"):
        self.logs_dir = Path(logs_dir)
        self.report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {},
            "recent_runs": [],
            "stage_metrics": {},
            "integrity_reports": {},
            "errors_summary": {},
            "recommendations": []
        }
    
    def load_pipeline_runs(self) -> List[Dict]:
        """Load and parse pipeline runs data."""
        runs_file = self.logs_dir / "pipeline_runs.json"
        if not runs_file.exists():
            return []
        
        with open(runs_file, 'r') as f:
            runs_data = json.load(f)
        
        # Extract the last 10 runs for dashboard
        recent_runs = []
        for run in runs_data[-10:]:  # Last 10 runs
            run_summary = {
                "run_id": run.get("run_id"),
                "start_time": run.get("start_time"),
                "end_time": run.get("end_time"),
                "mode": run.get("mode"),
                "status": run.get("status"),
                "exit_code": run.get("exit_code"),
                "error_message": run.get("error_message"),
                "retry_count": run.get("retry_count", 0),
                "is_test": run.get("is_test", False),
                "stages": []
            }
            
            # Extract stage information from checkpoints
            if "checkpoints" in run:
                for checkpoint in run["checkpoints"]:
                    stage_info = {
                        "stage": checkpoint.get("stage"),
                        "status": checkpoint.get("status"),
                        "progress_percent": checkpoint.get("progress_percent", 0),
                        "elapsed_seconds": checkpoint.get("elapsed_seconds", 0),
                        "error_message": checkpoint.get("error_message"),
                        "timestamp": checkpoint.get("timestamp")
                    }
                    run_summary["stages"].append(stage_info)
            
            recent_runs.append(run_summary)
        
        return recent_runs
    
    def load_stage_metadata(self, date: str) -> Dict[str, Any]:
        """Load metadata for a specific date from all stages."""
        metadata = {}
        
        # Tickers metadata
        tickers_meta = self.logs_dir / "tickers" / f"dt={date}" / "metadata.json"
        if tickers_meta.exists():
            with open(tickers_meta, 'r') as f:
                metadata["tickers"] = json.load(f)
        
        # Fetch metadata
        fetch_meta = self.logs_dir / "fetch" / f"dt={date}" / "metadata.json"
        if fetch_meta.exists():
            with open(fetch_meta, 'r') as f:
                metadata["fetch"] = json.load(f)
        
        # Features metadata
        features_meta = self.logs_dir / "features" / f"dt={date}" / "metadata.json"
        if features_meta.exists():
            with open(features_meta, 'r') as f:
                metadata["features"] = json.load(f)
        
        return metadata
    
    def load_integrity_reports(self) -> Dict[str, Any]:
        """Load integrity reports from daily and summary directories."""
        integrity_data = {}
        
        # Daily reports
        daily_reports = list(self.logs_dir.glob("integrity_reports/daily/*.json"))
        for report_file in daily_reports[-5:]:  # Last 5 daily reports
            date = report_file.stem
            with open(report_file, 'r') as f:
                integrity_data[f"daily_{date}"] = json.load(f)
        
        # Summary reports
        summary_reports = list(self.logs_dir.glob("integrity_reports/summary/*.json"))
        for report_file in summary_reports[-3:]:  # Last 3 summary reports
            name = report_file.stem
            with open(report_file, 'r') as f:
                integrity_data[f"summary_{name}"] = json.load(f)
        
        return integrity_data
    
    def calculate_summary_metrics(self, runs: List[Dict]) -> Dict[str, Any]:
        """Calculate summary metrics from recent runs."""
        if not runs:
            return {}
        
        total_runs = len(runs)
        successful_runs = len([r for r in runs if r["status"] == "completed"])
        failed_runs = len([r for r in runs if r["status"] == "failed"])
        running_runs = len([r for r in runs if r["status"] == "running"])
        
        # Calculate average runtime for completed runs
        runtimes = []
        for run in runs:
            if run["start_time"] and run["end_time"]:
                start = datetime.fromisoformat(run["start_time"])
                end = datetime.fromisoformat(run["end_time"])
                runtime = (end - start).total_seconds()
                runtimes.append(runtime)
        
        avg_runtime = sum(runtimes) / len(runtimes) if runtimes else 0
        
        # Error analysis
        errors = {}
        for run in runs:
            if run["error_message"]:
                error_type = run["error_message"].split(";")[0].split(".")[0]
                errors[error_type] = errors.get(error_type, 0) + 1
        
        return {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "running_runs": running_runs,
            "success_rate": (successful_runs / total_runs * 100) if total_runs > 0 else 0,
            "average_runtime_seconds": avg_runtime,
            "error_distribution": errors,
            "runs_by_mode": {}
        }
    
    def generate_recommendations(self, summary: Dict, integrity_reports: Dict) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        
        # Check success rate
        if summary.get("success_rate", 0) < 80:
            recommendations.append("Low success rate detected - investigate recent failures")
        
        # Check for common errors
        error_dist = summary.get("error_distribution", {})
        if "fetch_data" in error_dist and error_dist["fetch_data"] > 2:
            recommendations.append("Multiple fetch_data failures - check API connectivity")
        
        if "process_features" in error_dist and error_dist["process_features"] > 1:
            recommendations.append("Feature processing issues detected - review data quality")
        
        # Check integrity reports
        for report_key, report_data in integrity_reports.items():
            if "recommendations" in report_data:
                recommendations.extend(report_data["recommendations"])
        
        return list(set(recommendations))  # Remove duplicates
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate the complete dashboard report."""
        print("Loading pipeline runs...")
        runs = self.load_pipeline_runs()
        
        print("Loading integrity reports...")
        integrity_reports = self.load_integrity_reports()
        
        print("Calculating summary metrics...")
        summary = self.calculate_summary_metrics(runs)
        
        print("Generating recommendations...")
        recommendations = self.generate_recommendations(summary, integrity_reports)
        
        # Load recent stage metadata (last 3 days)
        recent_dates = []
        for i in range(3):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            recent_dates.append(date)
        
        stage_metrics = {}
        for date in recent_dates:
            stage_metrics[date] = self.load_stage_metadata(date)
        
        self.report.update({
            "summary": summary,
            "recent_runs": runs,
            "stage_metrics": stage_metrics,
            "integrity_reports": integrity_reports,
            "recommendations": recommendations
        })
        
        return self.report
    
    def save_report(self, output_file: str = "dashboard_report.json"):
        """Save the report to a JSON file."""
        report = self.generate_report()
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"Dashboard report saved to: {output_file}")
        return output_file

def main():
    """Main function to generate the dashboard report."""
    generator = DashboardReportGenerator()
    report_file = generator.save_report()
    
    # Print summary
    with open(report_file, 'r') as f:
        report = json.load(f)
    
    print("\n=== DASHBOARD REPORT SUMMARY ===")
    print(f"Generated at: {report['generated_at']}")
    print(f"Total runs analyzed: {report['summary']['total_runs']}")
    print(f"Success rate: {report['summary']['success_rate']:.1f}%")
    print(f"Average runtime: {report['summary']['average_runtime_seconds']:.1f} seconds")
    
    if report['recommendations']:
        print("\nRecommendations:")
        for rec in report['recommendations']:
            print(f"  - {rec}")
    
    print(f"\nDetailed report saved to: {report_file}")

if __name__ == "__main__":
    main() 