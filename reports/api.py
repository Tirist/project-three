#!/usr/bin/env python3
"""
api.py

Simple API endpoints for frontend dashboard consumption.
Provides JSON endpoints for pipeline status, reports, and metadata.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse

# Import common utilities
sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline" / "utils"))
try:
    from common import PipelineConfig, DataManager, LogManager
except ImportError:
    print("Warning: Could not import common utilities")

class PipelineAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for pipeline API endpoints."""
    
    def __init__(self, *args, **kwargs):
        self.config = PipelineConfig()
        self.data_manager = DataManager()
        self.log_manager = LogManager()
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            parsed_path = urllib.parse.urlparse(self.path)
            path = parsed_path.path
            
            if path == "/api/status":
                self._handle_status()
            elif path == "/api/reports/latest":
                self._handle_latest_report()
            elif path == "/api/reports/daily":
                self._handle_daily_reports()
            elif path == "/api/reports/weekly":
                self._handle_weekly_reports()
            elif path == "/api/data/freshness":
                self._handle_data_freshness()
            elif path == "/api/pipeline/runs":
                self._handle_pipeline_runs()
            elif path == "/":
                self._handle_index()
            else:
                self._handle_404()
                
        except Exception as e:
            self._handle_error(str(e))
    
    def _send_json_response(self, data: Dict[str, Any], status_code: int = 200):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
        response = {
            "timestamp": datetime.now().isoformat(),
            "status": "success" if status_code == 200 else "error",
            "data": data
        }
        
        self.wfile.write(json.dumps(response, indent=2, default=str).encode())
    
    def _handle_status(self):
        """Handle /api/status endpoint."""
        status = {
            "pipeline_status": self._get_pipeline_status(),
            "data_status": self._get_data_status(),
            "system_status": self._get_system_status()
        }
        self._send_json_response(status)
    
    def _handle_latest_report(self):
        """Handle /api/reports/latest endpoint."""
        latest_report = self._get_latest_integrity_report()
        self._send_json_response(latest_report)
    
    def _handle_daily_reports(self):
        """Handle /api/reports/daily endpoint."""
        daily_reports = self._get_daily_reports()
        self._send_json_response(daily_reports)
    
    def _handle_weekly_reports(self):
        """Handle /api/reports/weekly endpoint."""
        weekly_reports = self._get_weekly_reports()
        self._send_json_response(weekly_reports)
    
    def _handle_data_freshness(self):
        """Handle /api/data/freshness endpoint."""
        freshness = self._get_data_freshness()
        self._send_json_response(freshness)
    
    def _handle_pipeline_runs(self):
        """Handle /api/pipeline/runs endpoint."""
        runs = self._get_pipeline_runs()
        self._send_json_response(runs)
    
    def _handle_index(self):
        """Handle root endpoint with API documentation."""
        api_docs = {
            "endpoints": {
                "/api/status": "Get overall pipeline status",
                "/api/reports/latest": "Get latest integrity report",
                "/api/reports/daily": "Get daily reports",
                "/api/reports/weekly": "Get weekly reports",
                "/api/data/freshness": "Get data freshness information",
                "/api/pipeline/runs": "Get recent pipeline runs"
            },
            "version": "1.0.0",
            "description": "Pipeline API for frontend dashboard"
        }
        self._send_json_response(api_docs)
    
    def _handle_404(self):
        """Handle 404 errors."""
        error_data = {
            "error": "Endpoint not found",
            "available_endpoints": [
                "/api/status",
                "/api/reports/latest",
                "/api/reports/daily",
                "/api/reports/weekly",
                "/api/data/freshness",
                "/api/pipeline/runs"
            ]
        }
        self._send_json_response(error_data, 404)
    
    def _handle_error(self, error_message: str):
        """Handle general errors."""
        error_data = {
            "error": error_message
        }
        self._send_json_response(error_data, 500)
    
    def _get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        try:
            # Check if pipeline is currently running
            pipeline_running = self._is_pipeline_running()
            
            # Get latest run information
            latest_run = self._get_latest_pipeline_run()
            
            return {
                "running": pipeline_running,
                "last_run": latest_run,
                "next_scheduled": self._get_next_scheduled_run()
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _get_data_status(self) -> Dict[str, Any]:
        """Get data status information."""
        try:
            return {
                "raw_data": self._get_data_partition_info("raw"),
                "processed_data": self._get_data_partition_info("processed"),
                "ticker_data": self._get_data_partition_info("tickers")
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _get_system_status(self) -> Dict[str, Any]:
        """Get system status information."""
        try:
            import shutil
            total, used, free = shutil.disk_usage(".")
            
            return {
                "disk_usage": {
                    "total_gb": total // (1024**3),
                    "used_gb": used // (1024**3),
                    "free_gb": free // (1024**3),
                    "usage_percent": (used / total) * 100
                },
                "uptime": self._get_system_uptime()
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _get_latest_integrity_report(self) -> Dict[str, Any]:
        """Get the latest integrity report."""
        try:
            reports_dir = Path("logs/integrity_reports")
            if not reports_dir.exists():
                return {"error": "No reports directory found"}
            
            # Find latest report
            latest_report = None
            for report_type in ["daily", "weekly"]:
                type_dir = reports_dir / report_type
                if type_dir.exists():
                    for report_file in type_dir.glob("*.json"):
                        if latest_report is None or report_file.stat().st_mtime > latest_report.stat().st_mtime:
                            latest_report = report_file
            
            if latest_report:
                with open(latest_report, 'r') as f:
                    return json.load(f)
            else:
                return {"error": "No reports found"}
                
        except Exception as e:
            return {"error": str(e)}
    
    def _get_daily_reports(self) -> Dict[str, Any]:
        """Get daily reports."""
        try:
            reports_dir = Path("logs/integrity_reports/daily")
            if not reports_dir.exists():
                return {"reports": []}
            
            reports = []
            for report_file in sorted(reports_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
                with open(report_file, 'r') as f:
                    report_data = json.load(f)
                    report_data["filename"] = report_file.name
                    reports.append(report_data)
            
            return {"reports": reports}
        except Exception as e:
            return {"error": str(e)}
    
    def _get_weekly_reports(self) -> Dict[str, Any]:
        """Get weekly reports."""
        try:
            reports_dir = Path("logs/integrity_reports/weekly")
            if not reports_dir.exists():
                return {"reports": []}
            
            reports = []
            for report_file in sorted(reports_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
                with open(report_file, 'r') as f:
                    report_data = json.load(f)
                    report_data["filename"] = report_file.name
                    reports.append(report_data)
            
            return {"reports": reports}
        except Exception as e:
            return {"error": str(e)}
    
    def _get_data_freshness(self) -> Dict[str, Any]:
        """Get data freshness information."""
        try:
            freshness = {}
            for data_type in ["raw", "processed", "tickers"]:
                partitions = self.data_manager.list_partitions(data_type)
                if partitions:
                    latest_partition = partitions[-1]
                    freshness[data_type] = {
                        "latest_partition": latest_partition,
                        "partition_count": len(partitions),
                        "days_old": (datetime.now() - datetime.strptime(latest_partition, "%Y-%m-%d")).days
                    }
                else:
                    freshness[data_type] = {
                        "latest_partition": None,
                        "partition_count": 0,
                        "days_old": None
                    }
            
            return freshness
        except Exception as e:
            return {"error": str(e)}
    
    def _get_pipeline_runs(self) -> Dict[str, Any]:
        """Get recent pipeline runs."""
        try:
            runs_file = Path("logs/pipeline_runs.json")
            if not runs_file.exists():
                return {"runs": []}
            
            with open(runs_file, 'r') as f:
                runs_data = json.load(f)
            
            # Return last 10 runs
            recent_runs = runs_data.get("runs", [])[-10:]
            return {"runs": recent_runs}
        except Exception as e:
            return {"error": str(e)}
    
    def _is_pipeline_running(self) -> bool:
        """Check if pipeline is currently running."""
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if proc.info['name'] == 'python' and any('run_pipeline' in cmd for cmd in proc.info['cmdline']):
                    return True
            return False
        except ImportError:
            return False
    
    def _get_latest_pipeline_run(self) -> Optional[Dict[str, Any]]:
        """Get information about the latest pipeline run."""
        try:
            runs_file = Path("logs/pipeline_runs.json")
            if not runs_file.exists():
                return None
            
            with open(runs_file, 'r') as f:
                runs_data = json.load(f)
            
            runs = runs_data.get("runs", [])
            if runs:
                return runs[-1]
            return None
        except Exception:
            return None
    
    def _get_next_scheduled_run(self) -> Optional[str]:
        """Get information about the next scheduled run."""
        # This would require parsing crontab or checking schedule
        # For now, return a simple estimate
        return "5:30 PM daily (production), 2:00 AM daily (cleanup)"
    
    def _get_data_partition_info(self, data_type: str) -> Dict[str, Any]:
        """Get information about data partitions."""
        try:
            partitions = self.data_manager.list_partitions(data_type)
            if partitions:
                latest = partitions[-1]
                return {
                    "latest_partition": latest,
                    "partition_count": len(partitions),
                    "partitions": partitions[-5:]  # Last 5 partitions
                }
            else:
                return {
                    "latest_partition": None,
                    "partition_count": 0,
                    "partitions": []
                }
        except Exception as e:
            return {"error": str(e)}
    
    def _get_system_uptime(self) -> Optional[str]:
        """Get system uptime."""
        try:
            import psutil
            uptime_seconds = psutil.boot_time()
            uptime = datetime.now() - datetime.fromtimestamp(uptime_seconds)
            return str(uptime).split('.')[0]  # Remove microseconds
        except ImportError:
            return None

def run_api_server(port: int = 8080):
    """Run the API server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, PipelineAPIHandler)
    print(f"Pipeline API server running on port {port}")
    print(f"API documentation available at: http://localhost:{port}/")
    httpd.serve_forever()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline API Server")
    parser.add_argument("--port", type=int, default=8080, help="Port to run server on")
    args = parser.parse_args()
    
    run_api_server(args.port) 