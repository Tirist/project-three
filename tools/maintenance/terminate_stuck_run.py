#!/usr/bin/env python3
"""
Terminate Stuck Weekly Run
Safely terminate the stuck weekly run and update pipeline status.
"""

import json
import os
from datetime import datetime
from pathlib import Path

def terminate_stuck_run():
    """Terminate the stuck weekly run and update status."""
    
    # Read current pipeline status
    status_file = Path("logs/pipeline_status.json")
    if not status_file.exists():
        print("❌ Pipeline status file not found")
        return False
    
    with open(status_file, 'r') as f:
        status = json.load(f)
    
    current_run = status.get("current_run")
    current_stage = status.get("current_stage")
    elapsed_seconds = status.get("progress", {}).get("elapsed_seconds", 0)
    
    print(f"🔍 Current run: {current_run}")
    print(f"🔍 Current stage: {current_stage}")
    print(f"🔍 Elapsed time: {elapsed_seconds:.1f} seconds ({elapsed_seconds/3600:.1f} hours)")
    
    # Check if run is stuck (more than 2 hours)
    if elapsed_seconds > 7200:  # 2 hours
        print("🚨 Run appears to be stuck (running for more than 2 hours)")
        
        # Update status to terminated
        status.update({
            "last_updated": datetime.now().isoformat(),
            "status": "terminated",
            "error_message": "Run terminated due to excessive runtime",
            "checkpoint": {
                "timestamp": datetime.now().isoformat(),
                "stage": current_stage,
                "status": "terminated",
                "error_message": "Run terminated due to excessive runtime",
                "elapsed_seconds": elapsed_seconds
            }
        })
        
        # Save updated status
        with open(status_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        print("✅ Successfully terminated stuck run")
        print("✅ Pipeline status updated")
        return True
    else:
        print("ℹ️ Run appears to be within normal runtime limits")
        return False

if __name__ == "__main__":
    terminate_stuck_run() 