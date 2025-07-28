#!/usr/bin/env python3
"""
logger.py

Standardized logging utilities for the stock evaluation pipeline.
Provides consistent logging format and structure across all components.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

class PipelineLogger:
    """Standardized logger for pipeline components."""
    
    def __init__(self, name: str, log_dir: str = "logs", test_mode: bool = False):
        self.name = name
        self.log_dir = Path(log_dir)
        self.test_mode = test_mode
        
        # Set up log directory
        if test_mode:
            self.log_dir = self.log_dir / "test"
        
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize logger
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logger with file and console handlers."""
        logger = logging.getLogger(self.name)
        
        if not logger.handlers:
            # File handler
            log_file = self.log_dir / f"{self.name}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            
            # Formatter
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.setLevel(logging.INFO)
        
        return logger
    
    def info(self, message: str, **kwargs):
        """Log info message with optional structured data."""
        if kwargs:
            message = f"{message} | {json.dumps(kwargs, default=str)}"
        self.logger.info(message)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional structured data."""
        if kwargs:
            message = f"{message} | {json.dumps(kwargs, default=str)}"
        self.logger.warning(message)
    
    def error(self, message: str, **kwargs):
        """Log error message with optional structured data."""
        if kwargs:
            message = f"{message} | {json.dumps(kwargs, default=str)}"
        self.logger.error(message)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional structured data."""
        if kwargs:
            message = f"{message} | {json.dumps(kwargs, default=str)}"
        self.logger.debug(message)
    
    def log_stage_start(self, stage: str, **kwargs):
        """Log the start of a pipeline stage."""
        self.info(f"STAGE_START: {stage}", stage=stage, **kwargs)
    
    def log_stage_end(self, stage: str, duration: float, status: str = "completed", **kwargs):
        """Log the end of a pipeline stage."""
        self.info(
            f"STAGE_END: {stage} | {status} | {duration:.2f}s",
            stage=stage,
            duration=duration,
            status=status,
            **kwargs
        )
    
    def log_checkpoint(self, stage: str, current: int, total: int, **kwargs):
        """Log a checkpoint with progress information."""
        percentage = (current / total * 100) if total > 0 else 0
        self.info(
            f"CHECKPOINT: {stage} | {current}/{total} ({percentage:.1f}%)",
            stage=stage,
            current=current,
            total=total,
            percentage=percentage,
            **kwargs
        )

class StructuredLogger:
    """Logger that outputs structured JSON logs for frontend consumption."""
    
    def __init__(self, name: str, log_dir: str = "logs", test_mode: bool = False):
        self.name = name
        self.log_dir = Path(log_dir)
        self.test_mode = test_mode
        
        # Set up structured log directory
        if test_mode:
            self.log_dir = self.log_dir / "test" / "structured"
        else:
            self.log_dir = self.log_dir / "structured"
        
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def log_event(self, event_type: str, data: Dict[str, Any], timestamp: Optional[datetime] = None):
        """Log a structured event."""
        if timestamp is None:
            timestamp = datetime.now()
        
        event = {
            "timestamp": timestamp.isoformat(),
            "logger": self.name,
            "event_type": event_type,
            "test_mode": self.test_mode,
            **data
        }
        
        # Write to structured log file
        log_file = self.log_dir / f"{self.name}_{timestamp.strftime('%Y-%m-%d')}.jsonl"
        with open(log_file, 'a') as f:
            f.write(json.dumps(event) + '\n')
    
    def log_pipeline_start(self, run_id: str, mode: str, **kwargs):
        """Log pipeline start event."""
        self.log_event("pipeline_start", {
            "run_id": run_id,
            "mode": mode,
            **kwargs
        })
    
    def log_pipeline_end(self, run_id: str, duration: float, status: str, **kwargs):
        """Log pipeline end event."""
        self.log_event("pipeline_end", {
            "run_id": run_id,
            "duration": duration,
            "status": status,
            **kwargs
        })
    
    def log_stage_start(self, run_id: str, stage: str, **kwargs):
        """Log stage start event."""
        self.log_event("stage_start", {
            "run_id": run_id,
            "stage": stage,
            **kwargs
        })
    
    def log_stage_end(self, run_id: str, stage: str, duration: float, status: str, **kwargs):
        """Log stage end event."""
        self.log_event("stage_end", {
            "run_id": run_id,
            "stage": stage,
            "duration": duration,
            "status": status,
            **kwargs
        })
    
    def log_error(self, run_id: str, stage: str, error: str, **kwargs):
        """Log error event."""
        self.log_event("error", {
            "run_id": run_id,
            "stage": stage,
            "error": error,
            **kwargs
        })

def get_logger(name: str, log_dir: str = "logs", test_mode: bool = False) -> PipelineLogger:
    """Get a pipeline logger instance."""
    return PipelineLogger(name, log_dir, test_mode)

def get_structured_logger(name: str, log_dir: str = "logs", test_mode: bool = False) -> StructuredLogger:
    """Get a structured logger instance."""
    return StructuredLogger(name, log_dir, test_mode) 