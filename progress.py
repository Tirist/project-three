#!/usr/bin/env python3
"""
progress.py

Reusable progress handler utility for the stock evaluation pipeline.
Provides consistent progress tracking with tqdm across all pipeline components.

Usage:
    from progress import ProgressTracker
    
    with ProgressTracker(total=503, desc="Fetching tickers") as tracker:
        for i, ticker in enumerate(tickers):
            # Process ticker
            tracker.update(1, postfix={"current": ticker})
"""

import time
from typing import Dict, Any, Optional, Union
from contextlib import contextmanager
import logging

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    logging.warning("tqdm not available. Progress bars will be disabled.")

class ProgressTracker:
    """Progress tracking utility with tqdm integration."""
    
    def __init__(
        self, 
        total: int, 
        desc: str = "Processing", 
        unit: str = "items",
        disable: bool = False,
        **kwargs
    ):
        """
        Initialize progress tracker.
        
        Args:
            total: Total number of items to process
            desc: Description for the progress bar
            unit: Unit label for items being processed
            disable: Whether to disable progress bar
            **kwargs: Additional tqdm parameters
        """
        self.total = total
        self.desc = desc
        self.unit = unit
        self.disable = disable or not TQDM_AVAILABLE
        self.kwargs = kwargs
        self.pbar = None
        self.start_time = None
        
    def __enter__(self):
        """Context manager entry."""
        if not self.disable:
            self.pbar = tqdm(
                total=self.total,
                desc=self.desc,
                unit=self.unit,
                **self.kwargs
            )
            self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.pbar:
            self.pbar.close()
            
    def update(self, n: int = 1, postfix: Optional[Dict[str, Any]] = None):
        """Update progress bar."""
        if self.pbar:
            if postfix:
                self.pbar.set_postfix(postfix)
            self.pbar.update(n)
            
    def set_description(self, desc: str):
        """Update progress bar description."""
        if self.pbar:
            self.pbar.set_description(desc)
            
    def set_postfix(self, postfix: Dict[str, Any]):
        """Set postfix information."""
        if self.pbar:
            self.pbar.set_postfix(postfix)

class SimpleProgressTracker:
    """Simple progress tracker for when tqdm is not available."""
    
    def __init__(self, total: int, desc: str = "Processing", unit: str = "items"):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        self.last_update = 0
        
    def __enter__(self):
        print(f"{self.desc}: 0/{self.total} {self.unit}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time
        print(f"{self.desc}: {self.total}/{self.total} {self.unit} completed in {elapsed:.2f}s")
        
    def update(self, n: int = 1, postfix: Optional[Dict[str, Any]] = None):
        """Update progress."""
        self.current += n
        current_time = time.time()
        
        # Update every 10 items or every 5 seconds
        if (self.current % 10 == 0 or 
            current_time - self.last_update > 5 or 
            self.current == self.total):
            
            elapsed = current_time - self.start_time
            if self.current > 0:
                eta = (elapsed / self.current) * (self.total - self.current)
                eta_str = f"ETA: {eta:.1f}s"
            else:
                eta_str = "ETA: --"
                
            postfix_str = ""
            if postfix:
                postfix_str = f" | {', '.join(f'{k}={v}' for k, v in postfix.items())}"
                
            print(f"{self.desc}: {self.current}/{self.total} {self.unit} | {eta_str}{postfix_str}")
            self.last_update = current_time
            
    def set_description(self, desc: str):
        """Update description."""
        self.desc = desc
        
    def set_postfix(self, postfix: Dict[str, Any]):
        """Set postfix (handled in update method)."""
        pass

def get_progress_tracker(
    total: int, 
    desc: str = "Processing", 
    unit: str = "items",
    disable: bool = False,
    **kwargs
) -> Union[ProgressTracker, SimpleProgressTracker]:
    """
    Get appropriate progress tracker based on tqdm availability.
    
    Args:
        total: Total number of items to process
        desc: Description for the progress bar
        unit: Unit label for items being processed
        disable: Whether to disable progress bar
        **kwargs: Additional parameters
        
    Returns:
        ProgressTracker or SimpleProgressTracker
    """
    if TQDM_AVAILABLE and not disable:
        return ProgressTracker(total, desc, unit, disable, **kwargs)
    else:
        return SimpleProgressTracker(total, desc, unit)

@contextmanager
def progress_context(
    total: int, 
    desc: str = "Processing", 
    unit: str = "items",
    disable: bool = False,
    **kwargs
):
    """
    Context manager for progress tracking.
    
    Args:
        total: Total number of items to process
        desc: Description for the progress bar
        unit: Unit label for items being processed
        disable: Whether to disable progress bar
        **kwargs: Additional parameters
        
    Yields:
        Progress tracker instance
    """
    tracker = get_progress_tracker(total, desc, unit, disable, **kwargs)
    with tracker:
        yield tracker

def format_time(seconds: float) -> str:
    """Format time in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def format_progress(current: int, total: int, elapsed: float) -> str:
    """Format progress information."""
    if total == 0:
        return "0/0 (0%)"
    
    percentage = (current / total) * 100
    if current > 0:
        eta = (elapsed / current) * (total - current)
        eta_str = f" ETA: {format_time(eta)}"
    else:
        eta_str = ""
        
    return f"{current}/{total} ({percentage:.1f}%){eta_str}" 