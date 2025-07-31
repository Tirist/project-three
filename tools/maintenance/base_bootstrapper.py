#!/usr/bin/env python3
"""
Base Bootstrapper Class

Provides shared functionality for historical data bootstrapping scripts.
Handles statistics tracking, batch processing, rate limiting, and summary generation.
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm


class BaseBootstrapper(ABC):
    """Base class for historical data bootstrapping with shared functionality."""
    
    def __init__(self, output_dir: Path, batch_size: int = 10, rate_limit_delay: float = 1.0):
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistics tracking
        self.stats = {
            "total_tickers": 0,
            "successful_tickers": 0,
            "failed_tickers": 0,
            "total_rows": 0,
            "start_time": None,
            "end_time": None,
            "failed_tickers_list": [],
            "errors": {}
        }
    
    @abstractmethod
    def fetch_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch historical data for a single ticker. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def save_ticker_data(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save ticker data to disk. Must be implemented by subclasses."""
        pass
    
    def validate_ticker(self, ticker) -> bool:
        """Validate ticker format."""
        if not isinstance(ticker, str):
            self.logger.error(f"Invalid ticker type: {type(ticker)}, ticker: {ticker}")
            return False
        return True
    
    def process_single_ticker(self, ticker: str) -> bool:
        """Process a single ticker with error handling."""
        try:
            if not self.validate_ticker(ticker):
                self.stats["failed_tickers"] += 1
                self.stats["failed_tickers_list"].append(str(ticker))
                return False
            
            # Fetch data
            df = self.fetch_historical_data(ticker)
            
            if df is not None and len(df) > 0:
                # Save data
                if self.save_ticker_data(ticker, df):
                    self.stats["successful_tickers"] += 1
                    self.stats["total_rows"] += len(df)
                    self.logger.info(f"✅ Successfully processed {ticker} ({len(df)} rows)")
                    return True
                else:
                    self.stats["failed_tickers"] += 1
                    self.stats["failed_tickers_list"].append(ticker)
                    self.logger.error(f"❌ Failed to save data for {ticker}")
                    return False
            else:
                self.stats["failed_tickers"] += 1
                self.stats["failed_tickers_list"].append(ticker)
                self.logger.error(f"❌ Failed to fetch data for {ticker}")
                return False
                
        except Exception as e:
            error_msg = f"Unexpected error processing {ticker}: {str(e)}"
            self.logger.error(error_msg)
            self.stats["errors"][str(ticker)] = str(e)
            self.stats["failed_tickers"] += 1
            self.stats["failed_tickers_list"].append(str(ticker))
            return False
    
    def process_batch(self, tickers: List[str]) -> None:
        """Process a batch of tickers with rate limiting."""
        for ticker in tickers:
            self.process_single_ticker(ticker)
            
            # Rate limiting
            if self.rate_limit_delay > 0:
                time.sleep(self.rate_limit_delay)
    
    def validate_tickers_list(self, tickers: List) -> List[str]:
        """Validate and filter tickers list."""
        if not isinstance(tickers, list):
            self.logger.error(f"Tickers must be a list, got: {type(tickers)}")
            return []
        
        valid_tickers = []
        for ticker in tickers:
            if self.validate_ticker(ticker):
                valid_tickers.append(ticker)
            else:
                self.logger.warning(f"Skipping invalid ticker: {ticker} (type: {type(ticker)})")
        
        return valid_tickers
    
    def run(self, tickers: List) -> Dict:
        """Run the bootstrap process for all tickers."""
        # Validate tickers
        valid_tickers = self.validate_tickers_list(tickers)
        
        if not valid_tickers:
            self.logger.error("No valid tickers to process")
            return self.stats
        
        self.stats["start_time"] = datetime.now()
        self.stats["total_tickers"] = len(valid_tickers)
        
        self.logger.info(f"Starting bootstrap for {len(valid_tickers)} tickers")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Rate limit delay: {self.rate_limit_delay} seconds")
        
        # Process tickers in batches
        batches = [valid_tickers[i:i + self.batch_size] for i in range(0, len(valid_tickers), self.batch_size)]
        
        with tqdm(total=len(valid_tickers), desc="Bootstrap Progress") as pbar:
            for batch in batches:
                self.process_batch(batch)
                pbar.update(len(batch))
        
        self.stats["end_time"] = datetime.now()
        runtime = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        # Generate summary
        summary = self.generate_summary(runtime)
        
        # Save summary
        self.save_summary(summary)
        
        # Log summary
        self.log_summary(summary)
        
        return summary
    
    def generate_summary(self, runtime: float) -> Dict:
        """Generate bootstrap summary."""
        return {
            "bootstrap_summary": {
                "total_tickers": self.stats["total_tickers"],
                "successful_tickers": self.stats["successful_tickers"],
                "failed_tickers": self.stats["failed_tickers"],
                "success_rate": f"{(self.stats['successful_tickers'] / self.stats['total_tickers'] * 100):.2f}%" if self.stats['total_tickers'] > 0 else "0.00%",
                "total_rows": self.stats["total_rows"],
                "runtime_seconds": runtime,
                "runtime_minutes": runtime / 60,
                "runtime_hours": runtime / 3600,
                "start_time": self.stats["start_time"].isoformat(),
                "end_time": self.stats["end_time"].isoformat(),
                "failed_tickers_list": self.stats["failed_tickers_list"],
                "errors": self.stats["errors"]
            }
        }
    
    def save_summary(self, summary: Dict) -> None:
        """Save summary to file."""
        summary_file = self.output_dir / "bootstrap_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        self.logger.info(f"Summary saved to: {summary_file}")
    
    def log_summary(self, summary: Dict) -> None:
        """Log bootstrap summary."""
        self.logger.info("=" * 60)
        self.logger.info("BOOTSTRAP SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total tickers: {self.stats['total_tickers']}")
        self.logger.info(f"Successful: {self.stats['successful_tickers']}")
        self.logger.info(f"Failed: {self.stats['failed_tickers']}")
        self.logger.info(f"Success rate: {summary['bootstrap_summary']['success_rate']}")
        self.logger.info(f"Total rows: {self.stats['total_rows']:,}")
        self.logger.info(f"Runtime: {summary['bootstrap_summary']['runtime_hours']:.2f} hours")
        
        if self.stats["failed_tickers_list"]:
            self.logger.warning(f"Failed tickers: {', '.join(self.stats['failed_tickers_list'])}") 