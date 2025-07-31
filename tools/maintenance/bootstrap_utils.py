#!/usr/bin/env python3
"""
Bootstrap Utilities

Common utility functions for bootstrap scripts including ticker fetching,
configuration loading, and argument parsing.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import yaml


def setup_logging(log_level: str = "INFO", verbose: bool = False) -> logging.Logger:
    """Setup logging configuration."""
    if verbose:
        log_level = "DEBUG"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def load_config(config_path: str = "config/settings.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config or {}
    except Exception as e:
        print(f"❌ Error loading config file: {e}")
        return {}


def get_api_key_from_config(config: dict, api_key_arg: Optional[str] = None) -> Optional[str]:
    """Get API key from config or command line argument."""
    if api_key_arg:
        return api_key_arg
    
    api_key = config.get('alpha_vantage_api_key')
    if api_key:
        print(f"✅ Using API key from config file: {api_key[:8]}...")
        return api_key
    
    print("❌ No API key found in config file and --api-key not provided")
    print("Please either:")
    print("1. Add 'alpha_vantage_api_key: your_key' to config/settings.yaml")
    print("2. Use --api-key command line argument")
    return None


def get_tickers_from_args(args: argparse.Namespace) -> Tuple[List[str], bool]:
    """Get tickers from command line arguments."""
    if args.tickers:
        return args.tickers, True
    
    if hasattr(args, 'sp500') and args.sp500:
        return get_sp500_tickers()
    
    # Default test tickers
    default_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    return default_tickers, False


def get_sp500_tickers() -> Tuple[List[str], bool]:
    """Fetch S&P 500 tickers using TickerFetcher."""
    try:
        # Add pipeline directory to path for imports
        project_root = Path(__file__).parent.parent.parent
        sys.path.insert(0, str(project_root / "pipeline"))
        
        from fetch_tickers import TickerFetcher
        
        fetcher = TickerFetcher()
        tickers_result = fetcher.fetch_sp500_tickers()
        
        # Handle tuple return (tickers, company_names)
        if isinstance(tickers_result, tuple) and len(tickers_result) == 2:
            tickers, company_names = tickers_result
            print(f"✅ Fetched {len(tickers)} S&P 500 tickers")
        elif isinstance(tickers_result, list):
            tickers = tickers_result
            print(f"✅ Fetched {len(tickers)} S&P 500 tickers")
        else:
            print(f"❌ Unexpected tickers format: {type(tickers_result)}")
            return [], False
            
        if not tickers:
            print("❌ Failed to fetch S&P 500 tickers")
            return [], False
        
        return tickers, True
        
    except Exception as e:
        print(f"❌ Error fetching S&P 500 tickers: {e}")
        return [], False


def create_common_parser(description: str) -> argparse.ArgumentParser:
    """Create a common argument parser for bootstrap scripts."""
    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument("--output-dir", default="data/historical", help="Output directory")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to process")
    parser.add_argument("--sp500", action="store_true", help="Use S&P 500 tickers")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to configuration file")
    
    return parser


def validate_tickers(tickers: List) -> bool:
    """Validate tickers list format."""
    if not isinstance(tickers, list):
        print(f"❌ Invalid tickers format: {type(tickers)}")
        return False
    
    if not tickers:
        print("❌ No tickers provided")
        return False
    
    return True


def print_bootstrap_info(tickers: List[str], output_dir: Path, batch_size: int, rate_limit_delay: float):
    """Print bootstrap information."""
    print(f"📊 Processing {len(tickers)} tickers")
    print(f"📁 Output directory: {output_dir}")
    print(f"📦 Batch size: {batch_size}")
    print(f"⏱️  Rate limit delay: {rate_limit_delay} seconds")
    print("-" * 50) 