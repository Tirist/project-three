#!/usr/bin/env python3
"""
clean_pipeline_data.py

Deletes data/processed/* and logs/features/* for a fresh pipeline/test run.
Safe to call manually or from CI/CD.
"""
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_pipeline_data():
    dirs_to_clean = [
        Path('data/processed'),
        Path('logs/features'),
    ]
    for d in dirs_to_clean:
        if d.exists():
            logging.info(f"Cleaning directory: {d}")
            for item in d.iterdir():
                try:
                    if item.is_dir():
                        shutil.rmtree(item)
                        logging.info(f"Deleted directory: {item}")
                    else:
                        item.unlink()
                        logging.info(f"Deleted file: {item}")
                except Exception as e:
                    logging.warning(f"Failed to delete {item}: {e}")
        else:
            logging.info(f"Directory does not exist, skipping: {d}")

if __name__ == "__main__":
    print("\n=== Cleaning pipeline data (processed, logs) ===\n")
    clean_pipeline_data()
    print("Cleanup complete.") 