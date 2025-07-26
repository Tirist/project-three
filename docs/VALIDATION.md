# End-to-End Validation Checklist

This guide helps you validate the entire stock evaluation pipeline, from ticker ingestion to feature engineering.

---

## 1. Run the Full Test Pipeline

Execute the following commands in order:

```bash
python3 fetch_tickers.py --test --force
python3 fetch_data.py --test --batch-size 10 --cooldown 2
python3 process_features.py --test
```

---

## 2. Expected Directory Outputs

After a successful run, you should see the following files and folders:

```
data/tickers/dt=YYYY-MM-DD/tickers.csv
data/raw/dt=YYYY-MM-DD/AAPL.csv
data/processed/dt=YYYY-MM-DD/features.parquet
logs/tickers/dt=YYYY-MM-DD/metadata.json
logs/fetch/dt=YYYY-MM-DD/metadata.json
logs/fetch/dt=YYYY-MM-DD/errors.json
logs/features/dt=YYYY-MM-DD/metadata.json
```

---

## 3. Inspecting Sample Outputs

- **Tickers CSV:**
  - Open `data/tickers/dt=YYYY-MM-DD/tickers.csv` in a spreadsheet or text editor.
  - Confirm it contains S&P 500 tickers and company names.

- **OHLCV CSV:**
  - Open `data/raw/dt=YYYY-MM-DD/AAPL.csv`.
  - Confirm columns: `Date, Open, High, Low, Close, Volume` and recent data rows.

- **Features Parquet:**
  - Use pandas or Parquet viewer to inspect `data/processed/dt=YYYY-MM-DD/features.parquet`:
    ```python
    import pandas as pd
    df = pd.read_parquet('data/processed/dt=YYYY-MM-DD/features.parquet')
    print(df.head())
    print(df.columns)
    ```
  - Confirm presence of technical indicator columns (e.g., SMA_5, RSI_14, MACD, BB_Upper).

- **Sample Features (if --sample used):**
  - Open `data/processed/sample_features.csv` to quickly review a small subset.

---

## 4. Verifying Metadata and Logs

- **metadata.json:**
  - Open each `metadata.json` (tickers, fetch, features).
  - Confirm fields like:
    - `rate_limit_hits`
    - `total_sleep_time`
    - `batch_size`
    - `cooldown_seconds`
    - `status` (should be "success")
    - `tickers_processed`, `tickers_successful`, `tickers_failed`

- **errors.json:**
  - Open `logs/fetch/dt=YYYY-MM-DD/errors.json`.
  - Confirm that any failed tickers are listed with error messages and timestamps.

- **cleanup.json:**
  - Open the latest `logs/cleanup/cleanup_YYYY-MM-DD.json`.
  - Confirm old partitions are deleted as expected.

---

## 5. Rate Limit and Cooldown Testing

- To simulate rate limits, use the `--debug-rate-limit` flag (if enabled):
  ```bash
  python3 fetch_data.py --test --debug-rate-limit
  ```
- Confirm that metadata logs real cooldown events and cumulative sleep times.
- Check logs for messages about rate limit handling and cooldowns.

---

## 6. Summary Log Review

At the end of each script run, check the console for a summary log showing:
- Number of tickers processed
- Total runtime and sleep time
- Number of errors (with path to errors.json)

---

## 7. Troubleshooting

- If any step fails, check the corresponding `metadata.json` and `errors.json` for details.
- Use `--dry-run` to simulate runs without writing files.
- Use `--progress` to enable a real-time progress bar.

---

## 8. Additional Validation

- Run the full test suite:
  ```bash
  pytest tests/
  ```
- Confirm all tests pass for batching, cooldown, error handling, and feature calculations.

---

**For more details, see the main [README.md](../README.md).** 