# Pipeline Fix Scripts

This directory contains scripts to fix common pipeline issues and recover from failures.

## Available Fixes

### `populate_historical.py`

**Purpose**: Populates the historical data directory when it's incomplete or missing.

**When to use**: 
- Historical data directory has fewer tickers than expected
- Pipeline was interrupted before historical data was populated
- Need to bootstrap historical data from existing raw data

**Usage**:
```bash
python tools/maintenance/fixes/populate_historical.py
```

**What it does**:
1. Reads all CSV files from the latest raw data partition
2. Creates historical data structure: `data/raw/historical/ticker=SYMBOL/year=YYYY/`
3. Saves data as Parquet files for efficiency
4. Maintains the same data structure expected by the pipeline

**Example output**:
```
Found 502 CSV files to process
Processed 50 tickers...
Processed 100 tickers...
...
Historical data population completed: 502 successful, 0 failed
```

## Common Issues & Solutions

### Issue: Missing Historical Data
**Symptoms**: 
- `data/raw/historical` has fewer than 500 ticker directories
- Process_features.py fails with "No historical data found" warnings

**Solution**:
```bash
python tools/maintenance/fixes/populate_historical.py
```

### Issue: Pipeline Interruption
**Symptoms**:
- Raw data exists but processed data is missing
- Pipeline logs show "interrupted by user"

**Solution**:
1. Check if historical data is complete
2. If not, run: `python tools/maintenance/fixes/populate_historical.py`
3. Re-run process_features: `python pipeline/process_features.py`

### Issue: Missing Dependencies
**Symptoms**:
- ImportError for fastparquet, tqdm, or other packages

**Solution**:
```bash
pip install fastparquet tqdm
```

## Adding New Fix Scripts

When creating new fix scripts:

1. **Name clearly**: Use descriptive names like `fix_historical_data.py`
2. **Document purpose**: Add clear docstring explaining what the script fixes
3. **Add to this README**: Document when and how to use the script
4. **Test thoroughly**: Ensure the fix doesn't break existing functionality
5. **Include logging**: Add proper logging to track progress and errors

## Best Practices

- **Always backup data** before running fix scripts
- **Test on a small dataset** first when possible
- **Check logs** after running fixes to ensure success
- **Update documentation** when adding new fixes
- **Version control** all fix scripts for reproducibility 