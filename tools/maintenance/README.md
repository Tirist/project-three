# Maintenance Scripts

This directory contains maintenance scripts for the stock evaluation pipeline.

## Current Approach

**Use the pipeline, not bootstrap scripts!** The pipeline is much more efficient and uses the same data sources.

### Recommended: Fill Data Gaps Script

Use `fill_data_gaps.py` to check for missing data and run the pipeline to fill gaps:

```bash
python tools/maintenance/fill_data_gaps.py
```

This script:
1. Checks the latest date in historical data
2. Determines if there are gaps
3. Runs the pipeline to fetch missing data
4. Much faster than bootstrap scripts

## Deprecated Scripts

The following scripts have been moved to `deprecated/` and should **NOT** be used:

- `DO_NOT_USE_bootstrap_historical_data.py` - Alpha Vantage bootstrap (rate limited)
- `DO_NOT_USE_bootstrap_yfinance.py` - yfinance bootstrap (redundant)
- `DO_NOT_USE_demo_bootstrap.py` - Demo script (no longer needed)
- `DO_NOT_USE_consolidated_bootstrap.py` - Consolidated bootstrap (redundant)
- `DO_NOT_USE_populate_historical.py` - Historical data population (redundant)

## Why Pipeline Over Bootstrap?

1. **Efficiency**: Pipeline fetches only missing data, not entire history
2. **Consistency**: Uses same data sources and logic as daily runs
3. **Rate Limits**: Pipeline handles rate limiting properly
4. **Maintenance**: Less code to maintain
5. **Speed**: Much faster than bootstrap scripts

## Available Scripts

### Active Scripts

- **`fill_data_gaps.py`** - Check and fill data gaps using pipeline
- **`terminate_stuck_run.py`** - Terminate stuck pipeline runs
- **`organize_project.py`** - Organize project structure

### Utility Scripts

- **`base_bootstrapper.py`** - Base class for bootstrapping (kept for reference)
- **`bootstrap_utils.py`** - Utility functions (kept for reference)
- **`test_refactored_bootstrap.py`** - Test script (kept for reference)

## Usage

### Check and Fill Data Gaps

```bash
# Check for gaps and run pipeline if needed
python tools/maintenance/fill_data_gaps.py
```

### Terminate Stuck Runs

```bash
# Terminate stuck pipeline runs
python tools/maintenance/terminate_stuck_run.py
```

### Organize Project

```bash
# Organize project structure
python tools/maintenance/organize_project.py
```

## Data Sources

The pipeline uses:
- **Primary**: yfinance (fast, reliable, no rate limits)
- **Fallback**: Alpha Vantage (rate limited, used only if yfinance fails)

This is much better than the old approach of using Alpha Vantage as the primary source. 