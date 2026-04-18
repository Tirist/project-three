# Operations (portable layout)

## Golden path (full universe, production data directories)

From the repository root, after `.venv` and `pip install -r requirements.txt`:

```bash
bash ops/run_prod_data.sh
```

Equivalent:

```bash
python pipeline/run_pipeline.py --full --parallel 8 --skip-tests
```

For interactive runs that also execute pytest:

```bash
python pipeline/run_pipeline.py --full --parallel 8
```

## Environment

See [env.example](env.example). Copy to `.env` in the repo root for local overrides (do not commit `.env`).

## Scheduling

- **macOS**: [com.projectthree.pipeline.plist.example](com.projectthree.pipeline.plist.example) — install under `~/Library/LaunchAgents/` and adjust paths.
- **Linux**: [pipeline.service.example](pipeline.service.example) — install as a systemd user unit.
- **cron**: `bash scripts/setup_cron.sh` installs example jobs; the daily data line calls `ops/run_prod_data.sh`.

## Recovery vs routine `--prod`

`python pipeline/run_pipeline.py --prod` wipes **all** of `data/processed` and `logs/features` before running (recovery). For routine runs without that wipe, use `--prod --prod-no-clean`. Prefer `ops/run_prod_data.sh` or `--full --skip-tests` for scheduled daily data.
