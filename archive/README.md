## Archive Policy

This directory stores retired project assets that are no longer part of the active runtime path.

### Folder convention

- Use dated cleanup folders like `2026-04-cleanup`.
- Mirror original source paths under the dated folder when possible.
- Keep a short manifest entry for each moved file in the section below.

### Restore a file

1. Copy the file back to its original live path.
2. Re-run tests or the affected command.
3. Remove or update stale references in docs.

### Safety notes

- Archived scripts may be destructive, stale, or tied to old project layouts.
- Do not run archived scripts blindly.

## Manifest

| Date | From | To | Reason |
|------|------|----|--------|
| 2026-04-16 | `tools/maintenance/organize_project.py` | `archive/2026-04-cleanup/tools/maintenance/organize_project.py` | One-time reorg script; risky to re-run. |
| 2026-04-16 | `tools/maintenance/demo_bootstrap.py` | `archive/2026-04-cleanup/tools/maintenance/demo_bootstrap.py` | Legacy Alpha Vantage demo with stale assumptions. |
| 2026-04-16 | `tools/diagnostics/fix_test_suite.py` | `archive/2026-04-cleanup/tools/diagnostics/fix_test_suite.py` | Script mutates tests; should not be run accidentally. |
| 2026-04-16 | `tools/diagnostics/evaluate_bootstrap_failures.py` | `archive/2026-04-cleanup/tools/diagnostics/evaluate_bootstrap_failures.py` | Stale diagnostic path assumptions. |
| 2026-04-16 | `PROJECT_ORGANIZATION_SUMMARY.md` | `archive/2026-04-cleanup/docs/PROJECT_ORGANIZATION_SUMMARY.md` | Snapshot summary no longer part of live docs. |
| 2026-04-16 | `reports/status/IMMEDIATE_ACTION_PLAN.md` | `archive/2026-04-cleanup/docs/reports/status/IMMEDIATE_ACTION_PLAN.md` | Point-in-time operations document. |
| 2026-04-16 | `reports/status/IMMEDIATE_ACTIONS_SUMMARY.md` | `archive/2026-04-cleanup/docs/reports/status/IMMEDIATE_ACTIONS_SUMMARY.md` | Point-in-time completion snapshot. |
| 2026-04-16 | `reports/analysis/bootstrap_failure_analysis.md` | `archive/2026-04-cleanup/docs/reports/analysis/bootstrap_failure_analysis.md` | Historical analysis superseded by current workflow docs. |
