# Printer telemetry safety and development

- Work only in this repository; do not use sibling copies or linked worktrees.
- Prusa access is **read-only GET only**. Never upload, start/stop, or configure the printer.
- Credentials live outside the repo. Never print them, persist actual serials, or commit real raw logs.
- Keep generated/collected outputs under ignored `data/`; only synthetic fixtures belong in source control.
- One pipeline writer per dataset. Do not read Parquet during publication; rerun after interrupted exports.
- Run `.venv/bin/python -m pytest -q` after changes. Benchmarks use fresh output directories.
- Preserve timestamp provenance and unknown/missing job outcomes. Synthetic effects are not real findings.
- Do not commit or publish unless the owner asks. Follow `SECURITY.md` before publishing.
- When the local workspace knowledge base is available, follow its protocol; project notes are under
  `../../knowledge-base/projects/printer-telemetry-pipeline/`. Local agent settings stay untracked.
