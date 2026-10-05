# Printer Telemetry Pipeline

A runnable, local model of a printer telemetry warehouse: **read-only PrusaLink collection**, a seeded
mixed-generation fleet, SQL parsers, quality checks, incremental ingestion, and Parquet models in DuckDB.
Built to reason concretely about telemetry data engineering—not a production service or an ML model.

```text
PrusaLink GETs ─┐          parser + quarantine        raw-key dedupe + UTC/°C
               ├─ landing JSONL/text ──► parsed ──► clean ──► modeled Parquet
Fleet simulator┘                            DuckDB state + file manifest + quality report
```

For a short shareable introduction, see the [one-page project brief](docs/overview.md).

## Quick start

Python 3.11+; tested on Python 3.14.7 / DuckDB 1.5.6. No cloud account or real printer required.
Run all commands from this repository.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

# About 100,000 synthetic events, including duplicate, malformed, and late uploads.
.venv/bin/python -m telemetry.simulate_fleet --data-dir data/demo --printers 100 --days 3
.venv/bin/python -m telemetry.pipeline --data-dir data/demo

# No new files => no new rows or rewritten Parquet.
.venv/bin/python -m telemetry.pipeline --data-dir data/demo

# v1 quarantines a new maintenance event; v2 understands it. Raw stays intact.
.venv/bin/python -m telemetry.pipeline --data-dir data/demo --parser-version 2 --reprocess

.venv/bin/python -m pytest -q
```

The simulator refuses to overwrite existing synthetic data. Choose another `data/<name>` for a fresh run.
Changing parser versions requires `--reprocess`; ordinary runs only ingest new or modified files.

## Inspect the result

```bash
.venv/bin/python - <<'PY'
import duckdb
with duckdb.connect('data/demo/state.duckdb', read_only=True) as db:
    for row in db.execute('''
        SELECT model, firmware, material, jobs, labelled_jobs,
               round(failure_rate * 100, 1) AS failure_pct
        FROM failure_rates WHERE model = 'C' AND material = 'PETG'
        ORDER BY firmware
    ''').fetchall():
        print(row)
PY
```

A **deliberately planted synthetic effect** raises PETG failures on firmware 3.1.0 versus 3.0.1.
Failure rates divide by success/failure-labelled jobs, not by unknown or missing-end jobs. This is an
analysis demonstration, not a finding about Prusa or Formlabs hardware.

## Semantic definitions

`telemetry/definitions/print_jobs.toml` defines what each print-job field and metric means, written for
people and for AI assistants: plain descriptions, allowed values, synonyms, caveats, an owner, and example
questions. `telemetry/semantic.py` turns those definitions into SQL, so only defined fields can be queried,
every query needs a time range, and ratios are recomputed from their counts for each slice instead of
averaged. Tests check the definitions against the built tables and the example question against
`failure_rates`.

```bash
.venv/bin/python - <<'PY'
from datetime import datetime, timezone
import duckdb
from telemetry.semantic import compile_query, load
sql, parameters = compile_query(load(), ["labelled_prints", "failure_rate"], ["firmware"],
                                {"model": "C", "material": "PETG"},
                                datetime(2000, 1, 1, tzinfo=timezone.utc), datetime(2100, 1, 1, tzinfo=timezone.utc))
with duckdb.connect('data/demo/state.duckdb', read_only=True) as db:
    print(db.execute(sql, parameters).fetchall())
PY
```

| Output | Contents |
|---|---|
| `data/demo/landing/` | Retained raw files, partitioned by receipt date and source |
| `data/demo/parsed/event_date=*/` | Normalized schema, raw timestamp token, units, and file/line lineage |
| `data/demo/clean/event_date=*/` | Deterministically deduplicated events, UTC time, Celsius temperatures |
| `data/demo/unparsed/events.parquet` | Rejected lines with reasons; nothing silently disappears |
| `data/demo/modeled/` | `fact_print_job`, `agg_status_hourly`, `dim_printer` |
| `data/demo/state.duckdb` | Manifest, local transactional state, model tables, `failure_rates` view |
| `data/demo/quality.json` | Coverage, duplicate counts, lateness, outcomes, rejects by firmware, sizes, run statistics |
| `data/demo/truth/` | Simulator-only ground truth; never used to build the models |

## Real printer: collection only

The collector **only makes GET requests** to four allowlisted PrusaLink endpoints. It has no upload,
start, stop, or configuration operations. Serial numbers are hashed before storage. Other response
content is preserved except serial/credential fields. Raw job filenames may still be private.

Keep credentials outside the repo, in `~/.config/printer-telemetry/prusalink.env` (HTTP digest authentication).
Use `--config /path/to/prusalink.env` for another location, or set the environment variables below:

```dotenv
PRUSALINK=your-printer.lan
PRUSALINK_USER=your-user
PRUSALINK_PASSWORD=your-password
```

```bash
# One sample, then exit.
.venv/bin/python -m telemetry.collect_prusa --once --data-dir data/real

# Foreground collection every five seconds; Ctrl-C stops it. Start a print yourself.
.venv/bin/python -m telemetry.collect_prusa --interval 5 --data-dir data/real

# Process captured readings; use the same parser version on later incremental runs.
.venv/bin/python -m telemetry.pipeline --data-dir data/real
```

PrusaLink supplies no device event timestamp here, so these readings explicitly use **receipt time**.
Job snapshots alone do not provide authoritative start/end or success labels. Models retain incomplete
lifecycle information rather than inventing successful jobs. A real idle reading has been validated;
no real print job has been captured yet. The collector is not installed as a service or auto-started.

## Reproduce benchmarks

```bash
.venv/bin/python -m telemetry.bench --data-dir data/bench/new-run --sizes 100 1000 10000 --days 3
```

This generates separate fleets, measures ingestion and warm-cache queries, verifies an unchanged second
run and ground-truth job counts, and demonstrates parser repair at the smallest scale. `--resume` skips
completed scales; incomplete directories are never overwritten. Detailed JSON and EXPLAIN plans stay
under `data/bench/`. Timings are local measurements, not predictions of BigQuery performance.

See [log formats](docs/log-formats.md), [design and limitations](docs/design.md), [measured results](docs/results.md), and
[publication safety](SECURITY.md).

**Safety:** runtime data/exports, dotenv variants, credential/key files, local agent settings, and
interview-only notes are ignored by Git. Use paths under `data/` for all generated or collected output.
Never commit credentials, actual printer serials, or raw real-printer logs. Ignore rules cannot catch
secrets embedded in source or remove already-tracked history; review and scan the publication tree.
Run one pipeline writer per dataset. Do not read Parquet during publication; rerun after an interrupted
export to repair it. A size/mtime fast path avoids rereading unchanged raw files; `--reprocess` bypasses it.
