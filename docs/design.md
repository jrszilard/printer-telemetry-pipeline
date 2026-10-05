# Design: a small telemetry warehouse

```text
                       immutable-by-convention raw retention
PrusaLink GET ─┐        received_date=... / source=...
              ├──► JSONL / text ──► SQL parser v1/v2 ─┬──► unparsed + rejection reason
Seeded fleet ─┘                                      └──► parsed / event_date
                                                            │
                          raw identity + earliest receipt ───┘
                          UTC provenance + explicit °C
                                                            ▼
                                                    clean / event_date
                                                            │
                          ┌─────────────────────────────────┼───────────────────┐
                          ▼                                 ▼                   ▼
                     fact_print_job                 agg_status_hourly     dim_printer
```

## Contracts and grain

- **Raw:** receipt time in the filename; Gen A adds an upload header, Prusa readings add per-line receipt
  time. Preserve raw so parser changes can be replayed. Real serial/credential fields are removed at collection.
- **Parsed:** one accepted line, retaining source, firmware, raw timestamp, unit, parser version, file, and
  line number. SQL uses `read_text` → line splitting → regex/JSON extraction. Unknown, truncated, invalid-time,
  missing-key, or invalid-temperature lines go to quarantine. Coverage excludes headers and blank lines.
- **Quarantine:** each rejected line keeps its device and firmware, taken from the line, the Gen A upload
  header, or the readable lines of the same upload, with `firmware_source` saying which (or `unknown`). The
  quality report's `rejects_by_firmware` gives reject rates per source and firmware and lists
  `format_change_suspects`: a rejection reason produced by only one firmware of a source that has several,
  the signature of a release that changed the format. It is a heuristic for review and never blocks a load.
- **Clean:** one event identity. Status identity is `(device, kind, raw timestamp, state, job)`; lifecycle
  identity is `(device, kind, raw timestamp, job)`. SHA-256 encodes an unambiguous tuple. Earliest receipt
  wins, then path/line break ties. Temperature conversion requires an explicit unit (legacy default is °C).
- **Jobs:** one `(device, job_id)`, assuming job ids are unique within a device. Includes observed boundaries,
  explicit start/end, duration when both exist, temperatures, error counts, material, firmware, and outcome.
  `unknown` and `no_end_event` are not success. Firmware/material cohorts use only labelled jobs as the
  failure-rate denominator. Hourly rows are **sample counts**, not uptime estimates.

## Time, late data, and replays

Gen B epoch milliseconds and Gen C offset-bearing ISO timestamps become UTC. Gen A has neither a reliable
clock nor timezone: `event_ts = received_at - (header_device_time - raw_device_time)`. That is an **upload
clock-offset estimate**, not recovered ground truth. It requires a freshly generated header, stable clock offset over the buffered interval, and bounded
network transit; buffered historical headers cannot distinguish clock skew from upload delay. The simulator
models a fresh header even after days offline. Prusa observations use `received_only` time.

A manifest tracks path, size, mtime, content checksum, parser version, and coverage. Unchanged size/mtime
skips raw reads; changed candidates with identical checksums only refresh metadata. Parsing stages 500-file
batches in separate commits, releasing intermediate tables before the final transaction applies all rows
and manifest/model changes. Disposable staging is discarded on retry. Actively appended collector files
are ingested as snapshots and replaced on later change.
Missing tracked raw files fail loudly. Parser version changes require a full `--reprocess` from raw.

For affected raw identities, look up all candidates and replace the canonical clean row. Invalidate **both
old and new corrected dates**, including when a retransmission moves a record across midnight. New parsed
batches append; touched clean partitions compact. A changed raw file also recompacts affected parsed dates.
All three model tables rebuild in full at this demo's scale.

DuckDB transactions protect the final ingestion and manifest/model updates. A durable dirty-export marker is committed
before publishing Parquet; a failed publication triggers full export repair next run. Individual files are
renamed into place, but this is **not a multi-table atomic snapshot**. One writer; readers wait until a run
finishes. Raw retention is the recovery source.

## How this maps to BigQuery

| Local demo | Production direction, subject to actual source contracts |
|---|---|
| Raw landing files | Object storage, retained raw objects, immutable object ids/checksums |
| SQL parsers + quarantine | Versioned batch/stream ingestion; schema/firmware contracts and reject monitoring |
| Event-date Parquet | Event-time partitioned tables; arrival partition retained for audit/backfill |
| Sort by device/time | Cluster on common device/firmware access paths, guided by query workload |
| Dedupe + touched partitions | Stable event ids; `MERGE`/partition replacement; retry-safe orchestrated backfills |
| SQL model rebuilds | dbt/scheduled models; incremental job state plus bounded late-data reconciliation |
| `quality.json` | Coverage, freshness, uniqueness, completeness, drift, and label-availability alerts |
| `telemetry/definitions/*.toml` | Semantic layer (for example LookML): one owned definition per metric, descriptions and synonyms for people and AI tools, required time filters |

**Deliberate limits:** single machine; state database duplicates exported data; local metadata fast path,
not adversarial file integrity; no cloud deployment/scheduler; no live metrics-stream support; no production
exactly-once guarantee; no real completed-print outcome observed. Raw-key dedupe assumes one status per
raw timestamp/state/job and will not solve every producer identity problem. Three days of warm-cache
benchmarks are evidence about this implementation only. A firmware/material association is not causation.
