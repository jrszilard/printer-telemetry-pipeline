# Measured results — 2026-10-02

**Synthetic fleets, not production or Formlabs measurements.** Seed 42; three days per fleet.
Linux x86-64 / local NVMe SSD / 16 logical CPUs / 31.25 GiB RAM; Python 3.14.7, DuckDB 1.5.6.
Pipeline configured for four threads, 500-file parser batches, and a **4 GB DuckDB managed-memory limit**.
That limit is not a measured total-process RAM cap. Ordinary workstation conditions; OS cache not flushed.

## Ingestion and repeatability

| Printers | Raw files | Raw event lines | Clean events | Print jobs | Pipeline wall time | No-op rerun |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 608 | 98,653 | 96,806 | 600 | 4.861 s | 0.253 s |
| 1,000 | 6,126 | 996,501 | 970,313 | 6,000 | 43.957 s | 0.697 s |
| 10,000 | 61,189 | 9,951,892 | 9,701,101 | 60,000 | 483.527 s | 6.242 s |

Pipeline time includes parsing, dedupe, full model rebuilds, exports, quality reporting, and database
close/checkpoint. Generation is separate: 1.123 / 11.151 / 117.567 seconds respectively. Each ingestion
was measured once. All three reruns processed **zero files**, preserved row counts, and left Parquet
sizes/mtimes unchanged. A no-op still scans metadata and computes quality statistics; it is not zero work.
All job counts, labels, material/firmware fields, and complete durations matched independent truth;
exported Parquet counts matched state, and clean duplicate-key counts were zero. Maximum Gen A start-time
error was 90 seconds, under the simulator's stated fresh-header/short-transit/stable-clock assumptions.

### Storage, parser v1

Logical file sizes in MiB (2²⁰ bytes); truth and benchmark artifacts excluded. Database size is separate
and includes local state plus reusable pages from disposable staging.

| Printers | Raw | Parsed Parquet | Clean Parquet | Models Parquet | State database |
|---:|---:|---:|---:|---:|---:|
| 100 | 17.19 | 3.72 | 3.68 | 0.10 | 25.51 |
| 1,000 | 174.37 | 37.16 | 36.81 | 0.89 | 283.76 |
| 10,000 | 1,740.46 | 370.59 | 366.96 | 8.71 | 2,437.51 |

At the largest scale, 222,336 repeated parsed candidates were removed and 28,455 rejected lines retained.
Parse coverage: Gen A **99.693%**, Gen B **99.799%**, Gen C **99.650%**. Unknown debug/maintenance events and
truncations explain the gap, not silent drops. The raw small-file count is a real operational cost;
compression alone does not address listing/opening 61,189 objects.

## Partition pruning

Same SQL: count events and average nozzle temperature for 2026-09-28. Warm-up then five timed repetitions,
alternating execution order; medians below. Normal EXPLAIN scans **1 of 3 clean files**. The comparison
turns off DuckDB's `filter_pushdown` optimizer and verifies no file filter; query results match.

| Printers | Pruned | Unpruned |
|---:|---:|---:|
| 100 | 1.141 ms | 1.261 ms |
| 1,000 | 1.916 ms | 2.635 ms |
| 10,000 | 8.411 ms | 17.434 ms |

These are warm-cache local timings, not cold-storage or BigQuery latency/cost predictions.

## Parser repair and useful analysis

Replayed the 100-printer raw dataset with parser v2: Gen C coverage rose **99.663% → 99.785%**, accepting
40 additional maintenance lines, with all **600 jobs unchanged**. Replay took 5.090 seconds. Other rejected
lines remained quarantined. This demonstrates why raw retention and versioned parsers matter.

The deliberately planted **model C / PETG** effect is visible in the 10,000-printer dataset:

| Firmware | All cohort jobs | Labelled jobs | Failures | Labelled failure rate |
|---|---:|---:|---:|---:|
| 3.0.1 | 3,471 | 3,236 | 174 | **5.38%** |
| 3.1.0 | 3,364 | 3,147 | 692 | **21.99%** |

Across the whole fleet, 2,359 jobs have unknown labels and 1,782 have no end event; neither is called
success or counted in the labelled-rate denominator. Small cohorts are noisy: the 100-printer new-firmware
cohort measured 11/30 = 36.7%, despite a planted 22% probability. Association is not causation, and this
synthetic effect says nothing about any manufacturer's hardware.

## Failure learned from, real-data boundary, and reproduction

The initial giant parse transaction exhausted its 4 GB budget during the 1,000-printer run. Separately
committed disposable parser staging fixed intermediate-state retention, while a final transaction still
updates warehouse rows/manifest/models together. The largest successful run spilled to disk. The original
**23 pipeline tests passed**, including failed-stage/transaction rollback, interrupted-export recovery,
late-data partition replacement, parser replay, UTC/units, and privacy checks. The public release adds
48 publication-safety regressions: **71 tests pass** in the combined suite.

**Real data:** one read-only Prusa Core One idle snapshot was captured and processed separately. No real
completed print, success label, or fleet-wide hardware finding has been observed. No printer upload or
control occurred; the collector is not continuously running.

```bash
.venv/bin/python -m telemetry.bench --data-dir data/bench/fresh-run --sizes 100 1000 10000 --days 3
```

Original detailed JSON/EXPLAIN artifacts: `data/bench/run-2026-10-02-staged/` (Git-ignored).
The 100-printer output finishes at parser v2; the two larger datasets remain v1. These measurements
precede the public release; generated artifacts and real telemetry are not part of that release.
See [design](design.md) for the BigQuery mapping and the [project brief](overview.md) for a short overview.
