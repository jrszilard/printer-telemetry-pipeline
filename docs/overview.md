# Printer Telemetry Pipeline — Project Brief

A small, runnable demonstration of turning messy printer telemetry into reliable analytical tables.
Python collects **read-only** PrusaLink snapshots and simulates a mixed-generation fleet; DuckDB SQL
normalizes the logs, and Parquet stores the layered results. No cloud account or printer is needed to run
the synthetic demo.

```text
GET-only collection / synthetic fleet
                │
                ▼
retained raw → parsed + quarantine → clean → print jobs / hourly readings / printer dimension
              versioned parsers      UTC/°C + raw-identity dedupe
```

## Engineering problems exercised

- **Format drift:** text logs and two JSON formats, renamed firmware fields, newly introduced event types.
- **Bad data:** truncated lines, unknown events, wrong device clocks, explicit Celsius/Fahrenheit units.
- **Late and repeated uploads:** deterministic deduplication and rebuilding both old/new affected dates.
- **Retry safety:** a manifest makes unchanged reruns a no-op; retained raw enables parser repair.
- **Recovery:** separately committed parser staging bounds intermediate state; the final database update
  is transactional. A durable marker repairs interrupted Parquet publication on retry.
- **Honest models:** incomplete print lifecycles and unknown outcomes remain explicit. Failure rates use
  labelled jobs, not an assumption that missing labels mean success. Hourly samples are not uptime.

## Measured synthetic scale

Three simulated days per fleet, seed 42, local Linux/NVMe workstation; four DuckDB threads.
Ingestion wall time includes exports, quality reporting, and database close, but excludes simulation.

| Synthetic printers | Clean events | Print jobs | Ingestion | Unchanged rerun |
|---:|---:|---:|---:|---:|
| 100 | 96,806 | 600 | 4.861 s | 0.253 s |
| 1,000 | 970,313 | 6,000 | 43.957 s | 0.697 s |
| 10,000 | 9,701,101 | 60,000 | 483.527 s | 6.242 s |

At the largest scale, 61,189 raw files became 366.96 MiB of clean Parquet. Independent simulator truth
matched all job counts, outcomes, material/firmware fields, and complete durations. Warm-cache date-query
medians were **8.411 ms with partition pruning** versus **17.434 ms without**, with matching results.

A **deliberately planted synthetic** firmware/material association measured 5.38% versus 21.99% failures.
This demonstrates cohort analysis and label-aware denominators; it is not evidence about any manufacturer's
hardware, nor a causal finding.

## Example event — entirely synthetic

```json
{
  "ts": "2026-09-28T14:00:00+00:00",
  "device": {"id": "demo-printer", "model": "C", "fw": "3.1.0"},
  "type": "status",
  "state": "PRINTING",
  "job": {"id": "demo-job"},
  "temps": {"bed_c": 80.0, "nozzle_c": 240.0, "chamber_c": 35.0}
}
```

## Scope and limitations

One real idle reading from a Prusa Core One was validated separately; **no real completed print has been
captured**. Real readings, device identifiers, job names, credentials, and local configuration are not
published. The collector cannot upload, start, stop, or configure the printer.

This is a local warehouse model, not a production service or ML model. Gen A clock correction assumes a
fresh upload header, stable clock offset, and short transit. Model tables rebuild in full at demo scale;
Parquet publication is recoverable, not a multi-table atomic snapshot. A 4 GB configured DuckDB managed
memory budget is not a total-process RAM measurement. These local timings do not predict BigQuery costs.

See the [README](../README.md) to run it, [design](design.md) for architecture/BigQuery mapping,
[results](results.md) for measurement details, and [security policy](../SECURITY.md) for publication rules.
