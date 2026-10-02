"""Reproduce scale, no-op, parser-repair, and partition-pruning measurements."""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
import platform
import re
import statistics
import sys
import time

import duckdb

from telemetry.pipeline import run_pipeline, write_json
from telemetry.simulate_fleet import simulate

QUERY = """
SELECT count(*) AS events, avg(nozzle_c) AS avg_nozzle_c
FROM read_parquet($files, hive_partitioning=true)
WHERE event_date = $day::DATE
"""


def parquet_signature(root: Path) -> dict:
    return {str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
            for layer in ("parsed", "clean", "modeled", "unparsed")
            for path in (root / layer).rglob("*.parquet")}


def query_benchmark(root: Path, repetitions: int = 5) -> dict:
    with duckdb.connect() as connection:
        connection.execute("SET threads = 4; SET TimeZone = 'UTC'")
        files = str(root / "clean/event_date=*/*.parquet")
        day = connection.execute("SELECT min(event_date) FROM read_parquet(?)", [files]).fetchone()[0]
        parameters = {"files": files, "day": str(day)}
        timings: dict[str, list[float]] = {"pruned": [], "unpruned": []}
        plans = {}
        results = {}
        for mode, disabled in (("pruned", ""), ("unpruned", "filter_pushdown")):
            connection.execute("SET disabled_optimizers = ?", [disabled])
            plans[mode] = connection.execute("EXPLAIN " + QUERY, parameters).fetchone()[1]
            results[mode] = connection.execute(QUERY, parameters).fetchone()  # warm-up
        if results["pruned"][0] != results["unpruned"][0] or not math.isclose(results["pruned"][1], results["unpruned"][1], rel_tol=1e-10):
            raise AssertionError("Pruned and unpruned queries returned different results")
        # Alternate order to reduce systematic cache/order bias. These are warm-cache measurements.
        for iteration in range(repetitions):
            order = ("pruned", "unpruned") if iteration % 2 == 0 else ("unpruned", "pruned")
            for mode in order:
                connection.execute("SET disabled_optimizers = ?", ["filter_pushdown" if mode == "unpruned" else ""])
                started = time.perf_counter()
                connection.execute(QUERY, parameters).fetchone()
                timings[mode].append(time.perf_counter() - started)
        connection.execute("SET disabled_optimizers = ''")
    ratio = re.search(r"Scanning Files: (\d+)/(\d+)", plans["pruned"])
    (root / "query-pruned.txt").write_text(plans["pruned"])
    (root / "query-unpruned.txt").write_text(plans["unpruned"])
    if "File Filters" in plans["unpruned"]:
        raise AssertionError("Unpruned plan unexpectedly contains file pruning")
    return {
        "date": str(day), "events": results["pruned"][0], "avg_nozzle_c": results["pruned"][1],
        "identical_results": True, "warm_cache": True, "repetitions": repetitions,
        "pruned_files": int(ratio[1]) if ratio else len(list((root / "clean").rglob("*.parquet"))),
        "total_files": int(ratio[2]) if ratio else len(list((root / "clean").rglob("*.parquet"))),
        "pruned_median_ms": round(statistics.median(timings["pruned"]) * 1000, 3),
        "unpruned_median_ms": round(statistics.median(timings["unpruned"]) * 1000, 3),
        "timings_ms": {key: [round(value * 1000, 3) for value in values] for key, values in timings.items()},
        "unpruned_method": "Same SQL; disable DuckDB filter_pushdown optimizer (verified by EXPLAIN)",
    }


def benchmark_size(root: Path, printers: int, days: int, repetitions: int = 5, reprocess: bool = False) -> dict:
    if root.exists():
        raise ValueError(f"Benchmark directory already exists: {root}; use a new output directory")
    simulation = simulate(root, printers=printers, days=days)
    started = time.perf_counter()
    first = run_pipeline(root, parser_version=1)
    first_seconds = time.perf_counter() - started  # includes database checkpoint/close
    signature = parquet_signature(root)
    started = time.perf_counter()
    second = run_pipeline(root, parser_version=1)
    noop_seconds = time.perf_counter() - started
    if not second["run"]["no_op"] or first["rows"] != second["rows"] or signature != parquet_signature(root):
        raise AssertionError("Second run changed rows or Parquet exports")
    if first["rows"]["fact_print_job"] != simulation["jobs"]:
        raise AssertionError("Modeled job count does not match ground truth")
    with duckdb.connect(str(root / "state.duckdb"), read_only=True) as connection:
        connection.execute("SET TimeZone = 'UTC'")
        duplicate_keys = connection.execute("SELECT count(*) - count(DISTINCT dedupe_key) FROM clean_events").fetchone()[0]
        if duplicate_keys:
            raise AssertionError("Clean identities are not unique")
        max_clock_error = connection.execute("""
          SELECT max(abs(epoch(f.start_ts - t.start_ts::TIMESTAMPTZ)))
          FROM fact_print_job f JOIN read_parquet(?) t USING (device_id, job_id) WHERE f.source = 'genA'
        """, [str(root / "truth/jobs.parquet")]).fetchone()[0]
        if max_clock_error is not None and max_clock_error > 90:
            raise AssertionError("Synthetic Gen A clock estimates do not match stated assumptions")
    result = {"printers": printers, "days": days, "simulation": simulation,
              "pipeline_seconds": round(first_seconds, 3), "noop_seconds": round(noop_seconds, 3),
              "noop_verified": True, "ground_truth_jobs_verified": True,
              "clean_duplicate_keys": duplicate_keys, "max_genA_clock_error_seconds": max_clock_error,
              "database_bytes": (root / "state.duckdb").stat().st_size,
              "quality_v1": first, "query": query_benchmark(root, repetitions)}
    if reprocess:
        started = time.perf_counter()
        repaired = run_pipeline(root, parser_version=2, reprocess=True)
        before = next(row for row in first["coverage_by_source"] if row["source"] == "genC")
        after = next(row for row in repaired["coverage_by_source"] if row["source"] == "genC")
        if after["parsed"] <= before["parsed"] or repaired["rows"]["fact_print_job"] != first["rows"]["fact_print_job"]:
            raise AssertionError("Parser repair did not improve coverage while retaining job counts")
        result["parser_repair"] = {"seconds": round(time.perf_counter() - started, 3),
                                   "genC_coverage_before": before["coverage"], "genC_coverage_after": after["coverage"],
                                   "extra_parsed_events": after["parsed"] - before["parsed"],
                                   "job_count_unchanged": True}
    write_json(root / "benchmark.json", result)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/bench"))
    parser.add_argument("--sizes", type=int, nargs="+", default=[100, 1000, 10000])
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--resume", action="store_true", help="Skip completed scales; never overwrite an incomplete scale")
    args = parser.parse_args(argv)
    if any(size < 30 for size in args.sizes) or args.days < 2 or args.repetitions < 1:
        parser.error("sizes must be >= 30, days >= 2, repetitions >= 1")
    metadata = {"python": sys.version.split()[0], "duckdb": duckdb.__version__,
                "os": platform.system(), "architecture": platform.machine(), "logical_cpus": os.cpu_count(),
                "ram_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                "pipeline_threads": 4, "pipeline_memory_limit": "4GB", "seed": 42,
                "notes": "Local single-machine, warm-cache query timings; no extrapolation to BigQuery."}
    results = []
    import json
    try:
        for size in sorted(set(args.sizes)):
            root = args.data_dir / f"scale-{size}"
            if args.resume and (root / "benchmark.json").exists():
                result = json.loads((root / "benchmark.json").read_text())
                if result["days"] != args.days or result["query"]["repetitions"] != args.repetitions:
                    raise ValueError("Existing benchmark settings differ; use a new output directory")
            else:
                print(f"\n--- {size} printers x {args.days} days ---", flush=True)
                result = benchmark_size(root, size, args.days, args.repetitions, reprocess=size == min(args.sizes))
            results.append(result)
            write_json(args.data_dir / "benchmarks.json", {"environment": metadata, "results": results})
            print(f"scale={size} clean_rows={result['quality_v1']['rows']['clean_events']} "
                  f"pipeline={result['pipeline_seconds']}s noop={result['noop_seconds']}s "
                  f"pruned={result['query']['pruned_median_ms']}ms unpruned={result['query']['unpruned_median_ms']}ms", flush=True)
    except (ValueError, AssertionError, duckdb.Error, OSError) as exc:
        parser.exit(1, f"Benchmark failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
