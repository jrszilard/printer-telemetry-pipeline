"""Incremental raw -> parsed -> clean -> modeled telemetry, in DuckDB and Parquet."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import shutil
import time
import uuid

import duckdb

SQL_DIR = Path(__file__).with_name("sql")
TABLE_SCHEMA = """
 path VARCHAR, line_number BIGINT, parser_version INTEGER, source VARCHAR,
 device_id VARCHAR, model VARCHAR, firmware VARCHAR, kind VARCHAR, device_ts_raw VARCHAR,
 received_at TIMESTAMPTZ, event_ts TIMESTAMPTZ, event_date DATE, timestamp_quality VARCHAR,
 clock_offset_seconds DOUBLE, job_id VARCHAR, state VARCHAR, material VARCHAR, layers INTEGER,
 bed_temp DOUBLE, nozzle_temp DOUBLE, chamber_temp DOUBLE, target_bed_temp DOUBLE,
 target_nozzle_temp DOUBLE, temp_unit VARCHAR, outcome VARCHAR, error_code VARCHAR, dedupe_key VARCHAR
"""
CLEAN_SELECT = """
SELECT * EXCLUDE (bed_temp, nozzle_temp, chamber_temp, target_bed_temp, target_nozzle_temp, temp_unit),
 CASE WHEN temp_unit = 'F' THEN (bed_temp - 32) / 1.8 ELSE bed_temp END AS bed_c,
 CASE WHEN temp_unit = 'F' THEN (nozzle_temp - 32) / 1.8 ELSE nozzle_temp END AS nozzle_c,
 chamber_temp AS chamber_c,
 CASE WHEN temp_unit = 'F' THEN (target_bed_temp - 32) / 1.8 ELSE target_bed_temp END AS target_bed_c,
 CASE WHEN temp_unit = 'F' THEN (target_nozzle_temp - 32) / 1.8 ELSE target_nozzle_temp END AS target_nozzle_c
FROM canonical_events
"""


def records(connection, sql: str, parameters=None) -> list[dict]:
    cursor = connection.execute(sql, parameters or [])
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=json_default) + "\n")
    temporary.replace(path)


def connect(data_dir: Path, threads: int = 4, memory_limit: str = "4GB"):
    data_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(data_dir / "state.duckdb"))
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("SET threads = ?", [threads])
    connection.execute("SET memory_limit = ?", [memory_limit])
    connection.execute("SET temp_directory = ?", [str(data_dir / "tmp")])
    connection.execute("SET preserve_insertion_order = false")
    return connection


def initialize(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS manifest (
          path VARCHAR PRIMARY KEY, size BIGINT, mtime_ns BIGINT, sha256 VARCHAR,
          parser_version INTEGER, source VARCHAR, lines BIGINT, parsed BIGINT
        );
        CREATE TABLE IF NOT EXISTS pipeline_meta (key VARCHAR PRIMARY KEY, value VARCHAR);
        INSERT INTO pipeline_meta VALUES ('exports_dirty', 'false') ON CONFLICT DO NOTHING;
    """)
    connection.execute(f"CREATE TABLE IF NOT EXISTS parsed_events ({TABLE_SCHEMA})")
    # Databases created before rejected lines carried firmware gain the columns; --reprocess fills them.
    connection.execute("""
        CREATE TABLE IF NOT EXISTS unparsed (
          path VARCHAR, line_number BIGINT, parser_version INTEGER, source VARCHAR,
          received_at TIMESTAMPTZ, line VARCHAR, reason VARCHAR,
          device_id VARCHAR, firmware VARCHAR, firmware_source VARCHAR
        );
        ALTER TABLE unparsed ADD COLUMN IF NOT EXISTS device_id VARCHAR;
        ALTER TABLE unparsed ADD COLUMN IF NOT EXISTS firmware VARCHAR;
        ALTER TABLE unparsed ADD COLUMN IF NOT EXISTS firmware_source VARCHAR;
        CREATE OR REPLACE TEMP VIEW canonical_events AS SELECT * FROM parsed_events WHERE false;
    """)
    connection.execute(f"CREATE TABLE IF NOT EXISTS clean_events AS {CLEAN_SELECT}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def export_partitions(connection, data_dir: Path, table: str, dates: set[date],
                      full: bool, append_run: str | None = None, where: str = "true"):
    folder = data_dir / ("parsed" if table == "parsed_events" else "clean")
    folder.mkdir(exist_ok=True)
    if full:
        dates = {row[0] for row in connection.execute(f"SELECT DISTINCT event_date FROM {table}").fetchall()}
        for stale in folder.glob("event_date=*"):
            if stale.name.split("=", 1)[1] not in {str(day) for day in dates}:
                shutil.rmtree(stale)
    for day in sorted(dates):
        partition = folder / f"event_date={day}"
        if not append_run and partition.exists():
            shutil.rmtree(partition)
        partition.mkdir(exist_ok=True)
        count = connection.execute(f"SELECT count(*) FROM {table} WHERE event_date = ? AND ({where})", [day]).fetchone()[0]
        if count == 0:
            if not append_run:
                partition.rmdir()
            continue
        target = partition / f"part_{append_run or 'compact'}.parquet"
        temporary = target.with_suffix(".parquet.tmp")
        connection.execute(
            f"COPY (SELECT * EXCLUDE (event_date) FROM {table} WHERE event_date = $event_date AND ({where}) "
            "ORDER BY device_id, event_ts, kind) TO $output (FORMAT PARQUET, COMPRESSION ZSTD)",
            {"event_date": day, "output": str(temporary)},
        )
        temporary.replace(target)


def export_models(connection, data_dir: Path):
    folder = data_dir / "modeled"
    folder.mkdir(exist_ok=True)
    for table in ("fact_print_job", "agg_status_hourly", "dim_printer"):
        target = folder / f"{table}.parquet"
        temporary = target.with_suffix(".parquet.tmp")
        connection.execute(f"COPY {table} TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(temporary)])
        temporary.replace(target)
    folder = data_dir / "unparsed"
    folder.mkdir(exist_ok=True)
    target = folder / "events.parquet"
    temporary = target.with_suffix(".parquet.tmp")
    connection.execute("COPY unparsed TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(temporary)])
    temporary.replace(target)


def rejects_by_firmware(connection) -> dict:
    """Reject rates per source and firmware, and reasons that only one firmware of a source produces."""
    accepted = {(row["source"], row["firmware"]): row["lines"] for row in records(connection, """
      SELECT source, coalesce(firmware, 'unknown') AS firmware, count(*) AS lines
      FROM parsed_events GROUP BY ALL
    """)}
    versions = {key: {"source": key[0], "firmware": key[1], "accepted": lines, "rejected": 0, "reasons": {}}
                for key, lines in accepted.items()}
    for row in records(connection, """
      SELECT source, coalesce(firmware, 'unknown') AS firmware, reason, count(*) AS lines
      FROM unparsed GROUP BY ALL ORDER BY ALL
    """):
        key = (row["source"], row["firmware"])
        entry = versions.setdefault(key, {"source": key[0], "firmware": key[1], "accepted": 0,
                                          "rejected": 0, "reasons": {}})
        entry["rejected"] += row["lines"]
        entry["reasons"][row["reason"]] = row["lines"]
    for entry in versions.values():
        entry["reject_rate"] = entry["rejected"] / (entry["accepted"] + entry["rejected"])
    # A reason produced by exactly one firmware of a source that has several is the signature of a
    # release that changed the format. A heuristic, so it is reported for review and never blocks.
    known = [entry for entry in versions.values() if entry["firmware"] != "unknown"]
    suspects = []
    for entry in known:
        siblings = [other for other in known if other["source"] == entry["source"] and other is not entry]
        if siblings:
            suspects += [{"source": entry["source"], "firmware": entry["firmware"], "reason": reason, "lines": lines}
                         for reason, lines in entry["reasons"].items()
                         if all(reason not in other["reasons"] for other in siblings)]
    return {"by_version": sorted(versions.values(), key=lambda entry: (entry["source"], entry["firmware"])),
            "format_change_suspects": sorted(suspects, key=lambda row: (row["source"], row["firmware"], row["reason"]))}


def quality_report(connection, data_dir: Path) -> dict:
    counts = {}
    for table in ("parsed_events", "clean_events", "unparsed", "fact_print_job", "agg_status_hourly", "dim_printer"):
        counts[table] = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    sizes = {layer: sum(path.stat().st_size for path in (data_dir / layer).rglob("*") if path.is_file())
             for layer in ("landing", "parsed", "clean", "modeled", "unparsed")}
    return {
        "rows": counts, "bytes": sizes,
        "duplicates_removed": counts["parsed_events"] - counts["clean_events"],
        "coverage_by_source": records(connection, """
          SELECT source, sum(lines)::BIGINT AS lines, sum(parsed)::BIGINT AS parsed,
                 sum(parsed)::DOUBLE / nullif(sum(lines), 0) AS coverage
          FROM manifest GROUP BY source ORDER BY source
        """),
        "unparsed_reasons": records(connection, "SELECT source, reason, count(*) AS lines FROM unparsed GROUP BY source, reason ORDER BY source, reason"),
        "rejects_by_firmware": rejects_by_firmware(connection),
        "lateness_seconds": records(connection, """
          SELECT source, quantile_cont(epoch(received_at - event_ts), .5) AS p50,
                 quantile_cont(epoch(received_at - event_ts), .95) AS p95,
                 quantile_cont(epoch(received_at - event_ts), .99) AS p99,
                 count(*) FILTER (WHERE received_at - event_ts > INTERVAL '1 day') AS over_one_day
          FROM clean_events GROUP BY source ORDER BY source
        """),
        "timestamp_quality": records(connection, "SELECT timestamp_quality, count(*) AS events FROM clean_events GROUP BY timestamp_quality ORDER BY timestamp_quality"),
        "outcomes": records(connection, "SELECT outcome, count(*) AS jobs FROM fact_print_job GROUP BY outcome ORDER BY outcome"),
        "failure_rates": records(connection, "SELECT * FROM failure_rates ORDER BY model, firmware, material"),
    }


def run_pipeline(data_dir: Path = Path("data"), parser_version: int = 1, reprocess: bool = False,
                 batch_files: int = 500, threads: int = 4, memory_limit: str = "4GB") -> dict:
    if parser_version not in (1, 2) or batch_files < 1 or threads < 1:
        raise ValueError("parser_version must be 1 or 2; batch_files and threads must be positive")
    data_dir = Path(data_dir).resolve()
    started = time.perf_counter()
    connection = connect(data_dir, threads, memory_limit)
    try:
        initialize(connection)
        tracked = {row["path"]: row for row in records(connection, "SELECT * FROM manifest")}
        existing_versions = {row["parser_version"] for row in tracked.values()}
        if existing_versions and existing_versions != {parser_version} and not reprocess:
            raise ValueError("Parser version changed; use --reprocess to avoid mixed parser versions")
        files = sorted(path for path in (data_dir / "landing").glob("received_date=*/source=*/*")
                       if path.is_file() and path.suffix in {".txt", ".jsonl"})
        if not files:
            raise ValueError("No landing files; run the simulator or collector first")
        missing = set(tracked) - {str(path) for path in files}
        if missing:
            raise ValueError("Previously ingested raw files are missing; restore them before running (raw retention is required)")
        dirty = connection.execute("SELECT value FROM pipeline_meta WHERE key = 'exports_dirty'").fetchone()[0] == "true"
        candidates: list[tuple[str, int, int]] = []
        unchanged_metadata: list[tuple[int, int, str]] = []
        for path in files:
            stat = path.stat()
            prior = tracked.get(str(path))
            if not reprocess and prior and prior["size"] == stat.st_size and prior["mtime_ns"] == stat.st_mtime_ns:
                continue
            if not reprocess and prior and prior["sha256"] == sha256_file(path):
                unchanged_metadata.append((stat.st_size, stat.st_mtime_ns, str(path)))
                continue
            candidates.append((str(path), stat.st_size, stat.st_mtime_ns))
        changed_paths = [path for path, _, _ in candidates if path in tracked]
        run_id = uuid.uuid4().hex
        full_export = dirty or reprocess
        parsed_dates: set[date] = set()
        clean_dates: set[date] = set()
        # Parsing is staged in separate commits. Replacing many temporary tables inside one
        # giant transaction retains their undo/catalog state and exhausted 4GB at 1,000 printers.
        # Staging is disposable: neither the manifest nor published tables advance until all
        # batches have parsed successfully and the final transaction commits.
        connection.execute("DROP TABLE IF EXISTS staging_events; DROP TABLE IF EXISTS staging_unparsed; DROP TABLE IF EXISTS staging_manifest;")
        connection.execute("CREATE OR REPLACE TEMP TABLE new_lineage (path VARCHAR)")
        connection.execute("CREATE OR REPLACE TEMP TABLE settings AS SELECT ?::INTEGER AS parser_version", [parser_version])
        if candidates:
            connection.execute("CREATE TABLE staging_events AS SELECT * FROM parsed_events WHERE false")
            connection.execute("CREATE TABLE staging_unparsed AS SELECT * FROM unparsed WHERE false")
            connection.execute("CREATE TABLE staging_manifest AS SELECT * FROM manifest WHERE false")
        for offset in range(0, len(candidates), batch_files):
            chunk = candidates[offset:offset + batch_files]
            connection.execute("""
              CREATE OR REPLACE TEMP TABLE uploads AS
              SELECT filename, content,
                     regexp_extract(filename, 'source=([^/]+)', 1) AS source,
                     try(to_timestamp(try_cast(regexp_extract(filename, '__([0-9]+)', 1) AS DOUBLE))) AS received_at,
                     split_part(content, chr(10), 1) AS header
              FROM read_text(?)
            """, [[item[0] for item in chunk]])
            connection.execute((SQL_DIR / "parse.sql").read_text())
            parsed_dates.update(row[0] for row in connection.execute("SELECT DISTINCT event_date FROM batch_events").fetchall())
            # Hash the snapshot actually decoded, not a separate file read. Later appends
            # change its size/mtime and cause replacement on the next run.
            metadata = records(connection, """
              SELECT filename AS path, octet_length(encode(content)) AS size, sha256(content) AS sha256, source,
                     (SELECT count(*) FROM extracted x WHERE x.path = u.filename) AS lines,
                     (SELECT count(*) FROM batch_events b WHERE b.path = u.filename) AS parsed
              FROM uploads u
            """)
            mtimes = {item[0]: item[2] for item in chunk}
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute("INSERT INTO staging_events SELECT * FROM batch_events")
                connection.execute("INSERT INTO staging_unparsed SELECT * FROM batch_unparsed")
                connection.execute("INSERT INTO new_lineage SELECT filename FROM uploads")
                connection.executemany("INSERT INTO staging_manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
                    (row["path"], row["size"], mtimes[row["path"]], row["sha256"], parser_version,
                     row["source"], row["lines"], row["parsed"]) for row in metadata
                ])
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            if len(candidates) > batch_files:
                print(f"parsed {min(offset + batch_files, len(candidates))}/{len(candidates)} files", flush=True)
        if candidates:
            connection.execute("DROP TABLE uploads; DROP TABLE decoded_lines; DROP TABLE extracted; DROP TABLE batch_events; DROP TABLE batch_unparsed;")
        connection.execute("BEGIN TRANSACTION")
        try:
            if reprocess:
                connection.execute("DELETE FROM manifest; DELETE FROM parsed_events; DELETE FROM clean_events; DELETE FROM unparsed;")
            if unchanged_metadata:
                connection.executemany("UPDATE manifest SET size = ?, mtime_ns = ? WHERE path = ?", unchanged_metadata)
            connection.execute("CREATE OR REPLACE TEMP TABLE affected_keys (dedupe_key VARCHAR)")
            if changed_paths and not reprocess:
                parsed_dates.update(row[0] for row in connection.execute(
                    "SELECT DISTINCT event_date FROM parsed_events WHERE path IN (SELECT unnest(?))", [changed_paths]).fetchall())
                connection.execute("INSERT INTO affected_keys SELECT DISTINCT dedupe_key FROM parsed_events WHERE path IN (SELECT unnest(?))", [changed_paths])
                connection.execute("DELETE FROM parsed_events WHERE path IN (SELECT unnest(?))", [changed_paths])
                connection.execute("DELETE FROM unparsed WHERE path IN (SELECT unnest(?))", [changed_paths])
                connection.execute("DELETE FROM manifest WHERE path IN (SELECT unnest(?))", [changed_paths])
            if candidates:
                connection.execute("INSERT INTO parsed_events SELECT * FROM staging_events")
                connection.execute("INSERT INTO unparsed SELECT * FROM staging_unparsed")
                connection.execute("INSERT INTO manifest SELECT * FROM staging_manifest")
                if reprocess or not tracked:
                    candidates_sql = "SELECT * FROM parsed_events"
                else:
                    connection.execute("INSERT INTO affected_keys SELECT DISTINCT dedupe_key FROM staging_events")
                    connection.execute("CREATE OR REPLACE TEMP TABLE distinct_keys AS SELECT DISTINCT dedupe_key FROM affected_keys")
                    clean_dates.update(row[0] for row in connection.execute("SELECT DISTINCT event_date FROM clean_events WHERE dedupe_key IN (SELECT dedupe_key FROM distinct_keys)").fetchall())
                    connection.execute("DELETE FROM clean_events WHERE dedupe_key IN (SELECT dedupe_key FROM distinct_keys)")
                    candidates_sql = "SELECT p.* FROM parsed_events p JOIN distinct_keys k USING (dedupe_key)"
                connection.execute(f"""
                  CREATE OR REPLACE TEMP VIEW canonical_events AS {candidates_sql}
                  QUALIFY row_number() OVER (PARTITION BY dedupe_key ORDER BY received_at, path, line_number) = 1
                """)
                connection.execute(f"CREATE OR REPLACE TEMP TABLE replacement_clean AS {CLEAN_SELECT}")
                clean_dates.update(row[0] for row in connection.execute("SELECT DISTINCT event_date FROM replacement_clean").fetchall())
                connection.execute("INSERT INTO clean_events SELECT * FROM replacement_clean")
                connection.execute((SQL_DIR / "models.sql").read_text())
                connection.execute("UPDATE pipeline_meta SET value = 'true' WHERE key = 'exports_dirty'")
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        # From here on, a failure must leave exports_dirty set so a retry reconciles all outputs.
        connection.execute("DROP TABLE IF EXISTS staging_events; DROP TABLE IF EXISTS staging_unparsed; DROP TABLE IF EXISTS staging_manifest;")
        if candidates or dirty:
            append_run = run_id if not full_export and not changed_paths else None
            export_partitions(connection, data_dir, "parsed_events", parsed_dates, full_export,
                              append_run=append_run,
                              where="path IN (SELECT path FROM new_lineage)" if append_run else "true")
            export_partitions(connection, data_dir, "clean_events", clean_dates, full_export)
            export_models(connection, data_dir)
        report = quality_report(connection, data_dir)
        report["run"] = {"files_processed": len(candidates), "files_changed": len(changed_paths),
                         "files_total": len(files), "parser_version": parser_version,
                         "reprocess": reprocess, "recovered_exports": dirty,
                         "clean_partitions_touched": len(clean_dates),
                         "no_op": not candidates and not dirty,
                         "seconds": round(time.perf_counter() - started, 3)}
        write_json(data_dir / "quality.json", report)
        connection.execute("UPDATE pipeline_meta SET value = 'false' WHERE key = 'exports_dirty'")
        return report
    finally:
        connection.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--parser-version", type=int, choices=(1, 2), default=1)
    parser.add_argument("--reprocess", action="store_true")
    parser.add_argument("--batch-files", type=int, default=500)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="4GB")
    args = parser.parse_args(argv)
    try:
        report = run_pipeline(args.data_dir, args.parser_version, args.reprocess, args.batch_files, args.threads, args.memory_limit)
    except (ValueError, duckdb.Error, OSError) as exc:
        parser.exit(1, f"Pipeline failed: {exc}\n")
    print(json.dumps({"run": report["run"], "rows": report["rows"], "coverage": report["coverage_by_source"],
                      "duplicates_removed": report["duplicates_removed"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
