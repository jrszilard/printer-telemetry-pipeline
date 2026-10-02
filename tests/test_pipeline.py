from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as parquet
import pytest

from telemetry.pipeline import run_pipeline
from telemetry.simulate_fleet import simulate

UTC = timezone.utc


def query(root, sql, parameters=None):
    with duckdb.connect(str(root / "state.duckdb"), read_only=True) as connection:
        connection.execute("SET TimeZone = 'UTC'")
        return connection.execute(sql, parameters or []).fetchall()


def upload(root, source, received, content, suffix=""):
    folder = root / "landing" / f"received_date={received.date()}" / f"source={source}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"test__{int(received.timestamp())}{suffix}.{'txt' if source == 'genA' else 'jsonl'}"
    path.write_text(content if isinstance(content, str) else "\n".join(json.dumps(row) for row in content) + "\n")
    return path


def b_event(ts, kind, **extra):
    return {"t": int(ts.timestamp() * 1000), "dev": "test-b", "model": "B", "fw": "2.2.0", "k": kind, **extra}


@pytest.fixture
def small_fleet(tmp_path):
    simulate(tmp_path, printers=30, days=2)
    run_pipeline(tmp_path, batch_files=20)
    return tmp_path


def test_clean_identity_job_count_clock_and_units_match_truth(small_fleet):
    root = small_fleet
    assert query(root, "SELECT count(*) - count(DISTINCT dedupe_key) FROM clean_events")[0][0] == 0
    assert query(root, "SELECT count(*) FROM fact_print_job")[0][0] == 120
    errors = query(root, """
      SELECT max(abs(epoch(f.start_ts - t.start_ts::TIMESTAMPTZ))),
             count(*) FILTER (WHERE f.outcome <> t.outcome)
      FROM fact_print_job f JOIN read_parquet(?) t USING(device_id, job_id)
      WHERE f.source = 'genA'
    """, [str(root / "truth/jobs.parquet")])[0]
    assert 0 <= errors[0] <= 90
    assert errors[1] == 0
    assert query(root, """
      SELECT max(abs(epoch(f.start_ts - t.start_ts::TIMESTAMPTZ)))
      FROM fact_print_job f JOIN read_parquet(?) t USING(device_id, job_id)
      WHERE f.source <> 'genA'
    """, [str(root / "truth/jobs.parquet")]) == [(0.,)]
    assert query(root, """
      SELECT count(*) FROM fact_print_job f JOIN read_parquet(?) t USING(device_id, job_id)
      WHERE f.outcome <> t.outcome
    """, [str(root / "truth/jobs.parquet")]) == [(0,)]
    assert query(root, "SELECT max(nozzle_c) FROM clean_events")[0][0] < 270
    assert query(root, "SELECT count(*) FROM fact_print_job WHERE duration_seconds < 0")[0][0] == 0
    assert query(root, "SELECT count(*) FROM unparsed WHERE reason = 'invalid_json'")[0][0] > 0
    schema = parquet.read_schema(next((root / "clean").rglob("*.parquet")))
    assert str(schema.field("event_ts").type) == "timestamp[us, tz=UTC]"
    assert str(schema.field("bed_c").type) == "double"


def test_second_run_is_noop_and_parquet_is_untouched(small_fleet):
    paths = list(small_fleet.glob("clean/**/*.parquet")) + list(small_fleet.glob("parsed/**/*.parquet")) + list(small_fleet.glob("modeled/*.parquet"))
    signature = {path: (path.stat().st_size, path.stat().st_mtime_ns) for path in paths}
    before = query(small_fleet, "SELECT count(*) FROM clean_events")
    result = run_pipeline(small_fleet)
    assert result["run"]["no_op"] and result["run"]["files_processed"] == 0
    assert query(small_fleet, "SELECT count(*) FROM clean_events") == before
    assert {path: (path.stat().st_size, path.stat().st_mtime_ns) for path in paths} == signature


def test_reprocess_parser_upgrade_adds_maintenance_not_jobs(small_fleet):
    before = json.loads((small_fleet / "quality.json").read_text())
    with pytest.raises(ValueError, match="reprocess"):
        run_pipeline(small_fleet, parser_version=2)
    after = run_pipeline(small_fleet, parser_version=2, reprocess=True)
    old_coverage = next(row["coverage"] for row in before["coverage_by_source"] if row["source"] == "genC")
    new_coverage = next(row["coverage"] for row in after["coverage_by_source"] if row["source"] == "genC")
    assert new_coverage > old_coverage
    assert after["rows"]["fact_print_job"] == before["rows"]["fact_print_job"]
    assert query(small_fleet, "SELECT count(*) FROM clean_events WHERE kind = 'maintenance'")[0][0] > 0
    assert query(small_fleet, "SELECT count(DISTINCT parser_version) FROM parsed_events")[0][0] == 1
    with duckdb.connect() as con:
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(small_fleet / "parsed/*/*.parquet")]).fetchone()[0] == after["rows"]["parsed_events"]


def test_late_job_end_updates_previous_day_and_outcome(tmp_path):
    start = datetime(2026, 9, 28, 20, tzinfo=UTC)
    end = start + timedelta(hours=2)
    upload(tmp_path, "genB", end, [b_event(start, "job_start", job="j1", material="PLA", layers=100),
                                  b_event(start, "status", job="j1", state="PRINTING", bed_temp=60, nozzle_temp=215)])
    run_pipeline(tmp_path)
    assert query(tmp_path, "SELECT outcome, duration_seconds FROM fact_print_job") == [("no_end_event", None)]
    upload(tmp_path, "genB", end + timedelta(days=3), [b_event(end, "job_end", job="j1", ok=True)])
    result = run_pipeline(tmp_path)
    assert result["run"]["files_processed"] == 1
    assert result["run"]["clean_partitions_touched"] == 1
    assert query(tmp_path, "SELECT outcome, duration_seconds FROM fact_print_job") == [("success", 7200.)]
    assert sorted(path.name for path in (tmp_path / "clean").iterdir()) == ["event_date=2026-09-28"]


def test_raw_timestamp_dedupe_moves_event_between_dates(tmp_path):
    content = ("#UPLOAD device=test-a model=A fw=1.4.2 device_time=2026-09-28 20:02:00\n"
               "2026-09-28 19:59:00 THERM bed=60 nozzle=215 tgt_bed=60 tgt_nozzle=215 fan=100 job=j1\n")
    original = datetime(2026, 9, 29, 0, 2, tzinfo=UTC)
    # A retransmission retains its OLD header. Ingest it first to exercise old partition invalidation.
    upload(tmp_path, "genA", original + timedelta(days=1), content)
    run_pipeline(tmp_path)
    assert (tmp_path / "clean/event_date=2026-09-29").exists()
    upload(tmp_path, "genA", original, content)
    report = run_pipeline(tmp_path)
    assert report["duplicates_removed"] == 1
    assert report["run"]["clean_partitions_touched"] == 2
    assert query(tmp_path, "SELECT event_ts::DATE FROM clean_events")[0][0].isoformat() == "2026-09-28"
    assert not (tmp_path / "clean/event_date=2026-09-29").exists()
    with duckdb.connect() as con:
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(tmp_path / "clean/*/*.parquet")]).fetchone()[0] == 1


def test_changed_upload_retracts_old_rows_and_replaces_parsed_exports(tmp_path):
    start = datetime(2026, 9, 28, 10, tzinfo=UTC)
    path = upload(tmp_path, "genB", start + timedelta(hours=1), [b_event(start, "job_start", job="j1", material="PETG"),
                  b_event(start + timedelta(hours=1), "job_end", job="j1", ok=True)])
    run_pipeline(tmp_path)
    path.write_text(path.read_text().replace('"ok": true', '"ok": false'))
    report = run_pipeline(tmp_path)
    assert report["run"]["files_changed"] == 1
    assert report["rows"]["parsed_events"] == 2
    assert query(tmp_path, "SELECT outcome FROM fact_print_job") == [("failure",)]
    with duckdb.connect() as con:
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(tmp_path / "parsed/*/*.parquet")]).fetchone()[0] == 2


def test_export_failure_is_repaired_on_next_run(tmp_path, monkeypatch):
    import telemetry.pipeline as pipeline
    simulate(tmp_path, printers=3, days=1)
    original = pipeline.export_models

    def fail(*args):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(pipeline, "export_models", fail)
    with pytest.raises(OSError, match="disk failure"):
        run_pipeline(tmp_path)
    assert query(tmp_path, "SELECT value FROM pipeline_meta WHERE key = 'exports_dirty'") == [("true",)]
    monkeypatch.setattr(pipeline, "export_models", original)
    report = run_pipeline(tmp_path)
    assert report["run"]["recovered_exports"] and not report["run"]["no_op"]
    assert report["run"]["files_processed"] == 0
    assert query(tmp_path, "SELECT value FROM pipeline_meta WHERE key = 'exports_dirty'") == [("false",)]
    assert (tmp_path / "modeled/fact_print_job.parquet").exists()
    with duckdb.connect() as con:
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(tmp_path / "parsed/*/*.parquet")]).fetchone()[0] == report["rows"]["parsed_events"]


def test_invalid_lines_are_quarantined_and_fahrenheit_is_explicit(tmp_path):
    ts = datetime(2026, 9, 28, 10, tzinfo=UTC)
    rows = [b_event(ts, "status", state="PRINTING", bed_temp=140, nozzle_temp=419, temp_unit="F"),
            b_event(ts + timedelta(seconds=1), "status", state="IDLE", bed_temp="NaN", nozzle_temp=25),
            b_event(ts + timedelta(seconds=2), "status", state="IDLE", bed_temp=25, nozzle_temp=25, temp_unit="K"),
            {"t": "wrong", "dev": "test-b", "k": "status"},
            {"t": int(ts.timestamp() * 1000), "dev": "test-b", "k": "job_end"}]
    content = "\n".join(json.dumps(row) for row in rows) + '\n{"truncated":\n'
    upload(tmp_path, "genB", ts + timedelta(hours=1), content)
    report = run_pipeline(tmp_path)
    assert report["rows"]["parsed_events"] == 1 and report["rows"]["unparsed"] == 5
    assert query(tmp_path, "SELECT bed_c, nozzle_c FROM clean_events") == [(60., 215.)]
    reasons = {row[0] for row in query(tmp_path, "SELECT reason FROM unparsed")}
    assert reasons == {"invalid_temperature", "unsupported_temperature_unit", "invalid_device_time", "missing_job_id", "invalid_json"}


def test_prusa_snapshot_does_not_fabricate_success(tmp_path):
    ts = datetime(2026, 10, 2, 10, tzinfo=UTC)
    record = {"received_at": ts.isoformat(), "device": {"id": "prusa-synthetic", "model": "Prusa Core One", "fw": "6.8.1"},
              "status": {"printer": {"state": "PRINTING", "temp_bed": 60, "temp_nozzle": 215}}, "job": {"id": 123}}
    upload(tmp_path, "prusalink", ts, [record])
    run_pipeline(tmp_path)
    assert query(tmp_path, "SELECT outcome, lifecycle_quality, duration_seconds FROM fact_print_job") == [("no_end_event", "observations_only", None)]
    assert query(tmp_path, "SELECT timestamp_quality FROM clean_events") == [("received_only",)]


@pytest.mark.parametrize("failure_phase", ["parse", "model"])
def test_failed_staging_or_final_transaction_does_not_advance_published_state(tmp_path, monkeypatch, failure_phase):
    import telemetry.pipeline as pipeline
    ts = datetime(2026, 9, 28, 10, tzinfo=UTC)
    upload(tmp_path, "genB", ts + timedelta(hours=1), [b_event(ts, "job_start", job="j1", material="PLA"),
           b_event(ts + timedelta(hours=1), "job_end", job="j1", ok=True)])
    run_pipeline(tmp_path)
    original_quality = (tmp_path / "quality.json").read_bytes()
    for days in (1, 2):
        upload(tmp_path, "genB", ts + timedelta(days=days), [b_event(ts + timedelta(days=days), "status",
               state="IDLE", bed_temp=25, nozzle_temp=26)])
    original_read = Path.read_text
    parse_calls = 0

    def broken_sql(path, *args, **kwargs):
        nonlocal parse_calls
        if path == pipeline.SQL_DIR / "parse.sql":
            parse_calls += 1
            if failure_phase == "parse" and parse_calls == 2:
                return "SELECT deliberately_missing_column;"
        if failure_phase == "model" and path == pipeline.SQL_DIR / "models.sql":
            return "SELECT deliberately_missing_column;"
        return original_read(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_text", broken_sql)
        with pytest.raises(duckdb.Error):
            run_pipeline(tmp_path, batch_files=1)
    assert query(tmp_path, "SELECT count(*) FROM manifest") == [(1,)]
    assert query(tmp_path, "SELECT count(*) FROM parsed_events") == [(2,)]
    assert query(tmp_path, "SELECT value FROM pipeline_meta WHERE key='exports_dirty'") == [("false",)]
    assert (tmp_path / "quality.json").read_bytes() == original_quality
    after = run_pipeline(tmp_path, batch_files=1)
    assert after["run"]["files_processed"] == 2 and after["rows"]["parsed_events"] == 4
    assert query(tmp_path, "SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 'staging_%'") == [(0,)]


def test_missing_raw_fails_instead_of_silently_erasing_history(tmp_path):
    simulate(tmp_path, printers=3, days=1)
    run_pipeline(tmp_path)
    next((tmp_path / "landing").rglob("*.txt")).unlink()
    with pytest.raises(ValueError, match="missing"):
        run_pipeline(tmp_path)


def test_simulation_is_deterministic_and_never_overwrites(tmp_path):
    first, second = tmp_path / "one", tmp_path / "two"
    simulate(first, printers=3, days=1, seed=7)
    simulate(second, printers=3, days=1, seed=7)
    for path in (first / "landing").rglob("*"):
        if path.is_file():
            assert path.read_bytes() == (second / "landing" / path.relative_to(first / "landing")).read_bytes()
    with pytest.raises(ValueError, match="nothing was overwritten"):
        simulate(first, printers=3, days=1)


def test_planted_failure_effect_is_visible_with_honest_denominator(tmp_path):
    simulate(tmp_path, printers=300, days=3)
    run_pipeline(tmp_path, parser_version=2)
    rows = query(tmp_path, "SELECT firmware, labelled_jobs, failure_rate FROM failure_rates WHERE model='C' AND material='PETG' ORDER BY firmware")
    before, after = rows
    assert before[0] == "3.0.1" and after[0] == "3.1.0"
    assert before[1] >= 50 and after[1] >= 50
    assert .13 < after[2] < .35
    assert after[2] > before[2] + .07
