from datetime import datetime, timezone

import duckdb
import pytest

from telemetry.pipeline import run_pipeline
from telemetry.semantic import compile_query, load
from telemetry.simulate_fleet import simulate

UTC = timezone.utc
ALL_TIME = (datetime(2000, 1, 1, tzinfo=UTC), datetime(2100, 1, 1, tzinfo=UTC))


@pytest.fixture(scope="module")
def fleet(tmp_path_factory):
    root = tmp_path_factory.mktemp("semantic")
    simulate(root, printers=100, days=2)
    run_pipeline(root, parser_version=2)
    with duckdb.connect(str(root / "state.duckdb"), read_only=True) as connection:
        connection.execute("SET TimeZone = 'UTC'")
        yield connection


def run(connection, model, metrics, group_by=(), where=None):
    sql, parameters = compile_query(model, metrics, list(group_by), where, *ALL_TIME)
    return connection.execute(sql, parameters).fetchall()


def test_definitions_match_the_built_table_and_are_complete(fleet):
    model = load()
    columns = {row[0] for row in fleet.execute(f"DESCRIBE {model['entity']['table']}").fetchall()}
    assert {dimension["column"] for dimension in model["dimensions"].values()} <= columns
    assert set(model["entity"]["key"]) <= columns
    outcomes = {row[0] for row in fleet.execute("SELECT DISTINCT outcome FROM fact_print_job").fetchall()}
    assert outcomes <= set(model["dimensions"]["outcome"]["values"])
    for name, metric in model["metrics"].items():
        assert metric["description"], name
        assert set(metric.get("ratio", [])) <= set(model["metrics"]), name
        if metric.get("certified"):
            assert metric["owner"] and metric["examples"] and metric["always_show_with"], name


def test_example_questions_match_the_pipeline_view(fleet):
    model = load()
    for example in model["metrics"]["failure_rate"]["examples"]:
        answer = run(fleet, model, ["labelled_prints", "failed_prints", "failure_rate"],
                     example["group_by"], example["where"])
        assert answer, example["question"]
        expected = fleet.execute(
            "SELECT firmware, labelled_jobs, failures, failure_rate FROM failure_rates "
            "WHERE model = ? AND material = ? ORDER BY firmware",
            [example["where"]["model"], example["where"]["material"]]).fetchall()
        assert [row[:3] for row in answer] == [row[:3] for row in expected]
        assert [row[3] for row in answer] == pytest.approx([row[3] for row in expected])


def test_rates_are_recomputed_from_counts_for_any_slice(fleet):
    model = load()
    answer = run(fleet, model, ["failure_rate"], ["firmware"])
    recomputed = fleet.execute(
        "SELECT firmware, sum(failures)::DOUBLE / sum(labelled_jobs) FROM failure_rates "
        "GROUP BY firmware ORDER BY firmware").fetchall()
    averaged = fleet.execute(
        "SELECT firmware, avg(failure_rate) FROM failure_rates GROUP BY firmware ORDER BY firmware").fetchall()
    assert [row[1] for row in answer] == pytest.approx([row[1] for row in recomputed])
    assert [row[1] for row in answer] != pytest.approx([row[1] for row in averaged])


def test_undefined_fields_and_unbounded_queries_are_refused():
    model = load()
    with pytest.raises(ValueError, match="Not defined"):
        compile_query(model, ["failure_rate"], ["serial_number"], None, *ALL_TIME)
    with pytest.raises(ValueError, match="requires a time range"):
        compile_query(model, ["failure_rate"], ["firmware"])
