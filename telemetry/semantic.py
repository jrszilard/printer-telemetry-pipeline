"""Turn semantic definitions (telemetry/definitions/*.toml) into SQL. Only defined fields can be queried."""

from __future__ import annotations

from pathlib import Path
import tomllib

DEFINITIONS_DIR = Path(__file__).with_name("definitions")


def load(name: str = "print_jobs") -> dict:
    with (DEFINITIONS_DIR / f"{name}.toml").open("rb") as stream:
        return tomllib.load(stream)


def metric_sql(model: dict, name: str) -> str:
    metric = model["metrics"][name]
    if "ratio" in metric:
        # Ratios are recomputed from their counts for every slice, never averaged.
        numerator, denominator = (metric_sql(model, part) for part in metric["ratio"])
        return f"CAST({numerator} AS DOUBLE) / NULLIF({denominator}, 0)"
    return metric["sql"]


def compile_query(model: dict, metrics: list[str], group_by: list[str] = (), where: dict | None = None,
                  start=None, end=None) -> tuple[str, list]:
    entity = model["entity"]
    if start is None or end is None:
        raise ValueError(f"{entity['name']} requires a time range: {entity['required_filter']}")
    undefined = [name for name in metrics if name not in model["metrics"]]
    undefined += [name for name in [*group_by, *(where or {})] if name not in model["dimensions"]]
    if undefined:
        raise ValueError(f"Not defined in the semantic model: {', '.join(undefined)}")
    columns = [model["dimensions"][name]["column"] for name in group_by]
    select = [f"{column} AS {name}" for column, name in zip(columns, group_by)]
    select += [f"{metric_sql(model, name)} AS {name}" for name in metrics]
    conditions = [f"{entity['time_sql']} >= ?", f"{entity['time_sql']} < ?"]
    parameters = [start, end]
    for name, value in (where or {}).items():
        conditions.append(f"{model['dimensions'][name]['column']} = ?")
        parameters.append(value)
    sql = f"SELECT {', '.join(select)} FROM {entity['table']} WHERE {' AND '.join(conditions)}"
    if columns:
        sql += f" GROUP BY {', '.join(columns)} ORDER BY {', '.join(columns)}"
    return sql, parameters
