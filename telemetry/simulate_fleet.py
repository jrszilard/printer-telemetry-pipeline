"""Deterministic mixed-generation fleet, with raw logs and separate ground truth."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import random
import time

import duckdb

UTC = timezone.utc
FIRMWARE = {"genA": ("1.4.2", "1.5.0"), "genB": ("2.0.3", "2.2.0"), "genC": ("3.0.1", "3.1.0")}


def iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def clock(value: datetime, offset: int) -> str:
    return (value + timedelta(seconds=offset)).strftime("%Y-%m-%d %H:%M:%S")


def format_event(event: dict, device: dict) -> str:
    source, firmware = device["source"], device["fw"]
    kind = event["kind"]
    job = event.get("job_id")
    if source == "genA":
        prefix = clock(event["ts"], device["clock_offset_seconds"])
        if kind == "status":
            label = "THERM" if event["state"] == "PRINTING" else "IDLE"
            return (
                f"{prefix} {label} bed={event['bed']:.1f} nozzle={event['nozzle']:.1f} "
                f"tgt_bed={event['target_bed']} tgt_nozzle={event['target_nozzle']} "
                f"fan=100 job={job or '-'}"
            )
        if kind == "job_start":
            return f'{prefix} JOB start id={job} material="{event["material"]}" layers={event["layers"]}'
        if kind == "job_end":
            result = {"success": "ok", "failure": "fail", "unknown": "unknown"}[event["outcome"]]
            return f"{prefix} JOB end id={job} result={result}"
        if kind == "error":
            return f"{prefix} ERR code={event['error_code']} job={job}"
        return f"{prefix} DBG subsystem=network message=heartbeat"
    if source == "genB":
        result = {"t": int(event["ts"].timestamp() * 1000), "dev": device["id"], "model": "B", "fw": firmware, "k": kind}
        if job:
            result["job"] = job
        if kind == "status":
            bed_key, nozzle_key = ("bed_temp", "nozzle_temp") if firmware == "2.2.0" else ("temp_bed", "temp_nozzle")
            unit = device["temp_unit"]
            convert = (lambda value: value * 1.8 + 32) if unit == "F" else (lambda value: value)
            result.update({bed_key: round(convert(event["bed"]), 2), nozzle_key: round(convert(event["nozzle"]), 2),
                           "target_bed": convert(event["target_bed"]), "target_nozzle": convert(event["target_nozzle"]),
                           "temp_unit": unit, "state": event["state"]})
        elif kind == "job_start":
            result.update(material=event["material"], layers=event["layers"])
        elif kind == "job_end" and event["outcome"] != "unknown":
            result["ok"] = event["outcome"] == "success"
        elif kind == "error":
            result["code"] = event["error_code"]
    else:
        result = {"ts": event["ts"].astimezone(timezone(timedelta(hours=2))).isoformat(), "device": {
            "id": device["id"], "model": "C", "fw": firmware}, "type": kind}
        if job:
            result["job"] = {"id": job}
        if kind == "status":
            result.update(temps={"bed_c": event["bed"], "nozzle_c": event["nozzle"], "chamber_c": event["chamber"]},
                          targets={"bed_c": event["target_bed"], "nozzle_c": event["target_nozzle"]}, state=event["state"])
        elif kind == "job_start":
            result["job"].update(material=event["material"], layers=event["layers"])
        elif kind == "job_end":
            result["outcome"] = event["outcome"]
        elif kind == "error":
            result["error"] = {"code": event["error_code"]}
        elif kind == "maintenance":
            result["action"] = "filter_check"
    return json.dumps(result, separators=(",", ":"))


def day_events(device: dict, day: date, rng: random.Random) -> tuple[list[dict], list[dict]]:
    midnight = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    events: list[dict] = []
    jobs: list[dict] = []
    intervals: list[tuple[datetime, datetime]] = []
    for slot, hour in enumerate((8, 14)):
        job_id = f"{device['id']}-{day:%Y%m%d}-{slot}"
        start = midnight + timedelta(hours=hour, minutes=rng.randint(0, 29))
        minutes = rng.randint(100, 140)
        end = start + timedelta(minutes=minutes)
        material = rng.choice(("PLA", "PETG", "ABS"))
        failure_probability = .22 if device["fw"] == "3.1.0" and material == "PETG" else .05
        failed = rng.random() < failure_probability
        label = "failure" if failed else "success"
        if rng.random() < .04:
            label = "unknown"
        missing_end = rng.random() < .03
        layers = rng.randint(100, 450)
        intervals.append((start, end))
        jobs.append({"device_id": device["id"], "model": device["source"][-1], "firmware": device["fw"],
                     "job_id": job_id, "material": material, "start_ts": iso(start), "end_ts": iso(end),
                     "outcome": "no_end_event" if missing_end else label, "physical_failure": failed,
                     "failure_probability": failure_probability, "duration_seconds": minutes * 60})
        events.append({"ts": start, "kind": "job_start", "job_id": job_id, "material": material, "layers": layers})
        for minute in range(minutes):
            bed = {"PLA": 60, "PETG": 80, "ABS": 100}[material]
            nozzle = {"PLA": 215, "PETG": 240, "ABS": 255}[material]
            events.append({"ts": start + timedelta(minutes=minute), "kind": "status", "state": "PRINTING",
                           "job_id": job_id, "bed": round(bed + rng.gauss(0, .5), 1),
                           "nozzle": round(nozzle + rng.gauss(0, 1), 1), "chamber": round(35 + rng.gauss(0, .7), 1),
                           "target_bed": bed, "target_nozzle": nozzle})
        if failed:
            events.append({"ts": end - timedelta(minutes=2), "kind": "error", "job_id": job_id,
                           "error_code": rng.choice(("E_TEMP", "E_MOTION", "E_FILAMENT"))})
        if not missing_end:
            events.append({"ts": end, "kind": "job_end", "job_id": job_id, "outcome": label})
    for minute in range(0, 24 * 60, 15):
        ts = midnight + timedelta(minutes=minute)
        if not any(start <= ts <= end for start, end in intervals):
            events.append({"ts": ts, "kind": "status", "state": "IDLE", "job_id": None,
                           "bed": round(24 + rng.random(), 1), "nozzle": round(25 + rng.random(), 1),
                           "chamber": 24., "target_bed": 0, "target_nozzle": 0})
    if device["source"] == "genA":
        events.append({"ts": midnight + timedelta(hours=6), "kind": "debug"})
    if device["source"] == "genC" and device["fw"] == "3.1.0":
        events.append({"ts": midnight + timedelta(hours=6), "kind": "maintenance"})
    return sorted(events, key=lambda event: (event["ts"], event["kind"])), jobs


def simulate(data_dir: Path, printers: int = 100, days: int = 3, seed: int = 42,
             start_date: date = date(2026, 9, 28)) -> dict:
    if printers < 1 or days < 1:
        raise ValueError("printers and days must be positive")
    landing = data_dir / "landing"
    if (data_dir / "truth").exists() or any(landing.glob("received_date=*/source=gen*/*")):
        raise ValueError("Synthetic data already exists; use a new --data-dir (nothing was overwritten)")
    started = time.perf_counter()
    truth = data_dir / "truth"
    truth.mkdir(parents=True)
    counters = {"printers": printers, "days": days, "seed": seed, "start_date": str(start_date),
                "files": 0, "event_lines": 0, "truncated_lines": 0, "retransmitted_files": 0,
                "clock_wrong_devices": 0, "offline_devices": 0, "jobs": 0}
    with (truth / "jobs.jsonl").open("w") as truth_jobs, (truth / "devices.jsonl").open("w") as truth_devices:
        for number in range(printers):
            rng = random.Random(seed + number * 104729)
            source = ("genA", "genB", "genC")[number % 3]
            wrong_clock = source == "genA" and rng.random() < .10
            clock_offset = -4 * 3600 + (rng.choice((-1, 1)) * rng.randint(2, 7) * 3600 if wrong_clock else 0)
            device = {"id": f"sim-{number:06d}", "source": source, "fw": rng.choice(FIRMWARE[source]),
                      "clock_offset_seconds": clock_offset, "wrong_clock": wrong_clock,
                      "offline": rng.random() < .08, "temp_unit": "F" if source == "genB" and rng.random() < .12 else "C"}
            counters["clock_wrong_devices"] += int(wrong_clock)
            counters["offline_devices"] += int(device["offline"])
            truth_devices.write(json.dumps(device) + "\n")
            for day_number in range(days):
                day = start_date + timedelta(days=day_number)
                events, jobs = day_events(device, day, rng)
                for job in jobs:
                    truth_jobs.write(json.dumps(job) + "\n")
                counters["jobs"] += len(jobs)
                noon = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=12)
                encoded = []
                for event in events:
                    line = format_event(event, device)
                    if event["kind"] == "status" and rng.random() < .002:
                        line = line[:45] if source == "genA" else line[:-15]
                        counters["truncated_lines"] += 1
                    encoded.append((event["ts"], line))
                for batch in range(2):
                    selected = [line for ts, line in encoded if (ts <= noon if batch == 0 else ts >= noon - timedelta(minutes=10))]
                    delay_days = rng.randint(1, 3) if device["offline"] and rng.random() < .75 else 0
                    send_at = noon + timedelta(hours=batch * 12, days=delay_days)
                    received_at = send_at + timedelta(seconds=rng.randint(2, 90))
                    header = ""
                    if source == "genA":
                        # Header is generated NOW, not when the buffered readings were recorded.
                        header = (f"#UPLOAD device={device['id']} model=A fw={device['fw']} "
                                  f"device_time={clock(send_at, clock_offset)}\n")
                    content = header + "\n".join(selected) + "\n"
                    extension = "txt" if source == "genA" else "jsonl"

                    def write_upload(arrival: datetime, suffix: str):
                        folder = landing / f"received_date={arrival.date()}" / f"source={source}"
                        folder.mkdir(parents=True, exist_ok=True)
                        path = folder / f"{device['id']}__{int(arrival.timestamp())}__{day_number:03d}-{batch}{suffix}.{extension}"
                        path.write_text(content)
                        counters["files"] += 1
                        counters["event_lines"] += len(selected)

                    write_upload(received_at, "")
                    if rng.random() < .02:
                        write_upload(received_at + timedelta(hours=1), "-retry")
                        counters["retransmitted_files"] += 1
            if printers >= 1000 and (number + 1) % 1000 == 0:
                print(f"simulated {number + 1}/{printers} printers", flush=True)
    connection = duckdb.connect()
    for table in ("jobs", "devices"):
        connection.execute("COPY (SELECT * FROM read_json_auto($input)) TO $output (FORMAT PARQUET, COMPRESSION ZSTD)",
                           {"input": str(truth / f"{table}.jsonl"), "output": str(truth / f"{table}.parquet")})
    connection.close()
    counters["raw_bytes"] = sum(path.stat().st_size for path in landing.glob("received_date=*/source=gen*/*"))
    counters["generation_seconds"] = round(time.perf_counter() - started, 3)
    (data_dir / "simulation.json").write_text(json.dumps(counters, indent=2) + "\n")
    return counters


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--printers", type=int, default=100)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 9, 28))
    args = parser.parse_args(argv)
    try:
        print(json.dumps(simulate(args.data_dir, args.printers, args.days, args.seed, args.start_date), indent=2))
    except ValueError as exc:
        parser.exit(1, f"{exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
