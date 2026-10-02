"""Poll PrusaLink using GET only. No upload/control code exists in this collector."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPDigestAuthHandler, HTTPPasswordMgrWithDefaultRealm, Request, build_opener

UTC = timezone.utc
GET_PATHS = frozenset({"/api/version", "/api/v1/info", "/api/v1/status", "/api/v1/job"})
IDENTITY_KEYS = frozenset({"serial", "serial_number", "sn", "api_key", "password", "username"})


class CollectionError(RuntimeError):
    """An intentionally credential-free error suitable for console output."""


def load_config(path: Path) -> dict[str, str]:
    """Read simple KEY=value config without executing shell code or logging values."""
    config: dict[str, str] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            key, sep, value = line.partition("=")
            if not sep:
                raise CollectionError("Invalid configuration line (expected KEY=value)")
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            config[key.strip()] = value
    for key in ("PRUSALINK", "PRUSALINK_USER", "PRUSALINK_PASSWORD"):
        if key in os.environ:
            config[key] = os.environ[key]
        if not config.get(key):
            raise CollectionError(f"Missing {key}; set it in the environment or the config file")
    return config


def redact_identity(value):
    """Responses are otherwise preserved; never persist serials or credential fields."""
    if isinstance(value, dict):
        return {key: redact_identity(item) for key, item in value.items() if key.lower() not in IDENTITY_KEYS}
    if isinstance(value, list):
        return [redact_identity(item) for item in value]
    return value


class PrusaClient:
    def __init__(self, config: dict[str, str], timeout: float = 10):
        host = config["PRUSALINK"].rstrip("/")
        if "://" not in host:
            host = "http://" + host
        parts = urlsplit(host)
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
            raise CollectionError("PRUSALINK must be an HTTP(S) host without embedded credentials")
        if parts.path not in {"", "/"} or parts.query or parts.fragment:
            raise CollectionError("PRUSALINK must be a host, not an API path")
        self.host = host
        self.timeout = timeout
        passwords = HTTPPasswordMgrWithDefaultRealm()
        passwords.add_password(None, host, config["PRUSALINK_USER"], config["PRUSALINK_PASSWORD"])
        # Do not follow redirects: credentials and reads stay on the configured printer.
        from urllib.request import HTTPRedirectHandler

        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        self.opener = build_opener(HTTPDigestAuthHandler(passwords), NoRedirect())

    def get(self, path: str) -> tuple[int, dict | None]:
        if path not in GET_PATHS:
            raise CollectionError("Endpoint is not on the read-only allowlist")
        request = Request(self.host + path, headers={"Accept": "application/json"}, method="GET")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                if response.status == 204:
                    return 204, None
                result = json.loads(response.read())
                if not isinstance(result, dict):
                    raise CollectionError("Expected a JSON object from PrusaLink")
                return response.status, result
        except HTTPError as exc:
            if exc.code == 204:
                return 204, None
            raise CollectionError(f"PrusaLink GET failed (HTTP {exc.code})") from None
        except (URLError, TimeoutError, OSError):
            raise CollectionError("PrusaLink GET failed (connection/timeout)") from None
        except (ValueError, UnicodeError):
            raise CollectionError("PrusaLink GET returned invalid JSON") from None


def identify(client: PrusaClient) -> dict:
    _, version = client.get("/api/version")
    _, info = client.get("/api/v1/info")
    serial = (info or {}).get("serial")
    if not isinstance(serial, str) or not serial.strip():
        raise CollectionError("Printer did not provide a serial for device hashing")
    return {
        "id": "prusa-" + hashlib.sha256(serial.encode()).hexdigest()[:24],
        "model": "Prusa Core One",
        "fw": (version or {}).get("firmware", "unknown"),
        "api_version": (version or {}).get("api"),
        "prusalink_version": (version or {}).get("server"),
        "nozzle_diameter": (info or {}).get("nozzle_diameter"),
    }


def collect_once(client: PrusaClient, device: dict, landing: Path, now=None) -> tuple[Path, dict]:
    started = (now or (lambda: datetime.now(UTC)))()
    status_code, status = client.get("/api/v1/status")
    job_code, job = client.get("/api/v1/job")
    received = (now or (lambda: datetime.now(UTC)))()
    if status_code != 200 or not status or not isinstance(status.get("printer"), dict):
        raise CollectionError("PrusaLink returned no printer status")
    record = {
        "source": "prusalink",
        "received_at": received.isoformat(),
        "request_started_at": started.isoformat(),
        "timestamp_quality": "received_only",
        "device": device,
        "status": redact_identity(status),
        "job_http_status": job_code,
        "job": redact_identity(job),
    }
    folder = landing / f"received_date={received.date()}" / "source=prusalink"
    folder.mkdir(parents=True, exist_ok=True)
    hour = received.replace(minute=0, second=0, microsecond=0).timestamp()
    path = folder / f"{device['id']}__{int(hour)}.jsonl"
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, separators=(",", ":")) + "\n")
    return path, record


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path.home() / ".config/lakeshore/prusalink.env")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--interval", type=float, default=5, help="Seconds between polls (minimum 1)")
    parser.add_argument("--samples", type=int, default=0, help="Stop after N successful samples; 0 = until Ctrl-C")
    parser.add_argument("--once", action="store_true", help="Capture just one sample")
    args = parser.parse_args(argv)
    if args.interval < 1 or args.samples < 0:
        parser.error("interval must be >= 1 and samples >= 0")
    try:
        client = PrusaClient(load_config(args.config))
        device = identify(client)
        count = failures = 0
        limit = 1 if args.once else args.samples
        while not limit or count < limit:
            cycle_start = time.monotonic()
            try:
                path, record = collect_once(client, device, args.data_dir / "landing")
                count += 1
                failures = 0
                state = record["status"]["printer"].get("state", "unknown")
                print(f"sample={count} device={device['id']} state={state} path={path}", flush=True)
            except CollectionError as exc:
                failures += 1
                print(f"Collection error: {exc} (attempt {failures}/5)", flush=True)
                if failures >= 5 or args.once:
                    return 1
            if not limit or count < limit:
                delay = max(args.interval, min(60, 2 ** failures)) if failures else args.interval
                time.sleep(max(0, delay - (time.monotonic() - cycle_start)))
    except CollectionError as exc:
        print(f"Collection error: {exc}")
        return 1
    except KeyboardInterrupt:
        print("Collector stopped; existing readings preserved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
