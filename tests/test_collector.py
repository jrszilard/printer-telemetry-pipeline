from datetime import datetime, timezone
import json
from urllib.error import URLError

import pytest

from telemetry.collect_prusa import (
    CollectionError, GET_PATHS, PrusaClient, collect_once, identify, load_config, redact_identity,
)


class FakeClient:
    def __init__(self):
        self.paths = []

    def get(self, path):
        self.paths.append(path)
        return {
            "/api/version": (200, {"firmware": "6.8.1", "server": "2.1.2", "api": "2.0"}),
            "/api/v1/info": (200, {"serial": "SYNTHETIC-SECRET-SERIAL", "nozzle_diameter": .4}),
            "/api/v1/status": (200, {"printer": {"state": "IDLE", "temp_bed": 24.1, "temp_nozzle": 25.2},
                                    "serial": "SYNTHETIC-SECRET-SERIAL"}),
            "/api/v1/job": (204, None),
        }[path]


def test_capture_hashes_serial_and_preserves_reading(tmp_path):
    client = FakeClient()
    device = identify(client)
    now = datetime(2026, 10, 2, 13, 5, tzinfo=timezone.utc)
    path, result = collect_once(client, device, tmp_path, now=lambda: now)
    assert client.paths == ["/api/version", "/api/v1/info", "/api/v1/status", "/api/v1/job"]
    assert set(client.paths) <= GET_PATHS
    assert device["id"].startswith("prusa-")
    assert identify(FakeClient())["id"] == device["id"]
    assert "SYNTHETIC-SECRET-SERIAL" not in path.read_text()
    assert result["timestamp_quality"] == "received_only"
    assert result["job"] is None and result["job_http_status"] == 204
    assert result["status"]["printer"]["temp_bed"] == 24.1
    assert path.parent.name == "source=prusalink"
    collect_once(client, device, tmp_path, now=lambda: now)
    assert len(path.read_text().splitlines()) == 2
    assert json.loads(path.read_text().splitlines()[0]) == result


def test_recursive_identity_redaction():
    assert redact_identity({"Serial": "SYNTHETIC-SERIAL", "nested": [{"password": "secret", "ok": 3}]}) == {"nested": [{"ok": 3}]}


def test_config_is_not_executed_and_environment_overrides(tmp_path, monkeypatch):
    for key in ("PRUSALINK", "PRUSALINK_USER", "PRUSALINK_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config.env"
    config.write_text('export PRUSALINK="printer.lan"\nPRUSALINK_USER=test\nPRUSALINK_PASSWORD=\'$(do-not-execute)\'\n')
    loaded = load_config(config)
    assert loaded["PRUSALINK_PASSWORD"] == "$(do-not-execute)"
    monkeypatch.setenv("PRUSALINK_USER", "override")
    assert load_config(config)["PRUSALINK_USER"] == "override"


@pytest.mark.parametrize("host", ["file:///etc/passwd", "http://user:secret@printer.lan", "http://printer.lan/api/v1/job"])
def test_rejects_unsafe_hosts(host):
    with pytest.raises(CollectionError):
        PrusaClient({"PRUSALINK": host, "PRUSALINK_USER": "u", "PRUSALINK_PASSWORD": "secret"})


def test_client_constructs_only_get_and_redacts_network_errors():
    client = PrusaClient({"PRUSALINK": "printer.lan", "PRUSALINK_USER": "u", "PRUSALINK_PASSWORD": "secret"})
    requests = []

    class Opener:
        def open(self, request, timeout):
            requests.append(request)
            raise URLError("password=secret")

    client.opener = Opener()
    with pytest.raises(CollectionError) as error:
        client.get("/api/v1/status")
    assert "secret" not in str(error.value)
    assert requests[0].get_method() == "GET"
    assert requests[0].data is None
    with pytest.raises(CollectionError, match="allowlist"):
        client.get("/api/v1/job/123")
    assert len(requests) == 1


def test_missing_serial_fails_closed():
    class Client:
        def get(self, path):
            return 200, {}
    with pytest.raises(CollectionError, match="serial"):
        identify(Client())
