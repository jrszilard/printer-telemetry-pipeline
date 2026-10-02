import json
from pathlib import Path
import subprocess

import pytest

from scripts.check_publication import audit_index, content_findings

ROOT = Path(__file__).resolve().parents[1]
PRIVATE_PATHS = (
    "data/real/reading.jsonl", "custom/landing/reading.txt",
    "custom/received_date=2026-10-02/source=prusalink/upload.txt", "custom/event_date=2026-10-02/readings.txt",
    ".env", ".env.local", ".env.production", "settings/prusalink.env", "settings/prusalink.env.local",
    "settings/prusalink.env~", "credentials.json", "secrets/config.yaml", ".secrets.toml",
    "keys/id_ed25519", "keys/client.pem", "keys/client.key", "trace.log", "out/export.parquet",
    "out/reading.jsonl", "out/state.duckdb", "out/state.duckdb.wal", ".claude/settings.json",
    ".pi/session.md", "docs/walkthrough.md", "docs/private/meeting.md", "backup.zip", "private/config.txt",
    ".github/workflows/ci.yml",
)
PUBLIC_PATHS = (
    ".gitignore", "README.md", "AGENTS.md", "SECURITY.md", "requirements.txt", "pytest.ini",
    "scripts/check_publication.py", "tests/test_publication_safety.py", "telemetry/collect_prusa.py",
    "telemetry/sql/parse.sql", "docs/overview.md", "docs/results.md",
)


@pytest.mark.parametrize("path", PRIVATE_PATHS)
def test_private_files_are_ignored_even_outside_data(path):
    result = subprocess.run(["git", "check-ignore", "--no-index", "--quiet", "--", path], cwd=ROOT)
    assert result.returncode == 0, path


@pytest.mark.parametrize("path", PUBLIC_PATHS)
def test_public_source_and_docs_are_not_ignored(path):
    result = subprocess.run(["git", "check-ignore", "--no-index", "--quiet", "--", path], cwd=ROOT)
    assert result.returncode == 1, path


@pytest.mark.parametrize("label,value", [
    ("GitHub token", "gh" + "p_" + "A" * 36),
    ("AWS access key", "AK" + "IA" + "A" * 16),
    ("private key", "-----BEGIN " + "RSA PRIVATE KEY-----"),
    ("provider API key", "sk-" + "ant-" + "A" * 40),
])
def test_recognized_secret_shapes_are_flagged_without_echoing_values(label, value):
    findings = content_findings(value)
    assert label in findings
    assert value not in " ".join(findings)


def test_serial_literals_must_be_explicitly_synthetic():
    key = "ser" + "ial"
    assert content_findings(json.dumps({key: "demo-not-marked"})) == ["non-synthetic serial literal"]
    assert content_findings(json.dumps({key: "SYNTHETIC-EXAMPLE"})) == []


def test_known_private_values_are_redacted_in_findings():
    value = "synthetic-sensitive-marker"
    findings = content_findings("before " + value + " after", [("known local value", value)])
    assert findings == ["known local value"]
    assert value not in " ".join(findings)


@pytest.fixture
def index_sandbox(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q", "-b", "main", str(tmp_path)], check=True)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".gitignore").write_text((ROOT / ".gitignore").read_text())
    (tmp_path / "README.md").write_text("Synthetic publication test.\n")
    subprocess.run(["git", "add", ".gitignore", "README.md"], check=True)
    return tmp_path


def test_force_added_ignored_file_is_rejected(index_sandbox):
    folder = index_sandbox / "docs/private"
    folder.mkdir(parents=True)
    (folder / "notes.md").write_text("Synthetic private fixture.\n")
    subprocess.run(["git", "add", "-f", "docs/private/notes.md"], check=True)
    _, problems = audit_index()
    assert any("force-added" in problem for problem in problems)


def test_audit_reads_index_not_replaced_working_copy(index_sandbox):
    token = "gh" + "p_" + "A" * 36
    (index_sandbox / "README.md").write_text(token + "\n")
    subprocess.run(["git", "add", "README.md"], check=True)
    (index_sandbox / "README.md").write_text("Safe working copy does not repair staged content.\n")
    _, problems = audit_index()
    assert any("GitHub token" in problem for problem in problems)
    assert token not in " ".join(problems)
