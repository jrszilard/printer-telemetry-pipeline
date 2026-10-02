"""Audit the exact Git index without printing matching secret values.

This is a conservative publication guard, not a replacement for a full secret scanner.
Run as: python -m scripts.check_publication
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import urlsplit

ROOT_FILES = frozenset({".gitignore", "AGENTS.md", "README.md", "SECURITY.md", "pytest.ini", "requirements.txt", "LICENSE", "NOTICE"})
PUBLIC_FOLDERS = ("telemetry/", "tests/", "scripts/", "docs/", ".github/workflows/")
MAX_FILE_BYTES = 512 * 1024
TOKEN_RULES = {
    "private key": re.compile(r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----"),
    "GitHub token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "provider API key": re.compile(r"\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{32,}\b"),
}
SERIAL_LITERAL = re.compile(r'''["'](?:serial|serial_number|sn)["']\s*:\s*["']([^"'\r\n]+)["']''', re.IGNORECASE)


def public_path(path: str) -> bool:
    return path in ROOT_FILES or path.startswith(PUBLIC_FOLDERS)


def content_findings(text: str, private_values: list[tuple[str, str]] | None = None) -> list[str]:
    findings = [label for label, pattern in TOKEN_RULES.items() if pattern.search(text)]
    if any(not value.startswith("SYNTHETIC-") for value in SERIAL_LITERAL.findall(text)):
        findings.append("non-synthetic serial literal")
    for label, value in private_values or []:
        if value and value in text:
            findings.append(label)
    return sorted(set(findings))


def git(*args: str, input_data: bytes | None = None) -> bytes:
    result = subprocess.run(["git", *args], input=input_data, capture_output=True, check=True)
    return result.stdout


def load_private_values(config_path: Path | None, data_dir: Path | None) -> list[tuple[str, str]]:
    """Optional local-only audit inputs. Values never enter a report or Git object."""
    values: list[tuple[str, str]] = []
    if config_path:
        from telemetry.collect_prusa import load_config
        config = load_config(config_path.expanduser())
        password = config["PRUSALINK_PASSWORD"]
        if len(password) < 4:
            raise ValueError("Local password is too short for an unambiguous substring audit; review manually")
        values.append(("local printer password", password))
        url = config["PRUSALINK"]
        host = urlsplit(url if "://" in url else "http://" + url).hostname
        if host:
            values.append(("local printer hostname", host))
    if data_dir:
        for path in data_dir.glob("landing/received_date=*/source=prusalink/*.jsonl"):
            for line in path.read_text().splitlines():
                record = json.loads(line)
                device_id = record.get("device", {}).get("id")
                if device_id:
                    values.append(("real device identifier", str(device_id)))
                job = record.get("job") or {}
                for name in ("name", "display_name", "path"):
                    value = (job.get("file") or {}).get(name)
                    if isinstance(value, str) and len(value) >= 4:
                        values.append(("real job filename/path", value))
    return list(dict.fromkeys(values))


def audit_index(private_values: list[tuple[str, str]] | None = None) -> tuple[int, list[str]]:
    entries = git("ls-files", "--stage", "-z").split(b"\0")
    problems: list[str] = []
    count = 0
    for entry in filter(None, entries):
        header, encoded_path = entry.split(b"\t", 1)
        mode, object_id, stage = header.decode().split()
        path = encoded_path.decode()
        count += 1
        if stage != "0" or mode not in {"100644", "100755"}:
            problems.append(f"{path}: conflicts, symlinks, and submodules are not allowed in this release")
            continue
        if not public_path(path):
            problems.append(f"{path}: outside the reviewed publication folders")
            continue
        ignored = subprocess.run(["git", "check-ignore", "--no-index", "--quiet", "--", path], capture_output=True)
        if ignored.returncode == 0:
            problems.append(f"{path}: ignored/private file was force-added")
            continue
        if ignored.returncode != 1:
            raise RuntimeError("Unable to validate ignore rules")
        if int(git("cat-file", "-s", object_id)) > MAX_FILE_BYTES:
            problems.append(f"{path}: oversized artifact requires separate review")
            continue
        try:
            text = git("cat-file", "blob", object_id).decode("utf-8")
        except UnicodeDecodeError:
            problems.append(f"{path}: binary data is not allowed in this text-only release")
            continue
        for finding in content_findings(text, private_values):
            problems.append(f"{path}: {finding} detected (value withheld)")
    if count == 0:
        problems.append("No staged/tracked publication files; explicitly stage the reviewed files first")
    return count, problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-config", type=Path, help="Optional local Prusa config, never printed/copied")
    parser.add_argument("--private-data", type=Path, help="Optional local real dataset, never printed/copied")
    args = parser.parse_args(argv)
    try:
        private_values = load_private_values(args.private_config, args.private_data)
        count, problems = audit_index(private_values)
    except Exception:
        print("Publication audit could not complete; no matching values were logged.")
        return 1
    if problems:
        for problem in problems:
            print(problem)
        return 1
    print(f"Publication guard passed: {count} Git-index text files; private-value comparisons={len(private_values)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
