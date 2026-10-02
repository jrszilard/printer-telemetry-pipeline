# Data privacy and publication safety

This public project contains code and **reviewed synthetic/aggregate documentation only**. Real printer
readings, pseudonymous real device ids, private job names, credentials, and local assistant/interview
configuration must stay local. A hashed device id is pseudonymous, not anonymous.

## Ignore policy

`.gitignore` excludes runtime folders and partitioned uploads, database/Parquet/JSONL/CSV exports, logs,
dotenv variants, credential/key files, backups/archives, local agent/editor settings, and interview-only
notes. Even dotenv example files are excluded; the README contains placeholders instead. Put every
collection/simulation run under `data/`, including custom output paths. The draft CI workflow is local-only
in this release; publishing it requires separately approved GitHub workflow permission.

**Ignore rules are not a content scanner.** They do not remove already-tracked files or history, cannot
prevent `git add -f`, and cannot recognize every secret pasted into an ordinary source/doc file.

## Before publishing

1. Review `git status` and explicitly stage only intended code/docs/tests. Do not stage the whole workspace.
2. Run the tests and the exact-index guard:
   ```bash
   .venv/bin/python -m pytest -q
   .venv/bin/python -m scripts.check_publication
   ```
3. For the local real-printer checkout, additionally compare against known private data **without printing
   matching values**:
   ```bash
   .venv/bin/python -m scripts.check_publication \
     --private-config ~/.config/lakeshore/prusalink.env --private-data data/real
   ```
   The optional inputs are read locally, never copied into the public Git tree or a finding message.
4. Run a full secret scanner over staged changes and committed history. The initial public release was
   audited with Gitleaks 8.30.1 (redacted findings), in addition to the repository guard. Future releases
   should repeat that scan; the guard's recognized token patterns are intentionally not exhaustive.
5. Inspect the staged tree, verify no generated/private files or symlinks are present, and use a GitHub
   no-reply commit email if a personal address should not become public. Check public visibility and
   anonymous access after publication.

The guard reads Git-index blobs, not just working copies, rejects ignored files that were force-added,
and restricts this release to small UTF-8 files in reviewed folders. Safety regression tests exercise
ignore behavior, token-shaped fixtures, and staged-versus-working-copy differences.

## If something sensitive is accidentally published

Revoke/rotate the affected credential first. Removing a file or adding an ignore rule does not undo
exposure; assess Git history, forks, caches, Actions logs, and artifacts. Coordinate any history rewrite
with the repository owner. Never paste live secrets or real telemetry into public issues or pull requests.

## Printer boundary

The collector only performs four allowlisted GETs and does not follow redirects. It has no upload,
start/stop, or configuration operations. Serial/credential fields are removed before storing responses;
other content (including possible job filenames) remains private. Tests use synthetic clients/fleets and
must not contact a real printer or load live credentials in CI.
