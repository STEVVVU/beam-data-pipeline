---
name: verify-beam-data-pipeline
description: Drive the Beam batch CLI locally and preserve output evidence.
---

## Launch
From the project root, create `.venv` and install `requirements-lock.txt`. This CLI exits after each run; no server is started.

## Doctor
Run `.venv/Scripts/python -c "import apache_beam; print(apache_beam.__version__)"` and ensure fixture files exist.

## Drive
Run `.venv/Scripts/python -m pipeline --source orders --input fixtures/orders.jsonl --output scratch/orders` from the project root. Use a new scratch path for every run.

## Evidence
Save the command, exit status, clean and reject output files outside scratch in `evidence/`. Check rows and diagnostics, not merely exit status.

## Cleanup
Remove only the scratch directory created by this run. Verify evidence remains. No processes survive a completed CLI run.
