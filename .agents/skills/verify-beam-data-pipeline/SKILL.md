---
name: verify-beam-data-pipeline
description: Drive the Beam batch CLI locally and preserve output evidence.
---

## Status

The real CLI, data checks and evidence retention are proved. The complete lifecycle remains draft because automatic approval review rejected cleanup of the retained scratch directory. Do not retry that deletion or use a different mechanism to circumvent the rejection. The retained scratch is 1,128 bytes; see evidence/verification.md. Running the CLI is not permission to bypass this boundary.

## Launch
From the project root, create `.venv` and install `requirements-lock.txt`. This CLI exits after each run; no server is started.

## Doctor
Run `.venv/Scripts/python -c "import apache_beam; print(apache_beam.__version__)"` and ensure fixture files exist.

## Drive
Run `.venv/Scripts/python -m pipeline --source orders --input fixtures/orders.jsonl --output scratch/orders` from the project root. Use a new scratch path for every run.

## Evidence
Save the command, exit status, clean and reject output files outside scratch in `evidence/`. Check rows and diagnostics, not merely exit status.

## Cleanup
No processes survive a completed CLI run. Preserve evidence outside runtime directories. Cleanup of the previously retained scratch directory is blocked as described above. Do not claim that part of the lifecycle passed.
