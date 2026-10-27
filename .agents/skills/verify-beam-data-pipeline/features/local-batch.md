# Local batch

## Sub-features
Normalize orders, deterministic key resolution, and rejected-row diagnostics.

## How to get to it (user POV)
Install the locked dependencies and run the CLI on the bundled order fixture.

## Driving it with the CLI
Follow Drive and Evidence in the parent skill. Confirm clean rows and rejects are written.

## Gotchas
Use a fresh output prefix; output files are sharded. The local runner does not prove cloud connectivity.
