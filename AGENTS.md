# Beam data pipeline

pipeline.py implements a real Beam graph invoked once per source; reports.py owns integer-cent SQLite reports and key checks. Use .venv and requirements-lock.txt locally; requirements-cloud-lock.txt stages the optional Google runner dependencies. See .agents/skills/verify-beam-data-pipeline/SKILL.md for current lifecycle commands and its cleanup limitation.

Run `.venv/Scripts/python -m pytest`, `.venv/Scripts/python -m ruff check .` and `.venv/Scripts/python -m build`. DirectRunner evidence does not prove Dataflow execution. Keep parse_float=Decimal at the JSON boundary and preserve checked integer money arithmetic. Retain previous failures and benchmark source revisions.

Automatic approval review rejected cleanup of the retained scratch directory. Do not retry or circumvent that action. No paid cloud jobs, credentials or deployment without authorization.
