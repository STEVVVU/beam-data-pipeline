"""Run isolated DirectRunner CLI workloads; retain every outcome."""
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def main():
    receipt = {"python": sys.version, "platform": platform.platform(), "processor": platform.processor(),
               "logical_cpus": os.cpu_count(), "source_sha256": hashlib.sha256(Path("pipeline.py").read_bytes()).hexdigest(), "runs": []}
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        for count in (100, 1000):
            source = root / f"input-{count}.jsonl"
            source.write_text("".join(json.dumps({"order_id": str(i), "customer_id": "C1", "product_id": "P1", "order_date": "2024-01-01", "quantity": 1}) + "\n" for i in range(count)), encoding="utf-8")
            for repeat in range(5):
                prefix = root / f"run-{count}-{repeat}"
                command = [sys.executable, "-m", "pipeline", "--source", "orders", "--input", str(source), "--output", str(prefix)]
                start = time.perf_counter()
                result = subprocess.run(command, capture_output=True, text=True, check=False)
                elapsed = time.perf_counter() - start
                clean = [json.loads(line) for file in root.glob(prefix.name + "-clean-*") for line in file.read_text().splitlines()]
                rejects = [line for file in root.glob(prefix.name + "-rejects-*") for line in file.read_text().splitlines()]
                receipt["runs"].append({"input_rows": count, "repeat": repeat + 1, "elapsed_seconds": elapsed, "exit_code": result.returncode,
                                        "clean_rows": len(clean), "reject_rows": len(rejects), "oracle": sorted(int(row["order_id"]) for row in clean) == list(range(count)), "stderr": result.stderr})
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
