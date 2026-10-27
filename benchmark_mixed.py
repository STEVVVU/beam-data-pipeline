"""Single observed runs at two workloads, not a statistical comparison."""
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

import apache_beam


def main():
    receipt = {"python": sys.version, "beam": apache_beam.__version__, "platform": platform.platform(),
               "processor": platform.processor(), "logical_cpus": os.cpu_count(),
               "source_sha256": hashlib.sha256(Path("pipeline.py").read_bytes()).hexdigest(), "runs": []}
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        for count in (10000, 100000):
            source = root / f"input-{count}.jsonl"
            expected_clean = set()
            expected_rejects = Counter()
            with source.open("w", encoding="utf-8") as stream:
                for i in range(count):
                    row = {"order_id": str(i), "customer_id": "C1", "product_id": "P1", "order_date": "2024-01-01", "quantity": 1}
                    line = json.dumps(row)
                    stream.write(line + "\n")
                    if i % 100 == 0:
                        expected_rejects[("duplicate", f"conflicting key {i}", json.dumps(row, sort_keys=True))] += 1
                        row["quantity"] = 2
                        stream.write(json.dumps(row) + "\n")
                        expected_rejects[("duplicate", f"conflicting key {i}", json.dumps(row, sort_keys=True))] += 1
                    else:
                        expected_clean.add(str(i))
                    if i % 10 == 0:
                        stream.write(line + "\n")
                for _ in range(1000):
                    stream.write('{"order_id":"hot","customer_id":"C1","product_id":"P1","order_date":"2024-01-01","quantity":1}\n')
                expected_clean.add("hot")
                for _ in range(100):
                    stream.write("not-json\n")
                expected_rejects[("orders", "Expecting value: line 1 column 1 (char 0)", "not-json")] = 100
            prefix = root / f"run-{count}"
            command = [sys.executable, "-m", "pipeline", "--source", "orders", "--input", str(source), "--output", str(prefix)]
            start = time.perf_counter()
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            elapsed = time.perf_counter() - start
            clean = [json.loads(line) for file in root.glob(prefix.name + "-clean-*") for line in file.read_text(encoding="utf-8").splitlines()]
            rejects = [json.loads(line) for file in root.glob(prefix.name + "-rejects-*") for line in file.read_text(encoding="utf-8").splitlines()]
            keys = [row["order_id"] for row in clean]
            outcomes = Counter((row["source"], row["reason"], row["raw"]) for row in rejects)
            receipt["runs"].append({"base_keys": count, "input_lines": count + count // 100 + count // 10 + 1100,
                "input_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "elapsed_seconds": elapsed,
                "exit_code": result.returncode, "clean_rows": len(clean), "reject_rows": len(rejects),
                "keys_oracle": len(keys) == len(set(keys)) and set(keys) == expected_clean,
                "values_oracle": all(row["quantity"] == 1 and row["customer_id"] == "C1" and row["product_id"] == "P1" and row["order_date"] == "2024-01-01" for row in clean),
                "reject_oracle": outcomes == expected_rejects, "stdout": result.stdout, "stderr": result.stderr})
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
