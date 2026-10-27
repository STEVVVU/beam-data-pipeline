"""Load local Beam JSONL outputs into SQLite and run key checks and reports."""
import argparse
import glob
import json
import sqlite3
from pathlib import Path

TABLES = {
    "customers": "customer_id TEXT, name TEXT, region TEXT",
    "products": "product_id TEXT, name TEXT, category TEXT, price_cents INTEGER",
    "orders": "order_id TEXT, customer_id TEXT, product_id TEXT, order_date TEXT, quantity INTEGER",
}
QUERIES = {
    "region_revenue_cents": "SELECT c.region, SUM(multiply_cents(o.quantity, p.price_cents)) revenue_cents FROM orders o JOIN customers c ON o.customer_id=c.customer_id JOIN products p ON o.product_id=p.product_id GROUP BY c.region ORDER BY c.region",
    "category_units": "SELECT p.category, SUM(o.quantity) units FROM orders o JOIN products p ON o.product_id=p.product_id JOIN customers c ON o.customer_id=c.customer_id GROUP BY p.category ORDER BY p.category",
    "customer_spend_cents": "SELECT c.customer_id, c.name, SUM(multiply_cents(o.quantity, p.price_cents)) spend_cents FROM orders o JOIN customers c ON o.customer_id=c.customer_id JOIN products p ON o.product_id=p.product_id GROUP BY c.customer_id,c.name ORDER BY c.customer_id",
}


def multiply_cents(quantity, cents):
    result = quantity * cents
    if result > 9223372036854775807:
        raise OverflowError("line total exceeds SQLite INT64 cents")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", help="directory containing <source>-clean-* output")
    args = parser.parse_args()
    db = sqlite3.connect(":memory:")
    db.create_function("multiply_cents", 2, multiply_cents)
    for table, schema in TABLES.items():
        db.execute(f"CREATE TABLE {table} ({schema})")
        files = glob.glob(str(Path(args.directory) / f"{table}-clean-*"))
        if not files:
            raise SystemExit(f"missing output for {table}")
        for file in files:
            for line in Path(file).read_text(encoding="utf-8").splitlines():
                row = json.loads(line)
                columns = list(row)
                db.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", [row[c] for c in columns])
    checks = {}
    for table in TABLES:
        key = {"customers": "customer_id", "products": "product_id", "orders": "order_id"}[table]
        checks[f"{table}_duplicate_keys"] = db.execute(f"SELECT COUNT(*) FROM (SELECT {key} FROM {table} GROUP BY {key} HAVING {key} IS NULL OR COUNT(*) > 1)").fetchone()[0]
    for table, key in (("customers", "customer_id"), ("products", "product_id")):
        checks[f"missing_{table}"] = db.execute(f"SELECT COUNT(*) FROM orders o LEFT JOIN {table} p ON o.{key}=p.{key} WHERE p.{key} IS NULL").fetchone()[0]
    print(json.dumps({"checks": checks, "reports": {name: db.execute(sql).fetchall() for name, sql in QUERIES.items()}}, indent=2))


if __name__ == "__main__":
    main()
