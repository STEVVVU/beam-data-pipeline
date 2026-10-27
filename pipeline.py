"""One-source Beam batch normalization with an explicit reject stream."""
import argparse
import json
from datetime import date
from decimal import Decimal, InvalidOperation

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

FIELDS = {
    "customers": ("customer_id", "name", "region"),
    "products": ("product_id", "name", "category", "price"),
    "orders": ("order_id", "customer_id", "product_id", "order_date", "quantity"),
}
SCHEMAS = {
    "customers": "customer_id:STRING,name:STRING,region:STRING",
    "products": "product_id:STRING,name:STRING,category:STRING,price_cents:INTEGER",
    "orders": "order_id:STRING,customer_id:STRING,product_id:STRING,order_date:DATE,quantity:INTEGER",
}


def parse(line, source):
    try:
        row = json.loads(line, parse_float=Decimal)
        if not isinstance(row, dict):
            raise TypeError("expected object")
        result = {}
        for field in FIELDS[source]:
            value = row[field]
            if value is None or isinstance(value, (dict, list, bool)):
                raise ValueError(f"invalid {field}")
            text = str(value).strip()
            if not text:
                raise ValueError(f"empty {field}")
            result[field] = text
        if source == "products":
            price = Decimal(result["price"])
            if not price.is_finite() or price < 0 or price.as_tuple().exponent < -2 or price > Decimal("92233720368547758.07"):
                raise ValueError("price must fit nonnegative INT64 cents with at most two decimals")
            result["price_cents"] = int(price * 100)
            del result["price"]
        if source == "orders":
            result["order_date"] = date.fromisoformat(result["order_date"]).isoformat()
            quantity = int(result["quantity"])
            if quantity <= 0 or quantity > 9223372036854775807:
                raise ValueError("quantity must be a positive INT64")
            result["quantity"] = quantity
        yield result
    except (ValueError, TypeError, KeyError, InvalidOperation) as exc:
        yield beam.pvalue.TaggedOutput("rejects", {"source": source, "raw": line, "reason": str(exc)})


def resolve(item):
    key, rows = item
    unique = sorted({json.dumps(row, sort_keys=True) for row in rows})
    if len(unique) == 1:
        yield json.loads(unique[0])
    else:
        for row in unique:
            yield beam.pvalue.TaggedOutput("rejects", {"source": "duplicate", "raw": row, "reason": f"conflicting key {key}"})


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=FIELDS, required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, help="local or gs:// prefix for clean and reject files")
    parser.add_argument("--bq-table", help="optional project:dataset.table; WRITE_EMPTY prevents accidental replacement")
    args, options = parser.parse_known_args(argv)
    with beam.Pipeline(options=PipelineOptions(options)) as p:
        parsed = (p | "Read" >> beam.io.ReadFromText(args.input)
                  | "Parse" >> beam.FlatMap(parse, args.source).with_outputs("rejects", main="valid"))
        dedup = (parsed.valid | "Key" >> beam.Map(lambda row: (row[FIELDS[args.source][0]], row))
                 | "Group" >> beam.GroupByKey()
                 | "Resolve" >> beam.FlatMap(resolve).with_outputs("rejects", main="clean"))
        rejects = ((parsed.rejects, dedup.rejects) | "Combine rejects" >> beam.Flatten())
        _ = (rejects | "Reject JSON" >> beam.Map(json.dumps) | "Write rejects" >> beam.io.WriteToText(args.output + "-rejects"))
        if args.bq_table:
            _ = dedup.clean | "BigQuery" >> beam.io.WriteToBigQuery(args.bq_table, schema=SCHEMAS[args.source], write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY)
        else:
            _ = (dedup.clean | "Clean JSON" >> beam.Map(json.dumps) | "Write clean" >> beam.io.WriteToText(args.output + "-clean"))


if __name__ == "__main__":
    run()
