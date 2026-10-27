import json

from apache_beam.pvalue import TaggedOutput

from pipeline import parse, resolve


def test_parse_and_conflicts():
    good = '{"order_id":"1","customer_id":"c","product_id":"p","order_date":"2024-01-01","quantity":"2"}'
    row = next(parse(good, "orders"))
    assert row["quantity"] == 2
    assert isinstance(next(parse(good.replace("2024-01-01", "bad"), "orders")), TaggedOutput)
    assert list(resolve(("1", [row, row]))) == [row]
    assert len(list(resolve(("1", [row, dict(row, quantity=3)])))) == 2
    assert isinstance(next(parse(json.dumps({"product_id":"p", "name":"x", "category":"y", "price":"NaN"}), "products")), TaggedOutput)
