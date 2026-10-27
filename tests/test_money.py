import json
import sqlite3
from decimal import Decimal

import pytest
from apache_beam.pvalue import TaggedOutput

from pipeline import parse
from reports import QUERIES, TABLES, multiply_cents


def product(price):
    return next(parse(json.dumps({"product_id": "p", "name": "x", "category": "y", "price": price}), "products"))


def test_cents_boundaries():
    assert product("0.01")["price_cents"] == 1
    assert product("92233720368547758.07")["price_cents"] == 9223372036854775807
    for value in ("-1", "0.001", "NaN", "92233720368547758.08"):
        assert isinstance(product(value), TaggedOutput)
    with pytest.raises(OverflowError):
        multiply_cents(2, 9223372036854775807)


@pytest.mark.parametrize("token,cents", [("0.29", 29), ("100000000000000.01", 10000000000000001), ("92233720368547758.07", 9223372036854775807)])
def test_numeric_and_string_prices_are_exact(token, cents):
    prefix = '{"product_id":"p","name":"x","category":"y","price":'
    for value in (token, json.dumps(token)):
        assert next(parse(prefix + value + "}", "products"))["price_cents"] == cents


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "0.001", "92233720368547758.08", "1.2.3"])
def test_invalid_numeric_tokens_are_rejected(token):
    line = '{"product_id":"p","name":"x","category":"y","price":' + token + "}"
    assert isinstance(next(parse(line, "products")), TaggedOutput)


def test_sql_matches_decimal_reference():
    db = sqlite3.connect(":memory:")
    db.create_function("multiply_cents", 2, multiply_cents)
    for table, schema in TABLES.items():
        db.execute(f"CREATE TABLE {table} ({schema})")
    db.execute("INSERT INTO customers VALUES ('c','Ada','North')")
    db.execute("INSERT INTO products VALUES ('p','x','y', 29)")
    db.execute("INSERT INTO orders VALUES ('o','c','p','2024-01-01', 3)")
    expected = int(Decimal("0.29") * 3 * 100)
    assert db.execute(QUERIES["region_revenue_cents"]).fetchall() == [("North", expected)]
    assert db.execute(QUERIES["customer_spend_cents"]).fetchall() == [("c", "Ada", expected)]
