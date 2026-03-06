"""Unit tests for metrics table schema generation."""

from __future__ import annotations

import orjson

from src.pipeline.metrics_table import (
    _build_hypothesis_source_pairs,
    _collect_metric_columns,
    _metric_table_name,
    _to_row_level_expression,
    _usage_json,
)
from src.validation.schema_models import Hypothesis


def _metadata_snapshot() -> dict:
    return {
        "tables": [
            {
                "catalog": "demo",
                "schema": "sales",
                "table": "orders",
                "columns": [
                    {"name": "sale_date", "data_type": "DATE"},
                    {"name": "discount_pct", "data_type": "DOUBLE"},
                ],
            },
            {
                "catalog": "demo",
                "schema": "sales",
                "table": "customers",
                "columns": [
                    {"name": "customer_id", "data_type": "STRING"},
                    {"name": "state", "data_type": "STRING"},
                ],
            },
        ]
    }


def _hypothesis(hid: str) -> Hypothesis:
    return Hypothesis.model_validate(
        {
            "hypothesis_id": hid,
            "statement": f"{hid} statement",
            "tables": ["demo.sales.orders"],
            "required_columns": ["orders.sale_date", "orders.discount_pct"],
            "derived_columns": [
                {"name": "avg_discount", "sql_expression": "AVG(discount_pct)", "data_type": "DOUBLE"}
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P1",
            "notes": "",
            "requires_new_source": False,
        }
    )


def _multi_table_hypothesis(hid: str) -> Hypothesis:
    return Hypothesis.model_validate(
        {
            "hypothesis_id": hid,
            "statement": f"{hid} multi-table",
            "tables": ["demo.sales.orders", "demo.sales.customers"],
            "required_columns": [
                "orders.sale_date",
                "orders.discount_pct",
                "customers.state",
            ],
            "derived_columns": [
                {
                    "name": "order_discount_avg",
                    "sql_expression": "AVG(orders.discount_pct)",
                    "data_type": "DOUBLE",
                },
                {
                    "name": "customer_state_len",
                    "sql_expression": "LENGTH(customers.state)",
                    "data_type": "INT",
                },
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P1",
            "notes": "",
            "requires_new_source": False,
        }
    )


def test_collect_metric_columns_keeps_hypothesis_specific_columns() -> None:
    cols = _collect_metric_columns([_hypothesis("H02"), _hypothesis("H06")], _metadata_snapshot())
    by_name = {c.name: c for c in cols}

    required_col = by_name.get("orders__sale_date")
    assert required_col is not None
    assert required_col.hypothesis_ids == {"H02", "H06"}

    derived_col = by_name.get("avg_discount")
    assert derived_col is not None
    assert derived_col.hypothesis_ids == {"H02", "H06"}

    usage = orjson.loads(_usage_json(cols))
    assert usage["orders__sale_date"] == "H02,H06"
    assert usage["avg_discount"] == "H02,H06"


def test_metric_table_name_uses_hypothesis_prefix() -> None:
    used: dict[str, str] = {}
    table_name = _metric_table_name("H01", "demo.sales.orders", used)
    assert table_name == "metric_h01_orders"


def test_build_hypothesis_source_pairs_has_entries_per_table() -> None:
    pairs = _build_hypothesis_source_pairs([_multi_table_hypothesis("H01")])
    pair_keys = {(item.hypothesis.hypothesis_id, item.source_table) for item in pairs}
    assert ("H01", "demo.sales.orders") in pair_keys
    assert ("H01", "demo.sales.customers") in pair_keys


def test_collect_metric_columns_scoped_per_source_table() -> None:
    rows = [_multi_table_hypothesis("H03")]
    order_cols = _collect_metric_columns(
        hypotheses=rows,
        metadata_snapshot=_metadata_snapshot(),
        source_table="demo.sales.orders",
    )
    customer_cols = _collect_metric_columns(
        hypotheses=rows,
        metadata_snapshot=_metadata_snapshot(),
        source_table="demo.sales.customers",
    )

    order_names = {col.name for col in order_cols}
    customer_names = {col.name for col in customer_cols}

    assert "orders__sale_date" in order_names
    assert "orders__discount_pct" in order_names
    assert "order_discount_avg" in order_names
    assert "customers__state" not in order_names
    assert "customer_state_len" not in order_names

    assert "customers__state" in customer_names
    assert "customer_state_len" in customer_names
    assert "orders__sale_date" not in customer_names
    assert "order_discount_avg" not in customer_names


def test_to_row_level_expression_rewrites_aggregate_wrappers() -> None:
    assert (
        _to_row_level_expression("SUM(CASE WHEN orders.discount_pct > 0 THEN orders.discount_pct ELSE 0 END)")
        == "CASE WHEN orders.discount_pct > 0 THEN orders.discount_pct ELSE 0 END"
    )
    assert _to_row_level_expression("AVG(orders.discount_pct)") == "orders.discount_pct"
    assert _to_row_level_expression("COUNT(*)") == "1"
    assert (
        _to_row_level_expression("COUNT(orders.sale_id)")
        == "CASE WHEN orders.sale_id IS NULL THEN 0 ELSE 1 END"
    )
    assert (
        _to_row_level_expression("CASE WHEN orders.sales_channel = 'Online' THEN orders.quantity ELSE 0 END")
        == "CASE WHEN LOWER(orders.sales_channel) = 'online' THEN orders.quantity ELSE 0 END"
    )
    assert (
        _to_row_level_expression("CASE WHEN orders.region IN ('East', 'WEST') THEN orders.quantity ELSE 0 END")
        == "CASE WHEN LOWER(orders.region) IN ('east', 'west') THEN orders.quantity ELSE 0 END"
    )
