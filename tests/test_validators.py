"""Unit tests for validator helpers."""

from __future__ import annotations

from src.validation.schema_models import Hypothesis
from src.validation.validators import (
    _is_degenerate_metric_stats,
    assemble_dry_run_sql,
    build_catalog_index,
    parse_jsonl_hypotheses,
    validate_catalog_existence,
    validate_derived_data_type_compatibility,
    validate_derived_metric_contract,
    validate_pii_exclusion,
    validate_references_within_declared_tables,
    validate_supported_table_scope,
    validate_supported_derived_sql,
)


def _mock_snapshot() -> dict:
    return {
        "tables": [
            {
                "catalog": "dev_analytics",
                "schema": "sales",
                "table": "orders",
                "columns": [
                    {"name": "order_date", "data_type": "date", "pii": False, "tags": {}},
                    {
                        "name": "customer_email",
                        "data_type": "string",
                        "pii": True,
                        "tags": {"pii": "true"},
                    },
                    {"name": "revenue", "data_type": "double", "pii": False, "tags": {}},
                ],
            }
        ]
    }


def _sample_hypothesis() -> Hypothesis:
    return Hypothesis.model_validate(
        {
            "hypothesis_id": "H01",
            "statement": "Revenue changes with discount policy.",
            "tables": ["`dev_analytics`.`sales`.`orders`"],
            "required_columns": ["orders.order_date", "orders.revenue"],
            "derived_columns": [
                {
                    "name": "avg_revenue",
                    "sql_expression": "AVG(revenue)",
                    "data_type": "DOUBLE",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1000, "direction": "above"},
            "priority": "P1",
            "notes": "",
            "requires_new_source": False,
        }
    )


def test_catalog_existence_validation() -> None:
    index = build_catalog_index(_mock_snapshot())
    hypothesis = _sample_hypothesis()
    errors = validate_catalog_existence(hypothesis, index)
    assert errors == []


def test_pii_exclusion_validation() -> None:
    index = build_catalog_index(_mock_snapshot())
    hypothesis = Hypothesis.model_validate(
        {
            **_sample_hypothesis().model_dump(),
            "required_columns": ["orders.customer_email"],
        }
    )
    errors = validate_pii_exclusion(hypothesis, index)
    assert errors
    assert "PII column is not allowed" in errors[0]


def test_assemble_dry_run_sql() -> None:
    hypothesis = _sample_hypothesis()
    sql = assemble_dry_run_sql(hypothesis, "AVG(revenue)", "avg_revenue")
    assert sql.startswith("EXPLAIN SELECT")
    assert "LIMIT 0" in sql
    assert "CROSS JOIN" not in sql


def test_parse_jsonl_normalizes_bare_required_columns() -> None:
    jsonl_lines = [
        (
            '{"hypothesis_id":"H01","statement":"test statement",'
            '"tables":["`dev_analytics`.`sales`.`orders`"],'
            '"required_columns":["order_date","revenue"],'
            '"derived_columns":[],"window":"7d","granularity":"daily",'
            '"threshold":{"type":"gt","value":1,"direction":"above"},'
            '"priority":"P1","notes":"","requires_new_source":false}'
        )
    ]
    context_bundle = {
        "tables": [
            {
                "fqn": "`dev_analytics`.`sales`.`orders`",
                "table": "orders",
                "columns": [
                    {"name": "order_date"},
                    {"name": "revenue"},
                ],
            }
        ]
    }

    parsed = parse_jsonl_hypotheses(jsonl_lines, context_bundle=context_bundle)
    assert parsed.parse_errors == []
    model = parsed.hypotheses_by_id["H01"]
    assert model.required_columns == ["orders.order_date", "orders.revenue"]


def test_reference_must_be_within_declared_tables() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H02",
            "statement": "Reference outside declared tables should be rejected.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.order_date", "customers.customer_id"],
            "derived_columns": [],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_references_within_declared_tables(hypothesis)
    assert errors
    assert "declared tables" in errors[0]


def test_multi_table_derived_expression_requires_table_qualified_refs() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H03",
            "statement": "Ambiguous derived expression should be rejected for multi-table case.",
            "tables": ["dev_analytics.sales.orders", "dev_analytics.sales.customers"],
            "required_columns": ["orders.order_date", "customers.customer_email"],
            "derived_columns": [
                {"name": "avg_revenue", "sql_expression": "AVG(revenue)", "data_type": "DOUBLE"}
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_references_within_declared_tables(hypothesis)
    assert errors
    assert any("table-qualified references" in msg for msg in errors)


def test_validate_supported_table_scope_rejects_multi_table_hypothesis() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H10",
            "statement": "Multiple source tables should be rejected.",
            "tables": ["dev_analytics.sales.orders", "dev_analytics.sales.customers"],
            "required_columns": ["orders.order_date", "orders.revenue"],
            "derived_columns": [
                {
                    "name": "non_negative_revenue",
                    "sql_expression": "CASE WHEN orders.revenue > 0 THEN orders.revenue ELSE 0 END",
                    "data_type": "DOUBLE",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_supported_table_scope(hypothesis)
    assert errors
    assert "exactly one source table" in errors[0]


def test_validate_supported_derived_sql_rejects_window_expressions() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H04",
            "statement": "Window function should be rejected.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.order_date", "orders.revenue"],
            "derived_columns": [
                {
                    "name": "rolling_rank",
                    "sql_expression": "ROW_NUMBER() OVER (ORDER BY orders.revenue DESC)",
                    "data_type": "BIGINT",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_supported_derived_sql(hypothesis)
    assert errors
    assert "OVER" in errors[0] or "ROW_NUMBER" in errors[0]


def test_validate_supported_derived_sql_rejects_aggregate_wrapper() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H05",
            "statement": "Aggregate wrapper should be rejected for row-level metrics.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.order_date", "orders.revenue"],
            "derived_columns": [
                {
                    "name": "total_revenue",
                    "sql_expression": "SUM(orders.revenue)",
                    "data_type": "DOUBLE",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_supported_derived_sql(hypothesis)
    assert errors
    assert "aggregate wrapper" in errors[0]


def test_is_degenerate_metric_stats_flags_constant_zero_like() -> None:
    assert _is_degenerate_metric_stats(
        {"nonnull_rows": 100, "distinct_nonnull": 1, "zero_like_rows": 100}
    )
    assert not _is_degenerate_metric_stats(
        {"nonnull_rows": 100, "distinct_nonnull": 5, "zero_like_rows": 20}
    )


def test_validate_derived_metric_contract_rejects_passthrough_expression() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H06",
            "statement": "Derived metric should not be a raw passthrough.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.revenue"],
            "derived_columns": [
                {
                    "name": "metric_copy",
                    "sql_expression": "orders.revenue",
                    "data_type": "DOUBLE",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_derived_metric_contract(hypothesis)
    assert errors
    assert "direct column passthrough" in errors[0]


def test_validate_derived_metric_contract_rejects_non_numeric_data_type() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H07",
            "statement": "Derived metric should be numeric for evaluation.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.revenue"],
            "derived_columns": [
                {
                    "name": "is_large_revenue",
                    "sql_expression": "CASE WHEN orders.revenue > 100 THEN 1 ELSE 0 END",
                    "data_type": "STRING",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_derived_metric_contract(hypothesis)
    assert errors
    assert "must be numeric" in errors[0]


def test_validate_derived_metric_contract_requires_multiple_required_refs() -> None:
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H09",
            "statement": "Derived expression should use multiple required columns when declared.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.revenue", "orders.order_date"],
            "derived_columns": [
                {
                    "name": "revenue_non_negative",
                    "sql_expression": "CASE WHEN orders.revenue > 0 THEN orders.revenue ELSE 0 END",
                    "data_type": "DOUBLE",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P2",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_derived_metric_contract(hypothesis)
    assert errors
    assert "must use at least two required columns" in errors[-1]


def test_validate_derived_data_type_compatibility_for_simple_ref() -> None:
    index = build_catalog_index(_mock_snapshot())
    hypothesis = Hypothesis.model_validate(
        {
            "hypothesis_id": "H08",
            "statement": "Derived data type should align with source type for bare refs.",
            "tables": ["dev_analytics.sales.orders"],
            "required_columns": ["orders.revenue"],
            "derived_columns": [
                {
                    "name": "revenue_as_text",
                    "sql_expression": "orders.revenue",
                    "data_type": "STRING",
                }
            ],
            "window": "7d",
            "granularity": "daily",
            "threshold": {"type": "gt", "value": 1, "direction": "above"},
            "priority": "P3",
            "notes": "",
            "requires_new_source": False,
        }
    )
    errors = validate_derived_data_type_compatibility(hypothesis, index)
    assert errors
    assert "does not match source column type" in errors[0]
