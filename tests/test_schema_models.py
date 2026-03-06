"""Unit tests for hypothesis schema models."""

from __future__ import annotations

import pytest

from src.validation.schema_models import Hypothesis, parse_hypothesis_line


def test_parse_valid_hypothesis_line() -> None:
    line = (
        '{"hypothesis_id":"H01","statement":"Revenue drops when discount rises too fast.",'
        '"tables":["`dev_analytics`.`sales`.`orders`"],'
        '"required_columns":["orders.order_date","orders.discount_pct"],'
        '"derived_columns":[{"name":"daily_discount","sql_expression":"AVG(discount_pct)","data_type":"DOUBLE"}],'
        '"window":"28d","granularity":"daily",'
        '"threshold":{"type":"gt","value":0.12,"direction":"above"},'
        '"priority":"P1","notes":"Monitor promotional bursts","requires_new_source":false}'
    )
    hypothesis = parse_hypothesis_line(line)
    assert isinstance(hypothesis, Hypothesis)
    assert hypothesis.hypothesis_id == "H01"
    assert hypothesis.window == "28d"


def test_parse_invalid_hypothesis_line_missing_threshold_value() -> None:
    line = (
        '{"hypothesis_id":"H02","statement":"Invalid threshold payload.",'
        '"tables":["`dev_analytics`.`sales`.`orders`"],'
        '"required_columns":["orders.order_date"],'
        '"derived_columns":[],"window":"7d","granularity":"daily",'
        '"threshold":{"type":"gt","direction":"above"},'
        '"priority":"P2","notes":"","requires_new_source":false}'
    )
    with pytest.raises(Exception):
        parse_hypothesis_line(line)
