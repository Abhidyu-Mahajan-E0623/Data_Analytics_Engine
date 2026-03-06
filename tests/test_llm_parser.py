"""Tests for resilient LLM payload parsing."""

from __future__ import annotations

import orjson

from src.llm.azure_openai import _parse_hypothesis_payload


def test_parse_payload_with_jsonl_string() -> None:
    row = {
        "hypothesis_id": "H01",
        "statement": "s",
        "tables": ["t"],
        "required_columns": ["t.c"],
        "derived_columns": [],
        "window": "7d",
        "granularity": "daily",
        "threshold": {"type": "gt", "value": 1, "direction": "above"},
        "priority": "P1",
        "notes": "",
        "requires_new_source": False,
    }
    payload = {"human_text": "ok", "jsonl": orjson.dumps(row).decode("utf-8")}
    content = orjson.dumps(payload).decode("utf-8")
    _, lines = _parse_hypothesis_payload(content)
    assert len(lines) == 1
    assert '"hypothesis_id":"H01"' in lines[0]


def test_parse_payload_with_array() -> None:
    content = (
        '['
        '{"hypothesis_id":"H01","statement":"s1","tables":["t"],"required_columns":["t.c"],'
        '"derived_columns":[],"window":"7d","granularity":"daily","threshold":{"type":"gt","value":1,'
        '"direction":"above"},"priority":"P1","notes":"","requires_new_source":false},'
        '{"hypothesis_id":"H02","statement":"s2","tables":["t"],"required_columns":["t.c"],'
        '"derived_columns":[],"window":"7d","granularity":"daily","threshold":{"type":"gt","value":1,'
        '"direction":"above"},"priority":"P2","notes":"","requires_new_source":false}'
        ']'
    )
    _, lines = _parse_hypothesis_payload(content)
    assert len(lines) == 2


def test_parse_payload_with_streamed_json_objects() -> None:
    content = (
        '{"hypothesis_id":"H01","statement":"s1","tables":["t"],"required_columns":["t.c"],'
        '"derived_columns":[],"window":"7d","granularity":"daily","threshold":{"type":"gt","value":1,'
        '"direction":"above"},"priority":"P1","notes":"","requires_new_source":false}'
        '\n'
        '{"hypothesis_id":"H02","statement":"s2","tables":["t"],"required_columns":["t.c"],'
        '"derived_columns":[],"window":"7d","granularity":"daily","threshold":{"type":"gt","value":1,'
        '"direction":"above"},"priority":"P2","notes":"","requires_new_source":false}'
    )
    _, lines = _parse_hypothesis_payload(content)
    assert len(lines) == 2
