"""Pydantic schema models for hypotheses."""

from __future__ import annotations

import re
from typing import Literal

import orjson
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

WINDOW_PATTERN = re.compile(r"^\d+[dwmy]$")
HYPOTHESIS_ID_PATTERN = re.compile(r"^H(0[1-9]|10)$")


class DerivedColumn(BaseModel):
    """Derived metric definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    sql_expression: str = Field(min_length=1)
    data_type: str = Field(min_length=1)


class ThresholdPattern(BaseModel):
    """Threshold rule for hypothesis evaluation."""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    value: float | None = None
    values: list[float] | None = None
    direction: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_values(self) -> "ThresholdPattern":
        has_value = self.value is not None
        has_values = bool(self.values)
        if not has_value and not has_values:
            raise ValueError("threshold must include either value or values")
        return self


class Hypothesis(BaseModel):
    """Validated hypothesis contract."""

    model_config = ConfigDict(extra="forbid")

    hypothesis_id: str
    statement: str = Field(min_length=5)
    tables: list[str] = Field(min_length=1)
    required_columns: list[str] = Field(min_length=1)
    derived_columns: list[DerivedColumn] = Field(default_factory=list)
    window: str
    granularity: Literal["daily", "weekly"]
    threshold: ThresholdPattern
    priority: Literal["P1", "P2", "P3"]
    notes: str = ""
    requires_new_source: bool = False

    @field_validator("hypothesis_id")
    @classmethod
    def validate_hypothesis_id(cls, value: str) -> str:
        if not HYPOTHESIS_ID_PATTERN.match(value):
            raise ValueError("hypothesis_id must match H01..H10")
        return value

    @field_validator("window")
    @classmethod
    def validate_window(cls, value: str) -> str:
        if not WINDOW_PATTERN.match(value.strip().lower()):
            raise ValueError("window must look like 7d/28d/4w/1m/1y")
        return value.strip().lower()

    @field_validator("required_columns")
    @classmethod
    def validate_required_columns(cls, columns: list[str]) -> list[str]:
        for item in columns:
            if "." not in item:
                raise ValueError(f"required column '{item}' must include table.column")
        return columns


def parse_hypothesis_line(raw_line: str) -> Hypothesis:
    """Parse one JSONL line into a hypothesis model."""
    payload = orjson.loads(raw_line)
    return Hypothesis.model_validate(payload)


def hypothesis_json_schema() -> dict:
    """Expose JSON schema for external checks."""
    return Hypothesis.model_json_schema()


def parse_hypothesis_line_safe(raw_line: str) -> tuple[Hypothesis | None, str | None]:
    """Safe parser that returns error text instead of raising."""
    try:
        return parse_hypothesis_line(raw_line), None
    except (orjson.JSONDecodeError, ValidationError, ValueError) as exc:
        return None, str(exc)
