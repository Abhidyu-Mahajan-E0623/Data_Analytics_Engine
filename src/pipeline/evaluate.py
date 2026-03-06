"""Evaluate published hypotheses and store results."""

from __future__ import annotations

import ast
from datetime import datetime
from typing import Any

import orjson

from src.config.settings import Settings
from src.connectors.databricks_sql import DatabricksSQLClient, sql_quote
from src.utils.io import OUTPUT_REPORTS_DIR, atomic_write_text
from src.utils.time import parse_window_to_timedelta, utc_now


def evaluate_hypotheses(
    run_id: str,
    settings: Settings,
    sql_client: DatabricksSQLClient,
    window_override: str | None = None,
) -> dict[str, Any]:
    """Evaluate active hypotheses for a run and append results table rows."""
    catalog_table = _table_fqn(settings, "hypothesis_catalog")
    results_table = _table_fqn(settings, "hypothesis_results")
    rows = sql_client.fetch_all(
        f"""
        SELECT
            run_id, hypothesis_id, domain, tables, required_columns, derived_columns,
            window, granularity, threshold
        FROM {catalog_table}
        WHERE run_id = {sql_quote(run_id)}
          AND status = 'active'
        ORDER BY hypothesis_id
        """
    )

    evaluated = 0
    alerts = 0
    lines = [f"Evaluation Summary - {run_id}", "==============================", ""]
    for row in rows:
        window = window_override or row.get("window") or "7d"
        now = utc_now()
        start = now - parse_window_to_timedelta(window)
        tables = _coerce_list(row.get("tables"))
        required_columns = _coerce_list(row.get("required_columns"))
        derived_columns = _coerce_map(row.get("derived_columns"))
        threshold = _coerce_threshold(row.get("threshold"))
        domain = str(row.get("domain", ""))
        hypothesis_id = str(row.get("hypothesis_id"))

        metric_expression = next(iter(derived_columns.values()), "COUNT(*)")
        from_clause = " FROM " + " CROSS JOIN ".join(tables) if tables else ""
        time_column = _detect_time_column(required_columns)
        where_clause = ""
        if time_column:
            where_clause = (
                f" WHERE {time_column} >= TIMESTAMP '{start.strftime('%Y-%m-%d %H:%M:%S')}'"
            )
        metric_query = (
            "SELECT CAST(AVG(metric_value) AS DOUBLE) AS eval_value "
            f"FROM (SELECT CAST(({metric_expression}) AS DOUBLE) AS metric_value{from_clause}"
            f"{where_clause}) m"
        )

        try:
            result_row = sql_client.fetch_one(metric_query) or {}
            eval_value_raw = result_row.get("eval_value")
            eval_value = float(eval_value_raw) if eval_value_raw is not None else 0.0
            status, comparison_value, explanation = _compare_threshold(eval_value, threshold)
        except Exception as exc:
            eval_value = 0.0
            comparison_value = None
            status = "WARN"
            explanation = f"Evaluation query failed: {exc}"

        if status == "ALERT":
            alerts += 1
        insert_sql = _build_results_insert_sql(
            table=results_table,
            run_id=run_id,
            hypothesis_id=hypothesis_id,
            domain=domain,
            eval_window_start=start,
            eval_window_end=now,
            eval_value=eval_value,
            comparison_value=comparison_value,
            status=status,
            explanation=explanation,
        )
        sql_client.execute(insert_sql)

        lines.append(f"{hypothesis_id}: {status} (value={eval_value:.4f})")
        lines.append(f"Reason: {explanation}")
        lines.append("")
        evaluated += 1

    summary_path = OUTPUT_REPORTS_DIR / f"summary_{run_id}.txt"
    atomic_write_text(summary_path, "\n".join(lines).rstrip() + "\n")
    return {"run_id": run_id, "evaluated": evaluated, "alerts": alerts, "summary_path": str(summary_path)}


def _build_results_insert_sql(
    table: str,
    run_id: str,
    hypothesis_id: str,
    domain: str,
    eval_window_start: datetime,
    eval_window_end: datetime,
    eval_value: float,
    comparison_value: float | None,
    status: str,
    explanation: str,
) -> str:
    comparison_sql = "NULL" if comparison_value is None else str(float(comparison_value))
    return f"""
    INSERT INTO {table}
    (
        run_id, hypothesis_id, domain, eval_window_start, eval_window_end,
        eval_value, comparison_value, status, explanation, computed_at
    )
    VALUES
    (
        {sql_quote(run_id)},
        {sql_quote(hypothesis_id)},
        {sql_quote(domain)},
        TIMESTAMP '{eval_window_start.strftime('%Y-%m-%d %H:%M:%S')}',
        TIMESTAMP '{eval_window_end.strftime('%Y-%m-%d %H:%M:%S')}',
        {float(eval_value)},
        {comparison_sql},
        {sql_quote(status)},
        {sql_quote(explanation)},
        current_timestamp()
    )
    """


def _compare_threshold(eval_value: float, threshold: dict[str, Any]) -> tuple[str, float | None, str]:
    threshold_type = str(threshold.get("type", "")).lower()
    direction = str(threshold.get("direction", "")).lower()
    value = threshold.get("value")
    values = threshold.get("values")

    if values and isinstance(values, list) and len(values) >= 2:
        low, high = float(values[0]), float(values[1])
        if low <= eval_value <= high:
            return "OK", (low + high) / 2.0, f"Value {eval_value:.4f} is inside [{low}, {high}]"
        return "ALERT", (low + high) / 2.0, f"Value {eval_value:.4f} is outside [{low}, {high}]"

    if value is None:
        return "WARN", None, "Threshold has no comparable value."

    target = float(value)
    if direction in {"up", "increase", "above", "greater", "greater_than"} or threshold_type in {
        "gt",
        "greater_than",
        "min",
    }:
        if eval_value > target:
            return "OK", target, f"Value {eval_value:.4f} is above threshold {target:.4f}"
        delta = abs(eval_value - target)
        if target > 0 and delta / target <= 0.05:
            return "WARN", target, f"Value {eval_value:.4f} is close to threshold {target:.4f}"
        return "ALERT", target, f"Value {eval_value:.4f} is below threshold {target:.4f}"

    if direction in {"down", "decrease", "below", "less", "less_than"} or threshold_type in {
        "lt",
        "less_than",
        "max",
    }:
        if eval_value < target:
            return "OK", target, f"Value {eval_value:.4f} is below threshold {target:.4f}"
        delta = abs(eval_value - target)
        if target > 0 and delta / target <= 0.05:
            return "WARN", target, f"Value {eval_value:.4f} is close to threshold {target:.4f}"
        return "ALERT", target, f"Value {eval_value:.4f} exceeds threshold {target:.4f}"

    if eval_value == target:
        return "OK", target, f"Value equals target {target:.4f}"
    return "WARN", target, f"Unsupported threshold direction/type. value={eval_value:.4f}, target={target:.4f}"


def _detect_time_column(required_columns: list[str]) -> str | None:
    for column in required_columns:
        normalized = column.replace("`", "").lower()
        parts = normalized.split(".")
        if not parts:
            continue
        col_name = parts[-1]
        if any(token in col_name for token in ("date", "time", "timestamp", "_dt")):
            return normalized
    return None


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = orjson.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except orjson.JSONDecodeError:
            pass
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except Exception:
            pass
        return [text]
    return [str(value)]


def _coerce_map(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = orjson.loads(text)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items()}
        except orjson.JSONDecodeError:
            pass
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items()}
        except Exception:
            pass
    return {}


def _coerce_threshold(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = orjson.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except orjson.JSONDecodeError:
            return {}
    return {}


def _table_fqn(settings: Settings, table_name: str) -> str:
    return f"`{settings.DATABRICKS_CATALOG}`.`{settings.DATABRICKS_SCHEMA_MONITORING}`.`{table_name}`"
