"""Create and refresh hypothesis-wise metrics tables from latest hypotheses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import re
from typing import Any

import orjson

from src.config.settings import Settings
from src.connectors.databricks_sql import DatabricksSQLError, DatabricksSQLClient
from src.utils.time import parse_window_to_timedelta, utc_now
from src.validation.schema_models import Hypothesis

SQL_REF_PATTERN = re.compile(r"([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)")
AGG_WRAPPER_PATTERN = re.compile(r"^\s*(sum|avg|min|max)\s*\((.*)\)\s*$", flags=re.IGNORECASE | re.DOTALL)
COUNT_PATTERN = re.compile(r"^\s*count\s*\((.*)\)\s*$", flags=re.IGNORECASE | re.DOTALL)
EQ_STR_PATTERN = re.compile(r"([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\s*=\s*'([^']*)'", flags=re.IGNORECASE)
IN_STR_PATTERN = re.compile(
    r"([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\s+in\s*\(([^)]*)\)",
    flags=re.IGNORECASE,
)


@dataclass
class MetricColumn:
    """Column spec for metrics tables."""

    key: str
    name: str
    sql_type: str
    source_kind: str
    source_expression: str
    hypothesis_ids: set[str] = field(default_factory=set)


@dataclass
class HypothesisSourcePair:
    """One hypothesis + one source table assignment."""

    hypothesis: Hypothesis
    source_table: str


def create_or_replace_metrics_tables(
    sql_client: DatabricksSQLClient,
    settings: Settings,
    run_id: str,
    domain: str,
    focus_areas: list[str] | None,
    hypotheses: list[Hypothesis],
    metadata_snapshot: dict[str, Any],
) -> list[str]:
    """Create/replace hypothesis-wise tables named metric_<hypothesis_id>_<table_name>."""
    catalog = settings.DATABRICKS_CATALOG
    schema = settings.DATABRICKS_SCHEMA_MONITORING
    focus_csv = ",".join(item.strip().lower() for item in (focus_areas or []) if item.strip())
    sql_client.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    pairs = _build_hypothesis_source_pairs(hypotheses)
    if not pairs:
        return []

    table_name_map: dict[str, str] = {}
    table_name_by_pair: dict[tuple[str, str], str] = {}
    desired_tables: set[str] = set()

    for pair in pairs:
        pair_key = (pair.hypothesis.hypothesis_id, pair.source_table)
        table_name = _metric_table_name(
            hypothesis_id=pair.hypothesis.hypothesis_id,
            source_table=pair.source_table,
            used_names=table_name_map,
        )
        table_name_by_pair[pair_key] = table_name
        desired_tables.add(table_name)

    _cleanup_stale_metrics_tables(
        sql_client=sql_client,
        catalog=catalog,
        schema=schema,
        desired_table_names=desired_tables,
    )

    created_tables: list[str] = []
    for pair in pairs:
        pair_key = (pair.hypothesis.hypothesis_id, pair.source_table)
        table_name = table_name_by_pair[pair_key]
        table_fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
        _create_or_replace_hypothesis_metrics_table(
            sql_client=sql_client,
            table_fqn=table_fqn,
            source_table=pair.source_table,
            run_id=run_id,
            domain=domain,
            focus_csv=focus_csv,
            hypothesis=pair.hypothesis,
            metadata_snapshot=metadata_snapshot,
        )
        created_tables.append(table_fqn)
    return created_tables


def _build_hypothesis_source_pairs(hypotheses: list[Hypothesis]) -> list[HypothesisSourcePair]:
    pairs: list[HypothesisSourcePair] = []
    for hypothesis in sorted(hypotheses, key=lambda item: item.hypothesis_id):
        seen: set[str] = set()
        for raw_table in hypothesis.tables:
            source_table = _normalize_ref(raw_table)
            if not source_table or source_table in seen:
                continue
            seen.add(source_table)
            pairs.append(HypothesisSourcePair(hypothesis=hypothesis, source_table=source_table))
    pairs.sort(key=lambda item: (item.hypothesis.hypothesis_id, item.source_table))
    return pairs


def _metric_table_name(hypothesis_id: str, source_table: str, used_names: dict[str, str]) -> str:
    parts = _normalize_ref(source_table).split(".")
    table_name = parts[-1] if parts else "table"
    schema_name = parts[-2] if len(parts) >= 2 else "schema"
    hid = _sanitize_identifier(hypothesis_id.lower())
    pair_key = f"{hid}::{_normalize_ref(source_table)}"

    base = _sanitize_identifier(f"metric_{hid}_{table_name}")
    if base not in used_names or used_names[base] == pair_key:
        used_names[base] = pair_key
        return base

    alt = _sanitize_identifier(f"metric_{hid}_{schema_name}_{table_name}")
    if alt not in used_names or used_names[alt] == pair_key:
        used_names[alt] = pair_key
        return alt

    suffix = hashlib.sha1(pair_key.encode("utf-8")).hexdigest()[:8]
    final = _sanitize_identifier(f"{alt}_{suffix}")
    used_names[final] = pair_key
    return final


def _cleanup_stale_metrics_tables(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    desired_table_names: set[str],
) -> None:
    for table_name in _list_metrics_tables(sql_client, catalog, schema):
        lowered = table_name.lower()
        if lowered == "metrics" or lowered.startswith("metrics_") or lowered.startswith("metric_"):
            if table_name not in desired_table_names:
                sql_client.execute(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table_name}`")


def _list_metrics_tables(sql_client: DatabricksSQLClient, catalog: str, schema: str) -> list[str]:
    rows = sql_client.fetch_all(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    names: list[str] = []
    for row in rows:
        name = str(row.get("tablename", "")).strip()
        if name:
            names.append(name)
    return names


def _create_or_replace_hypothesis_metrics_table(
    sql_client: DatabricksSQLClient,
    table_fqn: str,
    source_table: str,
    run_id: str,
    domain: str,
    focus_csv: str,
    hypothesis: Hypothesis,
    metadata_snapshot: dict[str, Any],
) -> None:
    metric_columns = _collect_metric_columns(
        hypotheses=[hypothesis],
        metadata_snapshot=metadata_snapshot,
        source_table=source_table,
    )

    ddl_columns = [
        "`run_id` STRING",
        "`domain` STRING",
        "`focus_areas` STRING",
        "`source_table` STRING",
    ]
    ddl_columns.extend([f"`{col.name}` {col.sql_type}" for col in metric_columns])

    ddl_sql = (
        f"CREATE OR REPLACE TABLE {table_fqn} (\n  "
        + ",\n  ".join(ddl_columns)
        + "\n)\nUSING DELTA"
    )
    sql_client.execute(ddl_sql)

    _insert_hypothesis_metrics_rows(
        sql_client=sql_client,
        table_fqn=table_fqn,
        source_table=source_table,
        run_id=run_id,
        domain=domain,
        focus_csv=focus_csv,
        hypothesis=hypothesis,
        metric_columns=metric_columns,
    )


def _collect_metric_columns(
    hypotheses: list[Hypothesis],
    metadata_snapshot: dict[str, Any],
    source_table: str | None = None,
) -> list[MetricColumn]:
    type_lookup = _build_column_type_lookup(metadata_snapshot)
    source_table_name = _normalize_ref(source_table or "").split(".")[-1]
    col_map: dict[str, MetricColumn] = {}
    used_names: set[str] = set()

    for hypothesis in hypotheses:
        hid = hypothesis.hypothesis_id
        for required in hypothesis.required_columns:
            normalized = _normalize_ref(required)
            if source_table_name and _ref_table_name(normalized) != source_table_name:
                continue
            key = f"req::{normalized}"
            if key not in col_map:
                alias = _unique_name(_required_alias(normalized), used_names)
                used_names.add(alias)
                col_map[key] = MetricColumn(
                    key=key,
                    name=alias,
                    sql_type=_normalize_sql_type(type_lookup.get(normalized, "STRING")),
                    source_kind="required",
                    source_expression=normalized,
                )
            col_map[key].hypothesis_ids.add(hid)

        for derived in hypothesis.derived_columns:
            if source_table_name and not _derived_applies_to_source(
                derived.sql_expression,
                hypothesis,
                source_table_name,
            ):
                continue
            base_name = _sanitize_identifier(derived.name)
            expr_norm = _normalize_expression(derived.sql_expression)
            key = f"drv::{base_name}::{expr_norm}"
            if key not in col_map:
                alias = _unique_name(base_name, used_names)
                used_names.add(alias)
                col_map[key] = MetricColumn(
                    key=key,
                    name=alias,
                    sql_type=_normalize_sql_type(derived.data_type or "DOUBLE"),
                    source_kind="derived",
                    source_expression=derived.sql_expression,
                )
            col_map[key].hypothesis_ids.add(hid)

    return list(col_map.values())


def _insert_hypothesis_metrics_rows(
    sql_client: DatabricksSQLClient,
    table_fqn: str,
    source_table: str,
    run_id: str,
    domain: str,
    focus_csv: str,
    hypothesis: Hypothesis,
    metric_columns: list[MetricColumn],
) -> None:
    column_names = ["run_id", "domain", "focus_areas", "source_table"] + [col.name for col in metric_columns]

    base_exprs = [
        f"'{_escape(run_id)}' AS `run_id`",
        f"'{_escape(domain)}' AS `domain`",
        f"'{_escape(focus_csv)}' AS `focus_areas`",
        f"'{_escape(source_table)}' AS `source_table`",
    ]

    if hypothesis.requires_new_source:
        null_exprs = [f"CAST(NULL AS {col.sql_type}) AS `{col.name}`" for col in metric_columns]
        sql_client.execute(
            f"INSERT INTO {table_fqn} ({_quoted_join(column_names)})\n"
            f"SELECT {', '.join(base_exprs + null_exprs)}"
        )
        return

    where_clause = _build_window_where_clause(hypothesis, source_table)
    effective_where_clause = _resolve_effective_where_clause(sql_client, source_table, where_clause)
    active_keys = _active_metric_keys(hypothesis, source_table)

    metric_exprs: list[str] = []
    for col in metric_columns:
        if col.key not in active_keys:
            metric_exprs.append(f"CAST(NULL AS {col.sql_type}) AS `{col.name}`")
            continue
        metric_exprs.append(
            f"{_metric_row_sql(col.source_expression, col.sql_type, col.source_kind, source_table, effective_where_clause)} AS `{col.name}`"
        )

    source_from = f" FROM {_quote_table_ref(source_table)}"
    full_insert = (
        f"INSERT INTO {table_fqn} ({_quoted_join(column_names)})\n"
        f"SELECT {', '.join(base_exprs + metric_exprs)}{source_from}{effective_where_clause}"
    )
    try:
        sql_client.execute(full_insert)
        return
    except DatabricksSQLError:
        pass

    # Fallback: keep rows with required columns, null derived columns.
    required_only_exprs: list[str] = []
    for col in metric_columns:
        if col.key in active_keys and col.source_kind == "required":
            required_only_exprs.append(
                f"{_metric_row_sql(col.source_expression, col.sql_type, 'required', source_table, effective_where_clause)} AS `{col.name}`"
            )
        else:
            required_only_exprs.append(f"CAST(NULL AS {col.sql_type}) AS `{col.name}`")
    required_insert = (
        f"INSERT INTO {table_fqn} ({_quoted_join(column_names)})\n"
        f"SELECT {', '.join(base_exprs + required_only_exprs)}{source_from}{effective_where_clause}"
    )
    try:
        sql_client.execute(required_insert)
        return
    except DatabricksSQLError:
        pass

    # Last resort: insert a single placeholder row.
    placeholder_exprs = [f"CAST(NULL AS {col.sql_type}) AS `{col.name}`" for col in metric_columns]
    sql_client.execute(
        f"INSERT INTO {table_fqn} ({_quoted_join(column_names)})\n"
        f"SELECT {', '.join(base_exprs + placeholder_exprs)}"
    )


def _metric_row_sql(
    source_expression: str,
    sql_type: str,
    source_kind: str,
    source_table: str,
    where_clause: str,
) -> str:
    if source_kind == "required":
        return f"CAST({source_expression} AS {sql_type})"
    row_level_expression = _to_row_level_expression(source_expression)
    return f"CAST(({row_level_expression}) AS {sql_type})"


def _active_metric_keys(hypothesis: Hypothesis, source_table: str) -> set[str]:
    source_table_name = _normalize_ref(source_table).split(".")[-1]
    keys: set[str] = set()
    for required in hypothesis.required_columns:
        normalized = _normalize_ref(required)
        if _ref_table_name(normalized) == source_table_name:
            keys.add(f"req::{normalized}")
    for derived in hypothesis.derived_columns:
        if _derived_applies_to_source(derived.sql_expression, hypothesis, source_table_name):
            keys.add(
                f"drv::{_sanitize_identifier(derived.name)}::{_normalize_expression(derived.sql_expression)}"
            )
    return keys


def _usage_json(columns: list[MetricColumn]) -> str:
    """Retained for tests and lightweight diagnostics."""
    usage_map = {col.name: ",".join(sorted(col.hypothesis_ids)) for col in columns}
    return orjson.dumps(usage_map).decode("utf-8")


def _resolve_effective_where_clause(
    sql_client: DatabricksSQLClient,
    source_table: str,
    where_clause: str,
) -> str:
    if not where_clause:
        return ""
    try:
        row = sql_client.fetch_one(f"SELECT COUNT(*) AS c FROM {_quote_table_ref(source_table)}{where_clause}")
        if int((row or {}).get("c", 0)) > 0:
            return where_clause
        return ""
    except Exception:
        return where_clause


def _build_column_type_lookup(metadata_snapshot: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for table in metadata_snapshot.get("tables", []):
        catalog = str(table.get("catalog", "")).strip().lower()
        schema = str(table.get("schema", "")).strip().lower()
        table_name = str(table.get("table", "")).strip().lower()
        for column in table.get("columns", []):
            col_name = str(column.get("name", "")).strip().lower()
            dtype = str(column.get("data_type", "STRING")).strip() or "STRING"
            if not col_name:
                continue
            refs = {
                f"{table_name}.{col_name}",
                f"{schema}.{table_name}.{col_name}",
                f"{catalog}.{schema}.{table_name}.{col_name}",
            }
            for ref in refs:
                lookup[ref] = dtype
    return lookup


def _build_window_where_clause(hypothesis: Hypothesis, source_table: str) -> str:
    source_table_name = _normalize_ref(source_table).split(".")[-1]
    scoped_required = [
        column
        for column in hypothesis.required_columns
        if _ref_table_name(_normalize_ref(column)) == source_table_name
    ]
    time_col = _detect_time_column(scoped_required)
    if not time_col:
        return ""
    start: datetime = utc_now() - parse_window_to_timedelta(hypothesis.window)
    return f" WHERE {time_col} >= TIMESTAMP '{start.strftime('%Y-%m-%d %H:%M:%S')}'"


def _detect_time_column(required_columns: list[str]) -> str | None:
    prioritized = (
        "sale_date",
        "transaction_date",
        "event_date",
        "created_at",
        "created_ts",
        "updated_at",
        "updated_ts",
    )
    fallback = ("timestamp", "time", "date", "_dt", "expiry")

    normalized: list[tuple[str, str]] = []
    for raw in required_columns:
        ref = _normalize_ref(raw)
        parts = ref.split(".")
        col = parts[-1] if parts else ref
        normalized.append((ref, col))

    for token in prioritized:
        for ref, col in normalized:
            if token == col or token in col:
                return ref
    for token in fallback:
        for ref, col in normalized:
            if token in col:
                return ref
    return None


def _required_alias(column_ref: str) -> str:
    parts = column_ref.split(".")
    if len(parts) >= 2:
        return _sanitize_identifier(f"{parts[-2]}__{parts[-1]}")
    return _sanitize_identifier(column_ref.replace(".", "__"))


def _ref_table_name(column_ref: str) -> str:
    normalized = _normalize_ref(column_ref)
    parts = normalized.split(".")
    return parts[0] if parts else ""


def _derived_applies_to_source(
    expression: str,
    hypothesis: Hypothesis,
    source_table_name: str,
) -> bool:
    refs = {_ref_table_name(match) for match in SQL_REF_PATTERN.findall(expression)}
    refs.discard("")
    if refs:
        return refs == {source_table_name}

    declared = {
        _normalize_ref(table).split(".")[-1]
        for table in hypothesis.tables
        if _normalize_ref(table)
    }
    return declared == {source_table_name}


def _to_row_level_expression(expression: str) -> str:
    """Convert common aggregate wrappers to row-level equivalents."""
    raw = expression.strip()
    agg_match = AGG_WRAPPER_PATTERN.match(raw)
    if agg_match:
        inner = agg_match.group(2).strip()
        raw = inner or "NULL"

    count_match = COUNT_PATTERN.match(raw)
    if count_match:
        inner = count_match.group(1).strip()
        if inner == "*":
            raw = "1"
        else:
            if inner.lower().startswith("distinct "):
                inner = inner[9:].strip()
            if not inner:
                raw = "1"
            else:
                raw = f"CASE WHEN {inner} IS NULL THEN 0 ELSE 1 END"

    return _normalize_string_comparisons(raw)


def _normalize_string_comparisons(expression: str) -> str:
    """Normalize quoted-string predicates to case-insensitive comparisons."""

    def replace_eq(match: re.Match[str]) -> str:
        col = match.group(1)
        lit = match.group(2)
        return f"LOWER({col}) = '{lit.lower()}'"

    normalized = EQ_STR_PATTERN.sub(replace_eq, expression)

    def replace_in(match: re.Match[str]) -> str:
        col = match.group(1)
        raw_values = match.group(2)
        parts = [part.strip() for part in raw_values.split(",") if part.strip()]
        string_values: list[str] = []
        for part in parts:
            if not (part.startswith("'") and part.endswith("'")):
                return match.group(0)
            string_values.append(part[1:-1].lower())
        rendered = ", ".join(f"'{value}'" for value in string_values)
        return f"LOWER({col}) IN ({rendered})"

    return IN_STR_PATTERN.sub(replace_in, normalized)


def _quote_table_ref(ref: str) -> str:
    parts = [part for part in _normalize_ref(ref).split(".") if part]
    return ".".join(f"`{part}`" for part in parts)


def _sanitize_identifier(raw: str) -> str:
    cleaned = []
    for char in raw:
        if char.isalnum() or char == "_":
            cleaned.append(char.lower())
        else:
            cleaned.append("_")
    value = "".join(cleaned).strip("_")
    if not value:
        value = "col"
    if value[0].isdigit():
        value = f"c_{value}"
    return value


def _unique_name(base: str, used_names: set[str]) -> str:
    if base not in used_names:
        return base
    idx = 2
    while f"{base}_{idx}" in used_names:
        idx += 1
    return f"{base}_{idx}"


def _normalize_ref(raw: str) -> str:
    return raw.replace("`", "").strip().lower()


def _normalize_expression(raw: str) -> str:
    return " ".join(raw.strip().lower().split())


def _normalize_sql_type(raw_type: str) -> str:
    value = raw_type.strip().upper()
    return value if value else "STRING"


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _quoted_join(names: list[str]) -> str:
    return ", ".join(f"`{name}`" for name in names)
