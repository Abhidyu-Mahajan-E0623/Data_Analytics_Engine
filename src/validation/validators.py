"""Hypothesis validation utilities."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Protocol

import orjson

from src.validation.schema_models import Hypothesis, parse_hypothesis_line_safe

SQL_REF_PATTERN = re.compile(r"([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)")
AGG_WRAPPER_PATTERN = re.compile(
    r"^\s*(sum|avg|count|min|max|stddev|variance)\s*\((.*)\)\s*$",
    flags=re.IGNORECASE | re.DOTALL,
)
BARE_COLUMN_PATTERN = re.compile(r"^[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*$")
NUMERIC_SQL_TYPES = (
    "tinyint",
    "smallint",
    "int",
    "integer",
    "bigint",
    "float",
    "double",
    "real",
    "decimal",
    "numeric",
)


class SQLExecutor(Protocol):
    """Protocol for SQL dry-run execution."""

    def execute(self, query: str, parameters: Any | None = None) -> None:
        """Execute SQL statement."""


@dataclass
class ParsedHypotheses:
    """Result of JSONL parsing."""

    hypotheses_by_id: dict[str, Hypothesis]
    parse_errors: list[str]


@dataclass
class _ColumnResolver:
    """Resolve bare column names to table.column references from context bundle."""

    table_to_columns: dict[str, set[str]]
    table_name_to_fqn: dict[str, list[str]]
    global_column_to_refs: dict[str, list[str]]


def parse_jsonl_hypotheses(
    jsonl_lines: list[str],
    context_bundle: dict[str, Any] | None = None,
) -> ParsedHypotheses:
    """Parse JSONL lines into typed models."""
    hypotheses: dict[str, Hypothesis] = {}
    parse_errors: list[str] = []
    resolver = _build_column_resolver(context_bundle)

    for index, line in enumerate(jsonl_lines, start=1):
        payload: dict[str, Any] | None = None
        try:
            parsed = orjson.loads(line)
            if isinstance(parsed, dict):
                payload = parsed
            else:
                parse_errors.append(f"line {index}: expected JSON object")
                continue
        except orjson.JSONDecodeError as exc:
            parse_errors.append(f"line {index}: invalid JSON ({exc})")
            continue

        normalized_payload = _normalize_hypothesis_payload(payload, resolver)
        normalized_line = orjson.dumps(normalized_payload).decode("utf-8")
        model, error = parse_hypothesis_line_safe(normalized_line)
        if error:
            parse_errors.append(f"line {index}: {error}")
            continue
        assert model is not None
        if model.hypothesis_id in hypotheses:
            parse_errors.append(f"line {index}: duplicate hypothesis_id {model.hypothesis_id}")
            continue
        hypotheses[model.hypothesis_id] = model

    return ParsedHypotheses(hypotheses_by_id=hypotheses, parse_errors=parse_errors)


def build_catalog_index(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Build normalized lookup sets from metadata snapshot."""
    table_refs: set[str] = set()
    column_refs: set[str] = set()
    pii_column_refs: set[str] = set()
    column_types: dict[str, str] = {}

    for table in snapshot.get("tables", []):
        catalog = str(table["catalog"]).lower()
        schema = str(table["schema"]).lower()
        table_name = str(table["table"]).lower()
        table_fq = f"{catalog}.{schema}.{table_name}"
        table_refs.update({table_fq, f"{schema}.{table_name}", table_name})

        for column in table.get("columns", []):
            col = str(column["name"]).lower()
            dtype = str(column.get("data_type", "string")).strip().lower()
            refs = {
                f"{table_name}.{col}",
                f"{schema}.{table_name}.{col}",
                f"{table_fq}.{col}",
            }
            column_refs.update(refs)
            for ref in refs:
                column_types[ref] = dtype
            if _is_pii(column):
                pii_column_refs.update(refs)

    return {
        "tables": table_refs,
        "columns": column_refs,
        "pii_columns": pii_column_refs,
        "column_types": column_types,
    }


def validate_hypothesis_set(
    hypotheses: dict[str, Hypothesis],
    catalog_index: dict[str, Any],
    sql_client: SQLExecutor | None = None,
) -> tuple[dict[str, Hypothesis], dict[str, list[str]]]:
    """Validate hypotheses and split valid/invalid outputs."""
    valid: dict[str, Hypothesis] = {}
    invalid: dict[str, list[str]] = {}

    for hypothesis_id in sorted(hypotheses):
        hypothesis = hypotheses[hypothesis_id]
        errors = validate_hypothesis(hypothesis, catalog_index, sql_client)
        if errors:
            invalid[hypothesis_id] = errors
            continue
        valid[hypothesis_id] = hypothesis
    return valid, invalid


def validate_hypothesis(
    hypothesis: Hypothesis,
    catalog_index: dict[str, Any],
    sql_client: SQLExecutor | None = None,
) -> list[str]:
    """Validate catalog references, PII restrictions and SQL dry-run."""
    errors: list[str] = []
    errors.extend(validate_supported_table_scope(hypothesis))
    errors.extend(validate_references_within_declared_tables(hypothesis))
    errors.extend(validate_supported_derived_sql(hypothesis))
    errors.extend(validate_derived_metric_contract(hypothesis))
    errors.extend(validate_catalog_existence(hypothesis, catalog_index))
    errors.extend(validate_derived_data_type_compatibility(hypothesis, catalog_index))
    errors.extend(validate_pii_exclusion(hypothesis, catalog_index))
    if sql_client:
        sql_errors = validate_sql_expressions(hypothesis, sql_client)
        errors.extend(sql_errors)
        if not sql_errors:
            errors.extend(validate_non_degenerate_derived_values(hypothesis, sql_client))
    return errors


def validate_supported_table_scope(hypothesis: Hypothesis) -> list[str]:
    """Keep hypotheses within one table to avoid unsafe implicit joins."""
    unique_tables = {_normalize_ref(table) for table in hypothesis.tables if _normalize_ref(table)}
    if len(unique_tables) <= 1:
        return []
    return [
        "Hypothesis must declare exactly one source table; multi-table hypotheses are not supported for metrics materialization."
    ]


def validate_references_within_declared_tables(hypothesis: Hypothesis) -> list[str]:
    """Ensure required and derived references stay within hypothesis.tables."""
    errors: list[str] = []
    declared_table_names = {
        _normalize_ref(table).split(".")[-1]
        for table in hypothesis.tables
        if _normalize_ref(table)
    }
    if not declared_table_names:
        errors.append("Hypothesis must declare at least one source table.")
        return errors

    for column in hypothesis.required_columns:
        normalized = _normalize_ref(column)
        if "." not in normalized:
            continue
        table_name = normalized.split(".")[0]
        if table_name not in declared_table_names:
            errors.append(
                f"Required column '{column}' must belong to one of declared tables: "
                f"{sorted(declared_table_names)}."
            )

    for derived in hypothesis.derived_columns:
        refs = SQL_REF_PATTERN.findall(derived.sql_expression)
        if len(declared_table_names) > 1 and not refs:
            errors.append(
                f"Derived expression '{derived.name}' must use table-qualified references when multiple tables are declared."
            )
        for ref in refs:
            table_name = _normalize_ref(ref).split(".")[0]
            if table_name not in declared_table_names:
                errors.append(
                    f"Derived expression '{derived.name}' references '{ref}', "
                    f"expected only declared tables: {sorted(declared_table_names)}."
                )
    return errors


def validate_supported_derived_sql(hypothesis: Hypothesis) -> list[str]:
    """Reject known Databricks-incompatible or unstable derived SQL patterns."""
    errors: list[str] = []
    for derived in hypothesis.derived_columns:
        expr = derived.sql_expression
        normalized = expr.lower()
        if _is_aggregate_wrapper(expr):
            errors.append(
                f"Derived expression '{derived.name}' uses aggregate wrapper SQL; provide row-level SQL instead."
            )
        if " over " in normalized:
            errors.append(
                f"Derived expression '{derived.name}' uses window function OVER which is not supported for metrics materialization."
            )
        if " limit " in normalized:
            errors.append(
                f"Derived expression '{derived.name}' uses LIMIT which is not allowed in expression context."
            )
        if re.search(r"\brow_number\s*\(", normalized):
            errors.append(
                f"Derived expression '{derived.name}' uses ROW_NUMBER which is not supported for metrics materialization."
            )
        if re.search(r"\brank\s*\(", normalized) or re.search(r"\bdense_rank\s*\(", normalized):
            errors.append(
                f"Derived expression '{derived.name}' uses ranking functions which are not supported for metrics materialization."
            )
        if "over" in normalized and re.search(r"count\s*\(\s*distinct", normalized):
            errors.append(
                f"Derived expression '{derived.name}' uses COUNT DISTINCT with OVER, unsupported in Databricks SQL."
            )
    return errors


def validate_derived_metric_contract(hypothesis: Hypothesis) -> list[str]:
    """Enforce non-trivial numeric derived metrics for downstream evaluation."""
    errors: list[str] = []
    required_refs = {_normalize_ref(col) for col in hypothesis.required_columns}
    for derived in hypothesis.derived_columns:
        if _is_bare_column_expression(derived.sql_expression):
            errors.append(
                f"Derived expression '{derived.name}' is a direct column passthrough; use a transformed metric expression."
            )
        if not _is_numeric_sql_type(derived.data_type):
            errors.append(
                f"Derived column '{derived.name}' data_type '{derived.data_type}' must be numeric (INT/BIGINT/DOUBLE/DECIMAL/FLOAT)."
            )
        derived_refs = {_normalize_ref(ref) for ref in SQL_REF_PATTERN.findall(derived.sql_expression)}
        if len(required_refs) >= 2 and len(required_refs.intersection(derived_refs)) < 2:
            errors.append(
                f"Derived expression '{derived.name}' must use at least two required columns when multiple required_columns are declared."
            )
    return errors


def validate_derived_data_type_compatibility(
    hypothesis: Hypothesis,
    catalog_index: dict[str, Any],
) -> list[str]:
    """For simple column-based expressions, ensure declared type family matches source metadata type."""
    errors: list[str] = []
    type_lookup = catalog_index.get("column_types", {})
    if not isinstance(type_lookup, dict):
        return errors
    for derived in hypothesis.derived_columns:
        bare_ref = _extract_bare_column_ref(derived.sql_expression)
        if not bare_ref:
            continue
        source_type = str(type_lookup.get(_normalize_ref(bare_ref), "")).strip().lower()
        declared_type = str(derived.data_type).strip().lower()
        if source_type and declared_type and not _same_type_family(source_type, declared_type):
            errors.append(
                f"Derived column '{derived.name}' data_type '{derived.data_type}' does not match source column type '{source_type}'."
            )
    return errors


def _is_aggregate_wrapper(expression: str) -> bool:
    normalized = expression.strip()
    if " over " in normalized.lower():
        return False
    return bool(AGG_WRAPPER_PATTERN.match(normalized))


def validate_catalog_existence(
    hypothesis: Hypothesis,
    catalog_index: dict[str, Any],
) -> list[str]:
    """Validate table and required column references exist in metadata."""
    errors: list[str] = []

    for table in hypothesis.tables:
        normalized = _normalize_ref(table)
        if normalized not in catalog_index["tables"]:
            errors.append(f"Unknown table reference: {table}")

    for column in hypothesis.required_columns:
        normalized = _normalize_ref(column)
        if normalized not in catalog_index["columns"]:
            errors.append(f"Unknown required column: {column}")

    return errors


def validate_pii_exclusion(hypothesis: Hypothesis, catalog_index: dict[str, Any]) -> list[str]:
    """Reject hypotheses that include PII columns in required or derived definitions."""
    pii_refs = catalog_index["pii_columns"]
    errors: list[str] = []

    for column in hypothesis.required_columns:
        if _normalize_ref(column) in pii_refs:
            errors.append(f"PII column is not allowed: {column}")

    for derived in hypothesis.derived_columns:
        for ref in SQL_REF_PATTERN.findall(derived.sql_expression):
            if _normalize_ref(ref) in pii_refs:
                errors.append(
                    f"Derived expression '{derived.name}' references PII column: {ref}"
                )
    return errors


def validate_sql_expressions(hypothesis: Hypothesis, sql_client: SQLExecutor) -> list[str]:
    """Dry-run SQL expressions using EXPLAIN + LIMIT 0."""
    errors: list[str] = []
    for derived in hypothesis.derived_columns:
        dry_run_sql = assemble_dry_run_sql(hypothesis, derived.sql_expression, derived.name)
        try:
            sql_client.execute(dry_run_sql)
        except Exception as exc:
            errors.append(f"SQL dry-run failed for {derived.name}: {exc}")
    return errors


def validate_non_degenerate_derived_values(
    hypothesis: Hypothesis,
    sql_client: SQLExecutor,
) -> list[str]:
    """Reject derived expressions that evaluate to near-constant output on sample rows."""
    fetch_one = getattr(sql_client, "fetch_one", None)
    if not callable(fetch_one):
        return []

    errors: list[str] = []
    for derived in hypothesis.derived_columns:
        source_table = _resolve_probe_table(hypothesis, derived.sql_expression)
        if not source_table:
            continue
        probe_sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN metric_value IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_rows,
            COUNT(DISTINCT metric_value) AS distinct_nonnull,
            SUM(CASE WHEN metric_value IN ('0', '0.0') THEN 1 ELSE 0 END) AS zero_like_rows,
            SUM(CASE WHEN metric_value_numeric IS NOT NULL THEN 1 ELSE 0 END) AS numeric_rows
        FROM (
            SELECT
                CAST(({derived.sql_expression}) AS STRING) AS metric_value,
                TRY_CAST(({derived.sql_expression}) AS DOUBLE) AS metric_value_numeric
            FROM {source_table}
            LIMIT 5000
        ) probe
        """
        try:
            stats = fetch_one(probe_sql) or {}
        except Exception:
            continue
        if _has_insufficient_numeric_coverage(stats):
            errors.append(
                f"Derived expression '{derived.name}' is not reliably numeric on sample rows; ensure numeric output."
            )
            continue
        if _is_degenerate_metric_stats(stats):
            errors.append(
                f"Derived expression '{derived.name}' appears degenerate on sample data; avoid constant/all-zero output."
            )
    return errors


def _is_degenerate_metric_stats(stats: dict[str, Any]) -> bool:
    nonnull = int(stats.get("nonnull_rows", 0) or 0)
    distinct = int(stats.get("distinct_nonnull", 0) or 0)
    zero_like = int(stats.get("zero_like_rows", 0) or 0)
    if nonnull < 25:
        return False
    if distinct <= 1:
        return True
    if zero_like == nonnull:
        return True
    return False


def _has_insufficient_numeric_coverage(stats: dict[str, Any]) -> bool:
    nonnull = int(stats.get("nonnull_rows", 0) or 0)
    numeric_rows = int(stats.get("numeric_rows", 0) or 0)
    if nonnull < 25:
        return False
    return numeric_rows < max(5, int(nonnull * 0.70))


def _resolve_probe_table(hypothesis: Hypothesis, expression: str) -> str | None:
    refs = SQL_REF_PATTERN.findall(expression)
    if refs:
        ref_table_name = _normalize_ref(refs[0]).split(".")[0]
        for table in hypothesis.tables:
            normalized = _normalize_ref(table)
            if normalized.split(".")[-1] == ref_table_name:
                return table
    if len(hypothesis.tables) == 1:
        return hypothesis.tables[0]
    return None


def assemble_dry_run_sql(
    hypothesis: Hypothesis,
    expression: str,
    alias: str = "metric_value",
) -> str:
    """Build safe dry-run SQL for a derived expression."""
    table_clause = _build_from_clause(hypothesis.tables)
    return f"EXPLAIN SELECT {expression} AS `{alias}` {table_clause} LIMIT 0"


def _build_from_clause(tables: list[str]) -> str:
    if not tables:
        return ""
    cleaned = [table.strip() for table in tables if table.strip()]
    if not cleaned:
        return ""
    return "FROM " + " CROSS JOIN ".join(cleaned)


def _normalize_ref(raw: str) -> str:
    return raw.replace("`", "").strip().lower()


def _strip_outer_parentheses(expression: str) -> str:
    value = expression.strip()
    while value.startswith("(") and value.endswith(")"):
        depth = 0
        balanced = True
        for idx, ch in enumerate(value):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    balanced = False
                    break
                if depth == 0 and idx != len(value) - 1:
                    balanced = False
                    break
        if not balanced or depth != 0:
            break
        value = value[1:-1].strip()
    return value


def _extract_bare_column_ref(expression: str) -> str | None:
    normalized = _normalize_ref(_strip_outer_parentheses(expression))
    if BARE_COLUMN_PATTERN.match(normalized):
        return normalized
    return None


def _is_bare_column_expression(expression: str) -> bool:
    return _extract_bare_column_ref(expression) is not None


def _is_numeric_sql_type(raw_type: str) -> bool:
    value = raw_type.strip().lower()
    if not value:
        return False
    if value.startswith("decimal") or value.startswith("numeric"):
        return True
    return value in NUMERIC_SQL_TYPES


def _type_family(raw_type: str) -> str:
    value = raw_type.strip().lower()
    if not value:
        return "unknown"
    if value.startswith("decimal") or value.startswith("numeric"):
        return "numeric"
    if value in NUMERIC_SQL_TYPES:
        return "numeric"
    if value in {"string", "varchar", "char", "text"}:
        return "string"
    if value in {"date"}:
        return "date"
    if value.startswith("timestamp") or value == "datetime":
        return "timestamp"
    if value in {"boolean", "bool"}:
        return "boolean"
    return value


def _same_type_family(left: str, right: str) -> bool:
    return _type_family(left) == _type_family(right)


def _is_pii(column: dict[str, Any]) -> bool:
    if column.get("pii") is True:
        return True
    tag_value = str(column.get("tags", {}).get("pii", "false")).lower()
    return tag_value == "true"


def _build_column_resolver(context_bundle: dict[str, Any] | None) -> _ColumnResolver | None:
    if not context_bundle:
        return None
    tables = context_bundle.get("tables", [])
    if not isinstance(tables, list):
        return None

    table_to_columns: dict[str, set[str]] = {}
    table_name_to_fqn: dict[str, list[str]] = {}
    global_column_to_refs: dict[str, list[str]] = {}

    for table in tables:
        if not isinstance(table, dict):
            continue
        fqn_raw = str(table.get("fqn", "")).strip()
        fqn = _normalize_ref(fqn_raw)
        if not fqn:
            continue
        table_name = fqn.split(".")[-1]
        table_name_to_fqn.setdefault(table_name, [])
        if fqn not in table_name_to_fqn[table_name]:
            table_name_to_fqn[table_name].append(fqn)

        columns = table.get("columns", [])
        for col in columns if isinstance(columns, list) else []:
            if not isinstance(col, dict):
                continue
            col_name = _normalize_ref(str(col.get("name", "")))
            if not col_name:
                continue
            table_to_columns.setdefault(table_name, set()).add(col_name)
            ref = f"{table_name}.{col_name}"
            global_column_to_refs.setdefault(col_name, [])
            if ref not in global_column_to_refs[col_name]:
                global_column_to_refs[col_name].append(ref)

    if not table_to_columns:
        return None
    return _ColumnResolver(
        table_to_columns=table_to_columns,
        table_name_to_fqn=table_name_to_fqn,
        global_column_to_refs=global_column_to_refs,
    )


def _normalize_hypothesis_payload(
    payload: dict[str, Any],
    resolver: _ColumnResolver | None,
) -> dict[str, Any]:
    normalized = dict(payload)

    raw_tables = payload.get("tables")
    normalized_tables = _normalize_tables(raw_tables, resolver)
    normalized["tables"] = normalized_tables

    raw_required = payload.get("required_columns")
    normalized["required_columns"] = _normalize_required_columns(
        raw_required,
        normalized_tables,
        resolver,
    )
    return normalized


def _normalize_tables(
    raw_tables: Any,
    resolver: _ColumnResolver | None,
) -> list[str]:
    if not isinstance(raw_tables, list):
        return []
    normalized_tables: list[str] = []
    for table in raw_tables:
        cleaned = _normalize_ref(str(table))
        if not cleaned:
            continue
        # If only table name is supplied, resolve to the unique known FQN.
        if "." not in cleaned and resolver:
            candidates = resolver.table_name_to_fqn.get(cleaned, [])
            if len(candidates) == 1:
                cleaned = candidates[0]
        normalized_tables.append(cleaned)
    return normalized_tables


def _normalize_required_columns(
    raw_required_columns: Any,
    tables: list[str],
    resolver: _ColumnResolver | None,
) -> list[str]:
    if not isinstance(raw_required_columns, list):
        return []

    table_names = [table.split(".")[-1] for table in tables if table]
    resolved: list[str] = []
    seen: set[str] = set()

    for value in raw_required_columns:
        raw = _normalize_ref(str(value))
        if not raw:
            continue

        candidate = raw
        if "." not in raw:
            matches: list[str] = []

            # Prefer columns that exist in explicitly selected tables.
            if resolver:
                for table_name in table_names:
                    columns = resolver.table_to_columns.get(table_name, set())
                    if raw in columns:
                        matches.append(f"{table_name}.{raw}")
            elif len(table_names) == 1:
                matches.append(f"{table_names[0]}.{raw}")

            # Fallback to global unique column mapping.
            if not matches and resolver:
                global_matches = resolver.global_column_to_refs.get(raw, [])
                if len(global_matches) == 1:
                    matches.extend(global_matches)

            if len(matches) == 1:
                candidate = matches[0]

        if candidate not in seen:
            seen.add(candidate)
            resolved.append(candidate)
    return resolved
