"""Prompt templates for generation and repair."""

from __future__ import annotations

from typing import Any

import orjson

SYSTEM_PROMPT = """
You are an analytics strategist creating falsifiable business hypotheses.

Rules:
1) Use only the provided schema context.
2) If required data is missing, set requires_new_source=true.
3) Produce exactly 10 hypotheses in IDs H01..H10.
4) Avoid PII and ignore columns tagged pii=true.
5) Return strict JSON only (no markdown, no prose outside JSON).
6) required_columns MUST be table.column strings from provided context, never bare column names.
7) tables values MUST be valid fully qualified table names from provided context.
8) Every sql_expression must reference only allowed columns from context; do not invent columns or tables.
9) Focus strictly on the provided focus_areas. Do not generate unrelated hypotheses.
10) If focus_areas contain multiple themes, hypotheses may span those themes but must remain inside them.
11) Each hypothesis must use exactly one source table.
12) Prefer bronze-layer tables first when suitable (schema/table names containing bronze).
13) Avoid window functions (OVER), ranking functions (ROW_NUMBER/RANK/DENSE_RANK), and LIMIT inside derived sql_expression.
14) Keep derived sql_expression compatible with simple SELECT execution in Databricks.
15) Derived sql_expression must be row-level (no SUM/AVG/COUNT/MIN/MAX wrappers).
16) Derived sql_expression must not be a direct passthrough like table.column; apply a meaningful transformation (CASE/arithmetic/date-diff/cast with logic).
17) Every derived_columns[*].data_type must be numeric (INT/BIGINT/DOUBLE/DECIMAL/FLOAT) because evaluation computes numeric aggregates.
18) If required_columns has 2+ entries, each derived sql_expression must reference at least two of those required_columns.

Output format (strict JSON object):
{
  "human_text": "human-readable report",
  "jsonl": "one valid JSON hypothesis object per line, exactly 10 lines"
}

Each JSONL object must include exactly:
- hypothesis_id (H01..H10)
- statement
- tables (array of fully-qualified table names)
- required_columns (array of table.column strings)
- derived_columns (array of {name, sql_expression, data_type})
- window (e.g. 7d, 28d)
- granularity (daily|weekly)
- threshold ({type, value or values, direction})
- priority (P1|P2|P3)
- notes
- requires_new_source (boolean)
""".strip()


def build_generation_messages(
    domain: str,
    context_bundle: dict[str, Any],
    focus_areas: list[str] | None = None,
    business_constraints: str | None = None,
    table_assignment_plan: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    """Build chat messages for first-pass hypothesis generation."""
    constraints = business_constraints or "None provided"
    resolved_focus = [item.strip().lower() for item in (focus_areas or []) if item.strip()]
    if not resolved_focus:
        resolved_focus = [domain.strip().lower()]
    allowed_tables, allowed_columns = _allowed_references(context_bundle)
    user_payload = {
        "task": (
            "Generate 10 hypotheses for the domain dataset with strong focus alignment. "
            "Use focus_areas to constrain topic intent."
        ),
        "domain": domain,
        "focus_areas": resolved_focus,
        "business_constraints": constraints,
        "generation_constraints": {
            "single_table_per_hypothesis": True,
            "prefer_bronze_layer": True,
        },
        "allowed_tables": allowed_tables,
        "table_assignment_plan": table_assignment_plan or {},
        "preferred_bronze_tables": _preferred_bronze_tables(allowed_tables),
        "allowed_column_references": allowed_columns,
        "context_bundle": context_bundle,
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": orjson.dumps(user_payload, option=orjson.OPT_INDENT_2).decode()},
    ]


def build_repair_messages(
    domain: str,
    context_bundle: dict[str, Any],
    focus_areas: list[str] | None,
    validation_errors: dict[str, list[str]],
    existing_valid_hypotheses: list[dict[str, Any]],
    business_constraints: str | None = None,
    table_assignment_plan: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    """Build chat messages to repair invalid hypotheses while keeping stable IDs."""
    constraints = business_constraints or "None provided"
    resolved_focus = [item.strip().lower() for item in (focus_areas or []) if item.strip()]
    if not resolved_focus:
        resolved_focus = [domain.strip().lower()]
    allowed_tables, allowed_columns = _allowed_references(context_bundle)
    repair_instructions = {
        "task": (
            "Repair only invalid hypotheses. Keep hypothesis_id stable. "
            "Return JSON with keys human_text and jsonl where jsonl contains only fixed hypotheses."
        ),
        "strict_rules": [
            "required_columns must be table.column (never bare column names).",
            "each hypothesis must use exactly one source table.",
            "follow table_assignment_plan exactly: each hypothesis_id must use only its assigned table.",
            "use only columns/tables present in context_bundle.",
            "all returned rows must include valid hypothesis_id H01..H10.",
            "derived sql_expression may only reference allowed_column_references.",
            "keep hypotheses strictly aligned to focus_areas.",
            "all required_columns and derived sql_expression references must stay inside the declared table.",
            "prefer bronze-layer tables where possible.",
            "avoid OVER/ROW_NUMBER/RANK/DENSE_RANK/LIMIT in derived sql_expression.",
            "derived sql_expression must be row-level and must not be wrapped in SUM/AVG/COUNT/MIN/MAX.",
            "derived sql_expression must not be a direct passthrough like table.column; apply transformation logic.",
            "derived_columns[*].data_type must be numeric (INT/BIGINT/DOUBLE/DECIMAL/FLOAT).",
            "if required_columns has 2+ entries, each derived sql_expression must reference at least two required_columns.",
        ],
        "domain": domain,
        "focus_areas": resolved_focus,
        "business_constraints": constraints,
        "allowed_tables": allowed_tables,
        "table_assignment_plan": table_assignment_plan or {},
        "preferred_bronze_tables": _preferred_bronze_tables(allowed_tables),
        "allowed_column_references": allowed_columns,
        "invalid_hypotheses_with_errors": validation_errors,
        "existing_valid_hypotheses": existing_valid_hypotheses,
        "context_bundle": context_bundle,
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": orjson.dumps(repair_instructions, option=orjson.OPT_INDENT_2).decode()},
    ]


def _allowed_references(context_bundle: dict[str, Any]) -> tuple[list[str], list[str]]:
    tables: list[str] = []
    cols: list[str] = []
    seen_tables: set[str] = set()
    seen_cols: set[str] = set()

    for table in context_bundle.get("tables", []):
        if not isinstance(table, dict):
            continue
        fqn = str(table.get("fqn", "")).replace("`", "").strip().lower()
        if fqn and fqn not in seen_tables:
            seen_tables.add(fqn)
            tables.append(fqn)

        table_name = fqn.split(".")[-1] if fqn else str(table.get("table", "")).strip().lower()
        for col in table.get("columns", []):
            if not isinstance(col, dict):
                continue
            col_name = str(col.get("name", "")).strip().lower()
            if not col_name or not table_name:
                continue
            ref = f"{table_name}.{col_name}"
            if ref not in seen_cols:
                seen_cols.add(ref)
                cols.append(ref)
    return tables, cols


def _preferred_bronze_tables(allowed_tables: list[str]) -> list[str]:
    bronze = []
    for table in allowed_tables:
        lowered = table.lower()
        if ".bronze." in lowered or ".bronze_" in lowered or "_bronze" in lowered:
            bronze.append(table)
    return bronze
