"""Insight generation pipeline.

Generates business insights from validated hypotheses and their
Databricks metrics tables, using Azure OpenAI GPT-4o for narrative
generation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config.settings import Settings
from src.connectors.databricks_sql import DatabricksSQLClient
from src.pipeline.persist import load_validated_hypotheses
from src.utils.io import (
    OUTPUT_DIR,
    OUTPUT_REPORTS_DIR,
    atomic_write_text,
    ensure_project_dirs,
)
from src.utils.logging import configure_logging
from src.utils.time import utc_iso
from src.validation.schema_models import Hypothesis


OUTPUT_INSIGHT_DIR = OUTPUT_DIR / "Insight"

# ---------------------------------------------------------------------------
# System prompt for GPT-4o insight generation
# ---------------------------------------------------------------------------

INSIGHT_SYSTEM_PROMPT = """\
You are a pharma business analyst who generates clear, actionable insights
for non-technical executives from data.

You will receive a JSON list of insight items. Each item contains:
- "id": sequential number
- "hypothesis": the original business hypothesis statement
- "notes": hypothesis notes
- "table": source table name
- "columns": columns used
- "metrics": aggregated metric statistics from the data

For EACH item, return:
1. "headline": A catchy but professional headline (e.g., "High-Value Specialties Fuel Sales")
2. "body": A 2-3 sentence narrative in plain English explaining the finding.
   Include actual numbers naturally. Write as if explaining to a business leader.
3. "reasoning": A brief explanation of how the numbers were derived/calculated.
4. "key_metrics": A dictionary of the most important metric names and their values.

Rules:
- Tone: Clear, human, conversational. No jargon, no variable names, no code.
- Use only the data provided. Never invent or hallucinate numbers.
- Include actual metric values (percentages, counts, averages) naturally in the text.
- If a metric is zero or null, say "no activity" rather than "0%".
- For small changes (<1%), say "marginally" rather than citing exact number.
- Keep headlines punchy but professional. No clickbait words like "skyrockets" or "plummets".

Return a JSON object with this exact structure:
{"results": [{"id": 0, "headline": "...", "body": "...", "reasoning": "...", "key_metrics": {"metric": value}}]}
"""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class InsightItem:
    """One generated insight for the report."""
    hypothesis_id: str
    hypothesis_statement: str
    headline: str
    body: str
    reasoning: str
    key_metrics: dict[str, Any]


@dataclass
class InsightResult:
    """Summary of insight generation run."""
    run_id: str
    insight_count: int
    output_path: str
    insights: list[InsightItem]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_insight_generation(
    settings: Settings,
    run_id: str,
    selected_ids: list[int],
    logger: logging.Logger | None = None,
) -> InsightResult:
    """Generate insights for the selected hypotheses from the given run.

    Args:
        settings: Application settings with Databricks + Azure OpenAI config.
        run_id: The hypothesis run ID to load from.
        selected_ids: User-selected hypothesis numbers (e.g. [1, 4, 5, 6]).
        logger: Optional logger.

    Returns:
        InsightResult with generated insights and output path.
    """
    ensure_project_dirs()
    OUTPUT_INSIGHT_DIR.mkdir(parents=True, exist_ok=True)
    logger = logger or configure_logging(run_id=run_id)

    # --- Step 1: Load hypotheses ---
    logger.info(
        "[insight] Step 1: Loading hypotheses from run %s",
        run_id,
        extra={"run_id": run_id, "module": "insight", "step": "load_hypotheses"},
    )
    all_hypotheses = load_validated_hypotheses(run_id)
    if not all_hypotheses:
        raise ValueError(f"No validated hypotheses found for run_id={run_id}")

    # --- Step 2: Filter to selected IDs ---
    selected_hids = {f"H{i:02d}" for i in selected_ids}
    selected = [h for h in all_hypotheses if h.hypothesis_id in selected_hids]
    if not selected:
        available = [h.hypothesis_id for h in all_hypotheses]
        raise ValueError(
            f"None of the selected hypotheses {sorted(selected_hids)} found. "
            f"Available: {available}"
        )
    logger.info(
        "[insight] Step 2: Filtered to %d selected hypotheses: %s",
        len(selected), [h.hypothesis_id for h in selected],
        extra={"run_id": run_id, "module": "insight", "step": "filter_hypotheses"},
    )

    # --- Step 3: Fetch metric data from Databricks ---
    sql_client = DatabricksSQLClient(settings=settings, logger=logger)
    catalog = settings.DATABRICKS_CATALOG
    schema = settings.DATABRICKS_SCHEMA_MONITORING

    payloads: list[dict[str, Any]] = []
    for idx, hypothesis in enumerate(selected):
        logger.info(
            "[insight] Step 3: Fetching metric data for %s (%d/%d)",
            hypothesis.hypothesis_id, idx + 1, len(selected),
            extra={"run_id": run_id, "module": "insight", "step": f"fetch_metrics_{hypothesis.hypothesis_id}"},
        )
        metric_table = _find_metric_table(sql_client, catalog, schema, hypothesis)
        if metric_table:
            stats = _fetch_metric_stats(sql_client, metric_table)
        else:
            stats = {"note": "No metrics table found; using hypothesis definition only."}
            logger.warning(
                "[insight] No metric table found for %s in %s.%s",
                hypothesis.hypothesis_id, catalog, schema,
                extra={"run_id": run_id, "module": "insight", "step": "metric_missing"},
            )

        payloads.append({
            "id": idx,
            "hypothesis": hypothesis.statement,
            "notes": hypothesis.notes,
            "table": ", ".join(hypothesis.tables),
            "columns": hypothesis.required_columns,
            "derived_columns": [
                {"name": d.name, "expression": d.sql_expression}
                for d in hypothesis.derived_columns
            ],
            "threshold": {
                "type": hypothesis.threshold.type,
                "value": hypothesis.threshold.value,
                "direction": hypothesis.threshold.direction,
            },
            "metrics": stats,
        })

    # --- Step 4: Call LLM for insight generation ---
    logger.info(
        "[insight] Step 4: Calling Azure OpenAI for insight generation (%d payloads)",
        len(payloads),
        extra={"run_id": run_id, "module": "insight", "step": "llm_call"},
    )
    llm_results = _call_insight_llm(settings, payloads, logger)

    # --- Step 5: Build insight items ---
    logger.info(
        "[insight] Step 5: Building %d insight items",
        len(selected),
        extra={"run_id": run_id, "module": "insight", "step": "build_items"},
    )
    insights: list[InsightItem] = []
    for idx, hypothesis in enumerate(selected):
        result_data = llm_results.get(idx, {})
        insights.append(InsightItem(
            hypothesis_id=hypothesis.hypothesis_id,
            hypothesis_statement=hypothesis.statement,
            headline=result_data.get("headline", hypothesis.statement),
            body=result_data.get("body", "Insight generation pending."),
            reasoning=result_data.get("reasoning", ""),
            key_metrics=result_data.get("key_metrics", {}),
        ))

    # --- Step 6: Render and save report ---
    report_content = _render_insight_report(run_id, insights)
    output_path = OUTPUT_INSIGHT_DIR / "Insight.txt"
    atomic_write_text(output_path, report_content + "\n")
    logger.info(
        "[insight] Step 6: Insight report saved to %s (%d insights)",
        output_path, len(insights),
        extra={"run_id": run_id, "module": "insight", "step": "save_report"},
    )

    return InsightResult(
        run_id=run_id,
        insight_count=len(insights),
        output_path=str(output_path),
        insights=insights,
    )


# ---------------------------------------------------------------------------
# Metric table discovery & querying
# ---------------------------------------------------------------------------

def _find_metric_table(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    hypothesis: Hypothesis,
) -> str | None:
    """Find the metric_* table for a given hypothesis."""
    hid = hypothesis.hypothesis_id.lower()
    try:
        rows = sql_client.fetch_all(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    except Exception:
        return None

    candidates: list[str] = []
    for row in rows:
        name = str(row.get("tablename") or row.get("table_name") or "").strip().lower()
        if name.startswith(f"metric_{hid}"):
            candidates.append(name)

    if not candidates:
        return None

    # If multiple, pick the first one alphabetically
    candidates.sort()
    return f"`{catalog}`.`{schema}`.`{candidates[0]}`"


def _fetch_metric_stats(sql_client: DatabricksSQLClient, table_fqn: str) -> dict[str, Any]:
    """Fetch aggregate statistics and sample rows from a metrics table."""
    stats: dict[str, Any] = {}

    # Get columns
    try:
        desc_rows = sql_client.fetch_all(f"DESCRIBE TABLE {table_fqn}")
    except Exception:
        return {"error": "Could not describe table"}

    columns: list[dict[str, str]] = []
    for row in desc_rows:
        col_name = str(row.get("col_name", "")).strip()
        data_type = str(row.get("data_type", "")).strip()
        if col_name and not col_name.startswith("#") and col_name not in (
            "run_id", "domain", "focus_areas", "source_table"
        ):
            columns.append({"name": col_name, "type": data_type})

    stats["columns"] = [c["name"] for c in columns]

    # Count
    try:
        count_row = sql_client.fetch_one(f"SELECT COUNT(*) AS cnt FROM {table_fqn}")
        stats["total_rows"] = int((count_row or {}).get("cnt", 0))
    except Exception:
        stats["total_rows"] = 0

    # Aggregates for numeric columns
    numeric_types = {"int", "bigint", "float", "double", "decimal", "numeric", "tinyint", "smallint"}
    numeric_cols = [c for c in columns if any(t in c["type"].lower() for t in numeric_types)]

    if numeric_cols:
        agg_parts = []
        for col in numeric_cols:
            cn = col["name"]
            agg_parts.extend([
                f"ROUND(AVG(CAST(`{cn}` AS DOUBLE)), 2) AS `avg_{cn}`",
                f"ROUND(SUM(CAST(`{cn}` AS DOUBLE)), 2) AS `sum_{cn}`",
                f"ROUND(MIN(CAST(`{cn}` AS DOUBLE)), 2) AS `min_{cn}`",
                f"ROUND(MAX(CAST(`{cn}` AS DOUBLE)), 2) AS `max_{cn}`",
                f"COUNT(`{cn}`) AS `count_{cn}`",
            ])
        agg_query = f"SELECT {', '.join(agg_parts)} FROM {table_fqn}"
        try:
            agg_row = sql_client.fetch_one(agg_query)
            if agg_row:
                aggregates: dict[str, dict[str, Any]] = {}
                for col in numeric_cols:
                    cn = col["name"]
                    aggregates[cn] = {
                        "avg": _safe_number(agg_row.get(f"avg_{cn}")),
                        "sum": _safe_number(agg_row.get(f"sum_{cn}")),
                        "min": _safe_number(agg_row.get(f"min_{cn}")),
                        "max": _safe_number(agg_row.get(f"max_{cn}")),
                        "count": _safe_number(agg_row.get(f"count_{cn}")),
                    }
                stats["numeric_aggregates"] = aggregates
        except Exception:
            pass

    # String column distinct counts
    string_cols = [c for c in columns if "string" in c["type"].lower()]
    if string_cols:
        distinct_parts = [
            f"COUNT(DISTINCT `{c['name']}`) AS `distinct_{c['name']}`"
            for c in string_cols
        ]
        try:
            dist_row = sql_client.fetch_one(
                f"SELECT {', '.join(distinct_parts)} FROM {table_fqn}"
            )
            if dist_row:
                stats["string_distinct_counts"] = {
                    c["name"]: _safe_number(dist_row.get(f"distinct_{c['name']}"))
                    for c in string_cols
                }
        except Exception:
            pass

    # Sample rows (5 rows)
    try:
        sample_rows = sql_client.fetch_all(f"SELECT * FROM {table_fqn} LIMIT 5")
        # Convert to serializable
        clean_samples = []
        for row in sample_rows:
            clean_row = {}
            for k, v in row.items():
                if k in ("run_id", "domain", "focus_areas", "source_table"):
                    continue
                clean_row[k] = _safe_number(v) if v is not None else None
            clean_samples.append(clean_row)
        stats["sample_rows"] = clean_samples
    except Exception:
        pass

    return stats


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def _call_insight_llm(
    settings: Settings,
    payloads: list[dict[str, Any]],
    logger: logging.Logger | None = None,
) -> dict[int, dict[str, Any]]:
    """Send batch insight prompt to Azure OpenAI and parse the response."""
    deployment = settings.AZURE_OPENAI_CHAT_DEPLOYMENT
    url = (
        f"{settings.AZURE_OPENAI_ENDPOINT}/openai/deployments/{deployment}/chat/completions"
        f"?api-version={settings.AZURE_OPENAI_API_VERSION}"
    )
    headers = {
        "Content-Type": "application/json",
        "api-key": settings.AZURE_OPENAI_API_KEY,
    }

    body = {
        "messages": [
            {"role": "system", "content": INSIGHT_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Generate insights for these hypothesis items. "
                    "Return VALID JSON only:\n"
                    + json.dumps(payloads, indent=2, default=str)
                ),
            },
        ],
        "temperature": 0.3,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
    }

    try:
        response = requests.post(url, headers=headers, json=body, timeout=120)
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]

        # Clean markdown fences if present
        if content.startswith("```json"):
            content = content.replace("```json", "").replace("```", "")
        elif content.startswith("```"):
            content = content.replace("```", "")

        parsed = json.loads(content)
        results = parsed.get("results", [])
        return {item["id"]: item for item in results if "id" in item}

    except Exception as exc:
        if logger:
            logger.error("Insight LLM call failed: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def _render_insight_report(run_id: str, insights: list[InsightItem]) -> str:
    """Build the formatted Insight.txt report."""
    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("OVERALL INSIGHTS -- DETAILED CALCULATIONS")
    lines.append("=" * 80)
    lines.append("")

    for idx, item in enumerate(insights, 1):
        lines.append("-" * 80)
        lines.append(f"INSIGHT {idx}: {item.headline}")
        lines.append("-" * 80)
        lines.append("")
        lines.append("Hypothesis Used:")
        lines.append(f"  {item.hypothesis_statement}")
        lines.append("")
        lines.append("Template Output:")
        lines.append(f"  {idx}. {item.headline}")
        # Wrap body text at ~76 chars with indent
        body_lines = _wrap_text(item.body, width=76, indent="  ")
        for bl in body_lines:
            lines.append(bl)
        lines.append("")
        if item.reasoning:
            lines.append("Reasoning & Calculation:")
            reasoning_lines = _wrap_text(item.reasoning, width=76, indent="  ")
            for rl in reasoning_lines:
                lines.append(rl)
            lines.append("")
        if item.key_metrics:
            lines.append("Key Metrics:")
            for metric_name, metric_value in item.key_metrics.items():
                display_name = metric_name.replace("_", " ").replace("-", " ")
                if isinstance(metric_value, float):
                    lines.append(f"  {display_name}: {metric_value:,.2f}")
                else:
                    lines.append(f"  {display_name}: {metric_value}")
            lines.append("")
        lines.append("")

    lines.append("=" * 80)
    lines.append("END OF REPORT")
    lines.append("=" * 80)

    return "\n".join(lines)


def _wrap_text(text: str, width: int = 76, indent: str = "  ") -> list[str]:
    """Simple word-wrap for report text."""
    words = text.split()
    result_lines: list[str] = []
    current_line = indent

    for word in words:
        if len(current_line) + len(word) + 1 > width + len(indent):
            result_lines.append(current_line)
            current_line = indent + word
        else:
            if current_line == indent:
                current_line += word
            else:
                current_line += " " + word
    if current_line.strip():
        result_lines.append(current_line)

    return result_lines if result_lines else [indent + text]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_number(value: Any) -> Any:
    """Convert to a JSON-safe number."""
    if value is None:
        return None
    try:
        f = float(value)
        if f == int(f) and abs(f) < 1e15:
            return int(f)
        return round(f, 2)
    except (TypeError, ValueError):
        return str(value)


def get_latest_run_id() -> str | None:
    """Read the latest run_id from Output/reports/latest_run_id.txt."""
    path = OUTPUT_REPORTS_DIR / "latest_run_id.txt"
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8").strip()


def list_hypotheses_for_run(run_id: str) -> list[dict[str, str]]:
    """List hypothesis IDs and statements for a run (for display)."""
    try:
        hypotheses = load_validated_hypotheses(run_id)
        return [
            {"id": h.hypothesis_id, "statement": h.statement}
            for h in hypotheses
        ]
    except Exception:
        return []
