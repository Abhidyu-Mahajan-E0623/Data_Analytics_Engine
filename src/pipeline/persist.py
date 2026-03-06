"""Run artifact persistence helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson

from src.utils.io import (
    INPUT_METADATA_DIR,
    INPUT_RUNS_DIR,
    OUTPUT_REPORTS_DIR,
    atomic_write_json,
    atomic_write_text,
    ensure_project_dirs,
    read_jsonl,
    run_output_dir,
    write_jsonl,
)
from src.validation.schema_models import Hypothesis


def save_metadata_snapshot(run_id: str, snapshot: dict[str, Any]) -> Path:
    """Save metadata snapshot under Input/metadata."""
    ensure_project_dirs()
    path = INPUT_METADATA_DIR / f"metadata_snapshot_{run_id}.json"
    return atomic_write_json(path, snapshot)


def save_run_input(run_id: str, payload: dict[str, Any]) -> Path:
    """Persist run input payload under Input/runs."""
    ensure_project_dirs()
    path = INPUT_RUNS_DIR / f"run_input_{run_id}.json"
    return atomic_write_json(path, payload)


def save_context_bundle(run_id: str, context_bundle: dict[str, Any]) -> Path:
    """Persist context bundle used for LLM generation."""
    ensure_project_dirs()
    path = OUTPUT_REPORTS_DIR / f"context_bundle_{run_id}.json"
    return atomic_write_json(path, context_bundle)


def save_prompt_record(run_id: str, prompt_record: dict[str, Any]) -> Path:
    """Persist prompt and metadata for traceability."""
    ensure_project_dirs()
    path = OUTPUT_REPORTS_DIR / f"prompt_record_{run_id}.json"
    return atomic_write_json(path, prompt_record)


def save_hypothesis_artifacts(
    run_id: str,
    human_text: str,
    valid_hypotheses: list[Hypothesis],
    raw_jsonl_lines: list[str],
    validation_report: dict[str, Any],
    run_meta: dict[str, Any],
) -> dict[str, Path]:
    """Save run artifacts in Output/hypotheses and Output/reports."""
    ensure_project_dirs()
    output_dir = run_output_dir(run_id)

    raw_jsonl_path = output_dir / "hypotheses_raw.jsonl"
    atomic_write_text(raw_jsonl_path, "\n".join(raw_jsonl_lines).strip() + "\n")

    hypotheses_jsonl_path = output_dir / "hypotheses.jsonl"
    write_jsonl(hypotheses_jsonl_path, [model.model_dump() for model in valid_hypotheses])

    hypotheses_txt_path = output_dir / "hypotheses.txt"
    if valid_hypotheses:
        txt_content = render_hypotheses_text(
            valid_hypotheses,
            llm_summary=human_text.strip() or None,
        )
    else:
        txt_content = render_raw_hypotheses_text(
            raw_jsonl_lines=raw_jsonl_lines,
            llm_summary=human_text.strip() or None,
            validation_report=validation_report,
        )
    atomic_write_text(hypotheses_txt_path, txt_content + "\n")

    validation_in_run = output_dir / "validation_report.json"
    atomic_write_json(validation_in_run, validation_report)
    validation_global = OUTPUT_REPORTS_DIR / f"validation_report_{run_id}.json"
    atomic_write_json(validation_global, validation_report)

    run_meta_in_run = output_dir / "run_meta.json"
    atomic_write_json(run_meta_in_run, run_meta)
    run_meta_global = OUTPUT_REPORTS_DIR / f"run_meta_{run_id}.json"
    atomic_write_json(run_meta_global, run_meta)

    latest_run_id = OUTPUT_REPORTS_DIR / "latest_run_id.txt"
    atomic_write_text(latest_run_id, f"{run_id}\n")

    return {
        "output_dir": output_dir,
        "hypotheses_txt": hypotheses_txt_path,
        "hypotheses_jsonl": hypotheses_jsonl_path,
        "hypotheses_raw_jsonl": raw_jsonl_path,
        "validation_report_run": validation_in_run,
        "validation_report_global": validation_global,
        "run_meta_run": run_meta_in_run,
        "run_meta_global": run_meta_global,
        "latest_run_id": latest_run_id,
    }


def load_validated_hypotheses(run_id: str) -> list[Hypothesis]:
    """Load validated hypotheses from Output/hypotheses/<run_id>/hypotheses.jsonl."""
    path = run_output_dir(run_id) / "hypotheses.jsonl"
    rows = read_jsonl(path)
    return [Hypothesis.model_validate(item) for item in rows]


def load_validation_report(run_id: str) -> dict[str, Any]:
    """Read validation report for a run."""
    path = run_output_dir(run_id) / "validation_report.json"
    if not path.exists():
        return {}
    return orjson.loads(path.read_bytes())


def render_hypotheses_text(
    hypotheses: list[Hypothesis],
    llm_summary: str | None = None,
) -> str:
    """Build human-readable hypothesis report."""
    lines = ["Validated hypotheses", "===================="]
    if llm_summary:
        lines.append(f"LLM summary: {llm_summary}")
    lines.append(f"Total valid hypotheses: {len(hypotheses)}")
    lines.append("")

    for hypothesis in sorted(hypotheses, key=lambda item: item.hypothesis_id):
        lines.append(f"{hypothesis.hypothesis_id} [{hypothesis.priority}]")
        lines.append("-" * 32)
        lines.append(f"Statement      : {hypothesis.statement}")
        lines.append(f"Tables: {', '.join(hypothesis.tables)}")
        lines.append(f"Window         : {hypothesis.window}")
        lines.append(f"Granularity    : {hypothesis.granularity}")
        lines.append("Required cols  :")
        for column in hypothesis.required_columns:
            lines.append(f"  - {column}")
        if hypothesis.derived_columns:
            lines.append("Derived cols   :")
            for derived in hypothesis.derived_columns:
                lines.append(f"  - {derived.name} [{derived.data_type}]")
                lines.append(f"    SQL: {derived.sql_expression}")
        else:
            lines.append("Derived cols   : none")
        threshold_json = orjson.dumps(
            hypothesis.threshold.model_dump(exclude_none=True),
            option=orjson.OPT_INDENT_2,
        ).decode()
        lines.append("Threshold      :")
        for threshold_line in threshold_json.splitlines():
            lines.append(f"  {threshold_line}")
        lines.append(f"Needs source   : {hypothesis.requires_new_source}")
        lines.append(f"Notes          : {hypothesis.notes or '-'}")
        lines.append("")
    return "\n".join(lines).rstrip()


def render_raw_hypotheses_text(
    raw_jsonl_lines: list[str],
    llm_summary: str | None = None,
    validation_report: dict[str, Any] | None = None,
) -> str:
    """Build human-readable report from raw LLM hypotheses when none validated."""
    lines = ["LLM hypotheses (unvalidated)", "==========================="]
    if llm_summary:
        lines.append(f"LLM summary: {llm_summary}")
    lines.append(f"Raw hypothesis lines: {len(raw_jsonl_lines)}")

    valid_count = int((validation_report or {}).get("valid_count", 0))
    invalid_count = int((validation_report or {}).get("invalid_count", 0))
    lines.append(f"Validation result: valid={valid_count}, invalid={invalid_count}")
    lines.append("")

    parsed_rows: list[dict[str, Any]] = []
    for line in raw_jsonl_lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            row = orjson.loads(stripped)
        except orjson.JSONDecodeError:
            continue
        if isinstance(row, dict):
            parsed_rows.append(row)

    if not parsed_rows:
        lines.append("No parseable raw hypotheses found in hypotheses_raw.jsonl.")
        return "\n".join(lines).rstrip()

    # Keep one row per hypothesis_id if present, else preserve first-seen rows.
    seen_ids: set[str] = set()
    display_rows: list[dict[str, Any]] = []
    for row in parsed_rows:
        raw_id = str(row.get("hypothesis_id", "")).strip()
        if raw_id:
            if raw_id in seen_ids:
                continue
            seen_ids.add(raw_id)
        display_rows.append(row)

    for idx, row in enumerate(display_rows, start=1):
        hypothesis_id = str(row.get("hypothesis_id", f"RAW{idx:02d}"))
        statement = str(row.get("statement", "")).strip() or "(missing statement)"
        priority = str(row.get("priority", "-")).strip() or "-"
        tables = _coerce_str_list(row.get("tables"))
        required_columns = _coerce_str_list(row.get("required_columns"))
        derived_columns = row.get("derived_columns")
        window = str(row.get("window", "-"))
        granularity = str(row.get("granularity", "-"))
        notes = str(row.get("notes", "")).strip() or "-"
        requires_new_source = bool(row.get("requires_new_source", False))

        lines.append(f"{hypothesis_id} [{priority}]")
        lines.append("-" * 32)
        lines.append(f"Statement      : {statement}")
        lines.append(f"Tables         : {', '.join(tables) if tables else '-'}")
        lines.append(f"Window         : {window}")
        lines.append(f"Granularity    : {granularity}")
        lines.append("Required cols  :")
        if required_columns:
            for column in required_columns:
                lines.append(f"  - {column}")
        else:
            lines.append("  - -")

        lines.append("Derived cols   :")
        if isinstance(derived_columns, list) and derived_columns:
            for item in derived_columns:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "derived"))
                dtype = str(item.get("data_type", "-"))
                expr = str(item.get("sql_expression", ""))
                lines.append(f"  - {name} [{dtype}]")
                lines.append(f"    SQL: {expr}")
        else:
            lines.append("  - none")

        threshold = row.get("threshold", {})
        try:
            threshold_json = orjson.dumps(threshold, option=orjson.OPT_INDENT_2).decode()
        except Exception:
            threshold_json = str(threshold)
        lines.append("Threshold      :")
        for threshold_line in threshold_json.splitlines():
            lines.append(f"  {threshold_line}")
        lines.append(f"Needs source   : {requires_new_source}")
        lines.append(f"Notes          : {notes}")
        lines.append("")

    parse_errors = (validation_report or {}).get("parse_errors", [])
    if isinstance(parse_errors, list) and parse_errors:
        lines.append("Validation parse errors (first 10)")
        lines.append("----------------------------------")
        for error in parse_errors[:10]:
            lines.append(f"- {error}")
        lines.append("")

    return "\n".join(lines).rstrip()


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]
