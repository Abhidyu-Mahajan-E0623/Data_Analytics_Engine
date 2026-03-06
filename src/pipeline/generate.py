"""End-to-end hypothesis generation pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.config.settings import Settings
from src.connectors.databricks_meta import DatabricksMetadataConnector
from src.connectors.databricks_sql import DatabricksSQLClient
from src.llm.azure_openai import AzureOpenAIClient
from src.llm.prompts import build_generation_messages
from src.pipeline.persist import (
    save_context_bundle,
    save_hypothesis_artifacts,
    save_metadata_snapshot,
    save_prompt_record,
    save_run_input,
)
from src.pipeline.metrics_table import create_or_replace_metrics_tables
from src.retrieval.selector import select_context_bundle
from src.utils.io import ensure_project_dirs
from src.utils.logging import configure_logging
from src.utils.time import new_run_id, utc_iso
from src.validation.validators import (
    build_catalog_index,
    parse_jsonl_hypotheses,
    validate_hypothesis_set,
)

EXPECTED_IDS = [f"H{i:02d}" for i in range(1, 11)]


@dataclass
class GenerateOutcome:
    """Summary of a generation run."""

    run_id: str
    valid_count: int
    invalid_count: int
    output_dir: str


def run_generate_pipeline(
    settings: Settings,
    sql_client: DatabricksSQLClient,
    llm_client: AzureOpenAIClient,
    logger: Any,
    domain: str,
    focus_areas: list[str] | None,
    top_k: int,
    run_id: str | None = None,
    business_constraints: str | None = None,
    max_repair_attempts: int = 2,
) -> GenerateOutcome:
    """Generate, validate, auto-repair and persist hypotheses."""
    ensure_project_dirs()
    run_id = run_id or new_run_id()
    logger = logger or configure_logging(run_id=run_id)
    resolved_focus_areas = [item.strip().lower() for item in (focus_areas or []) if item.strip()]
    if not resolved_focus_areas:
        resolved_focus_areas = [domain.strip().lower()]

    logger.info("Starting generation run", extra={"run_id": run_id, "domain": domain})
    save_run_input(
        run_id,
        {
            "run_id": run_id,
            "domain": domain,
            "focus_areas": resolved_focus_areas,
            "top_k": top_k,
            "business_constraints": business_constraints,
            "started_at": utc_iso(),
        },
    )

    meta_connector = DatabricksMetadataConnector(sql_client=sql_client, logger=logger)
    metadata_snapshot = meta_connector.fetch_metadata(
        catalog=settings.DATABRICKS_CATALOG,
        domain=domain,
        quality_preference="bronze",
    )
    metadata_snapshot_dict = metadata_snapshot.to_dict()
    save_metadata_snapshot(run_id=run_id, snapshot=metadata_snapshot_dict)

    context_bundle = select_context_bundle(
        metadata_snapshot,
        domain=domain,
        top_k=top_k,
        focus_areas=resolved_focus_areas,
    )
    table_assignment_plan = _build_table_assignment_plan(context_bundle, EXPECTED_IDS)
    save_context_bundle(run_id=run_id, context_bundle=context_bundle)

    prompt_messages = build_generation_messages(
        domain=domain,
        context_bundle=context_bundle,
        focus_areas=resolved_focus_areas,
        business_constraints=business_constraints,
    )
    first_generation = llm_client.generate_hypotheses(
        domain=domain,
        context_bundle=context_bundle,
        focus_areas=resolved_focus_areas,
        business_constraints=business_constraints,
        table_assignment_plan=table_assignment_plan,
    )
    save_prompt_record(
        run_id=run_id,
        prompt_record={
            "run_id": run_id,
            "prompt_hash": first_generation.prompt_hash,
            "messages": prompt_messages,
            "deployment": first_generation.deployment,
            "created_at": utc_iso(),
        },
    )

    all_raw_lines = list(first_generation.jsonl_lines)
    parsed = parse_jsonl_hypotheses(
        first_generation.jsonl_lines,
        context_bundle=context_bundle,
    )
    hypothesis_map = dict(parsed.hypotheses_by_id)
    parse_errors = list(parsed.parse_errors)

    catalog_index = build_catalog_index(metadata_snapshot_dict)
    valid, invalid = validate_hypothesis_set(hypothesis_map, catalog_index, sql_client)
    _enforce_expected_ids(hypothesis_map, invalid)
    _enforce_table_assignment(hypothesis_map, invalid, table_assignment_plan)

    repair_history: list[dict[str, Any]] = []
    repair_attempt = 0
    while repair_attempt < max_repair_attempts and _repairable_ids(invalid):
        repair_attempt += 1
        repair_targets = {hid: errors for hid, errors in invalid.items() if hid in EXPECTED_IDS}
        if parse_errors:
            repair_targets["_parse_errors"] = parse_errors[-20:]
        logger.info(
            "Running repair attempt %s for %s hypotheses",
            repair_attempt,
            len(repair_targets),
            extra={"run_id": run_id},
        )
        repaired = llm_client.repair_hypotheses(
            domain=domain,
            context_bundle=context_bundle,
            focus_areas=resolved_focus_areas,
            validation_errors=repair_targets,
            existing_valid_hypotheses=[item.model_dump() for item in valid.values()],
            business_constraints=business_constraints,
            table_assignment_plan=table_assignment_plan,
        )
        all_raw_lines.extend(repaired.jsonl_lines)
        repaired_parsed = parse_jsonl_hypotheses(
            repaired.jsonl_lines,
            context_bundle=context_bundle,
        )
        parse_errors.extend(
            [f"repair_attempt_{repair_attempt}: {msg}" for msg in repaired_parsed.parse_errors]
        )
        for hypothesis_id, model in repaired_parsed.hypotheses_by_id.items():
            hypothesis_map[hypothesis_id] = model
        valid, invalid = validate_hypothesis_set(hypothesis_map, catalog_index, sql_client)
        _enforce_expected_ids(hypothesis_map, invalid)
        _enforce_table_assignment(hypothesis_map, invalid, table_assignment_plan)
        repair_history.append(
            {
                "attempt": repair_attempt,
                "repaired_ids": sorted(repaired_parsed.hypotheses_by_id.keys()),
                "remaining_invalid_ids": sorted(invalid.keys()),
            }
        )

    valid_models = [valid[hid] for hid in EXPECTED_IDS if hid in valid]
    invalid_map = {
        hid: invalid.get(hid, ["missing or invalid output"])
        for hid in EXPECTED_IDS
        if hid not in valid
    }

    validation_report = {
        "run_id": run_id,
        "created_at": utc_iso(),
        "parse_errors": parse_errors,
        "repair_attempts": repair_attempt,
        "repair_history": repair_history,
        "valid_hypothesis_ids": [item.hypothesis_id for item in valid_models],
        "excluded_hypotheses": invalid_map,
        "valid_count": len(valid_models),
        "invalid_count": len(EXPECTED_IDS) - len(valid_models),
    }
    run_meta = {
        "run_id": run_id,
        "domain": domain,
        "focus_areas": resolved_focus_areas,
        "top_k": top_k,
        "started_at": utc_iso(),
        "prompt_hash": first_generation.prompt_hash,
        "model_deployment": first_generation.deployment,
        "top_k_assets_used": [table["fqn"] for table in context_bundle.get("tables", [])],
        "table_assignment_plan": table_assignment_plan,
        "counts": {
            "valid_hypotheses": len(valid_models),
            "invalid_hypotheses": len(EXPECTED_IDS) - len(valid_models),
        },
    }

    paths = save_hypothesis_artifacts(
        run_id=run_id,
        human_text=first_generation.human_text,
        valid_hypotheses=valid_models,
        raw_jsonl_lines=all_raw_lines,
        validation_report=validation_report,
        run_meta=run_meta,
    )

    if valid_models:
        metrics_tables = create_or_replace_metrics_tables(
            sql_client=sql_client,
            settings=settings,
            run_id=run_id,
            domain=domain,
            focus_areas=resolved_focus_areas,
            hypotheses=valid_models,
            metadata_snapshot=metadata_snapshot_dict,
        )
        logger.info(
            "Metrics tables refreshed: %s",
            ", ".join(metrics_tables) if metrics_tables else "(none)",
            extra={"run_id": run_id},
        )

    logger.info(
        "Generation completed: valid=%s invalid=%s",
        len(valid_models),
        len(EXPECTED_IDS) - len(valid_models),
        extra={"run_id": run_id},
    )
    return GenerateOutcome(
        run_id=run_id,
        valid_count=len(valid_models),
        invalid_count=len(EXPECTED_IDS) - len(valid_models),
        output_dir=str(paths["output_dir"]),
    )


def _enforce_expected_ids(
    hypotheses: dict[str, Any], invalid: dict[str, list[str]]
) -> None:
    for hypothesis_id in EXPECTED_IDS:
        if hypothesis_id not in hypotheses:
            invalid.setdefault(hypothesis_id, []).append("Missing hypothesis_id in output.")


def _enforce_table_assignment(
    hypotheses: dict[str, Any],
    invalid: dict[str, list[str]],
    table_assignment_plan: dict[str, str],
) -> None:
    if not table_assignment_plan:
        return
    for hypothesis_id, assigned_table in table_assignment_plan.items():
        hypothesis = hypotheses.get(hypothesis_id)
        if not hypothesis:
            continue
        normalized_tables = {
            _normalize_table_ref(table_ref)
            for table_ref in hypothesis.tables
            if _normalize_table_ref(table_ref)
        }
        if normalized_tables != {assigned_table}:
            invalid.setdefault(hypothesis_id, []).append(
                f"Table assignment mismatch. Use exactly this table for {hypothesis_id}: {assigned_table}."
            )


def _build_table_assignment_plan(
    context_bundle: dict[str, Any],
    hypothesis_ids: list[str],
) -> dict[str, str]:
    raw_tables = context_bundle.get("tables", [])
    if not isinstance(raw_tables, list):
        return {}

    ordered_tables: list[str] = []
    seen: set[str] = set()
    for item in raw_tables:
        if not isinstance(item, dict):
            continue
        table_ref = _normalize_table_ref(str(item.get("fqn", "")))
        if not table_ref or table_ref in seen:
            continue
        seen.add(table_ref)
        ordered_tables.append(table_ref)

    if len(ordered_tables) < 2:
        return {}

    plan: dict[str, str] = {}
    for idx, hypothesis_id in enumerate(hypothesis_ids):
        plan[hypothesis_id] = ordered_tables[idx % len(ordered_tables)]
    return plan


def _normalize_table_ref(raw: str) -> str:
    return raw.replace("`", "").strip().lower()


def _repairable_ids(invalid: dict[str, list[str]]) -> bool:
    return any(hid in EXPECTED_IDS for hid in invalid)
