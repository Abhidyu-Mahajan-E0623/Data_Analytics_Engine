"""LangGraph state graphs for anomaly, hypothesis, and insight pipelines."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from src.config.settings import Settings, load_settings_or_raise
from src.connectors.databricks_sql import DatabricksSQLClient
from src.llm.azure_openai import AzureOpenAIClient
from src.pipeline.generate import run_generate_pipeline
from src.utils.io import OUTPUT_HYPOTHESES_DIR
from src.utils.logging import configure_logging
from src.utils.time import new_run_id
from src_anomaly.pipeline import run_bronze_anomaly_detection
from src_insight.pipeline import (
    get_latest_run_id,
    list_hypotheses_for_run,
    run_insight_generation,
)


# ---------------------------------------------------------------------------
# State schemas
# ---------------------------------------------------------------------------

class AnomalyState(TypedDict, total=False):
    """State for the anomaly detection graph."""
    schema: str
    run_id: str
    total_anomalies: int
    report_text: str
    error: str


class HypothesisState(TypedDict, total=False):
    """State for the hypothesis generation graph."""
    schema: str
    domain: str
    run_id: str
    valid_count: int
    invalid_count: int
    hypotheses_text: str
    metrics_tables_created: bool
    error: str


class InsightState(TypedDict, total=False):
    """State for the insight generation graph."""
    run_id: str
    hypothesis_ids: list[int]
    insight_count: int
    insight_text: str
    error: str


# ---------------------------------------------------------------------------
# Node functions
# ---------------------------------------------------------------------------

def anomaly_node(state: AnomalyState) -> dict[str, Any]:
    """Run the anomaly detection pipeline and return formatted report text."""
    logger = logging.getLogger("schema_maker.graph")
    logger.info("[anomaly] ▶ Anomaly node started", extra={"module": "anomaly", "step": "start"})
    try:
        logger.info("[anomaly] Step 1: Loading settings", extra={"module": "anomaly", "step": "load_settings"})
        settings = load_settings_or_raise()
        schema = state.get("schema", "bronze")
        run_id = new_run_id()
        pipe_logger = configure_logging(run_id=run_id)

        logger.info(
            "[anomaly] Step 2: Running anomaly detection pipeline (run_id=%s, schema=%s)",
            run_id, schema,
            extra={"module": "anomaly", "step": "run_pipeline", "run_id": run_id},
        )
        outcome = run_bronze_anomaly_detection(
            settings=settings,
            run_id=run_id,
            catalog=settings.DATABRICKS_CATALOG,
            schema=schema,
            logger=pipe_logger,
        )

        # Read the generated report file
        logger.info(
            "[anomaly] Step 3: Reading report file (total_anomalies=%s)",
            outcome.total_anomalies,
            extra={"module": "anomaly", "step": "read_report", "run_id": run_id},
        )
        report_path = Path(outcome.report_path)
        report_text = report_path.read_text(encoding="utf-8") if report_path.exists() else ""

        logger.info(
            "[anomaly] ✔ Anomaly node completed — %s anomalies found",
            outcome.total_anomalies,
            extra={"module": "anomaly", "step": "done", "run_id": run_id},
        )
        return {
            "run_id": outcome.run_id,
            "total_anomalies": outcome.total_anomalies,
            "report_text": report_text,
        }
    except Exception as exc:
        logger.exception("[anomaly] ✘ Anomaly node failed: %s", exc, extra={"module": "anomaly", "step": "error"})
        return {"error": str(exc)}


def hypothesis_node(state: HypothesisState) -> dict[str, Any]:
    """Run hypothesis generation + metrics table creation.

    Inputs:
        schema — Databricks schema level for metadata (e.g. 'bronze', 'silver')
        domain — business focus areas, comma-separated (e.g. 'sales,marketing')
    """
    logger = logging.getLogger("schema_maker.graph")
    logger.info("[hypothesis] ▶ Hypothesis node started", extra={"module": "hypothesis", "step": "start"})
    try:
        logger.info("[hypothesis] Step 1: Loading settings", extra={"module": "hypothesis", "step": "load_settings"})
        settings = load_settings_or_raise()
        schema = state.get("schema", settings.DATABRICKS_SCHEMA_DOMAIN)
        domain_raw = state.get("domain", schema)

        # Parse domain into focus_areas list
        focus_areas = [
            token.strip().lower()
            for token in domain_raw.split(",")
            if token.strip()
        ]
        if not focus_areas:
            focus_areas = [schema.strip().lower()]

        run_id = new_run_id()
        pipe_logger = configure_logging(run_id=run_id)

        logger.info(
            "[hypothesis] Step 2: Initializing clients (run_id=%s, schema=%s, focus_areas=%s)",
            run_id, schema, focus_areas,
            extra={"module": "hypothesis", "step": "init_clients", "run_id": run_id},
        )
        sql_client = DatabricksSQLClient(settings=settings, logger=pipe_logger)
        llm_client = AzureOpenAIClient(settings=settings, logger=pipe_logger)

        logger.info(
            "[hypothesis] Step 3: Running hypothesis generation pipeline",
            extra={"module": "hypothesis", "step": "run_pipeline", "run_id": run_id},
        )
        outcome = run_generate_pipeline(
            settings=settings,
            sql_client=sql_client,
            llm_client=llm_client,
            logger=pipe_logger,
            domain=schema,            # schema is used as the DB schema filter
            focus_areas=focus_areas,   # domain tokens are used as LLM focus
            top_k=settings.DEFAULT_TOP_K,
            run_id=run_id,
            business_constraints=None,
        )

        # Read back the generated hypotheses.txt
        logger.info(
            "[hypothesis] Step 4: Reading generated hypotheses (valid=%s, invalid=%s)",
            outcome.valid_count, outcome.invalid_count,
            extra={"module": "hypothesis", "step": "read_output", "run_id": run_id},
        )
        hypotheses_txt_path = OUTPUT_HYPOTHESES_DIR / outcome.run_id / "hypotheses.txt"
        hypotheses_text = ""
        if hypotheses_txt_path.exists():
            hypotheses_text = hypotheses_txt_path.read_text(encoding="utf-8")

        logger.info(
            "[hypothesis] ✔ Hypothesis node completed — valid=%s, invalid=%s",
            outcome.valid_count, outcome.invalid_count,
            extra={"module": "hypothesis", "step": "done", "run_id": run_id},
        )
        return {
            "run_id": outcome.run_id,
            "valid_count": outcome.valid_count,
            "invalid_count": outcome.invalid_count,
            "hypotheses_text": hypotheses_text,
            "metrics_tables_created": outcome.valid_count > 0,
        }
    except Exception as exc:
        logger.exception("[hypothesis] ✘ Hypothesis node failed: %s", exc, extra={"module": "hypothesis", "step": "error"})
        return {"error": str(exc)}


def insight_node(state: InsightState) -> dict[str, Any]:
    """Run insight generation for selected hypotheses."""
    logger = logging.getLogger("schema_maker.graph")
    logger.info("[insight] ▶ Insight node started", extra={"module": "insight", "step": "start"})
    try:
        logger.info("[insight] Step 1: Loading settings", extra={"module": "insight", "step": "load_settings"})
        settings = load_settings_or_raise()

        # Resolve run_id (use latest if not provided)
        run_id = (state.get("run_id") or "").strip()
        if not run_id:
            run_id = get_latest_run_id() or ""
        if not run_id:
            logger.warning("[insight] No run_id provided and no latest run found", extra={"module": "insight", "step": "no_run_id"})
            return {"error": "No run_id provided and no latest run found. Run hypothesis generation first."}

        # Resolve hypothesis IDs (use all if not provided)
        hypothesis_ids = state.get("hypothesis_ids", [])
        if not hypothesis_ids:
            logger.info("[insight] Step 2: No hypothesis IDs provided — listing all for run %s", run_id, extra={"module": "insight", "step": "list_hypotheses"})
            available = list_hypotheses_for_run(run_id)
            if not available:
                return {"error": f"No hypotheses found for run_id={run_id}."}
            hypothesis_ids = [
                int(h["id"].replace("H", ""))
                for h in available
            ]

        logger.info(
            "[insight] Step 3: Running insight generation (run_id=%s, hypothesis_ids=%s)",
            run_id, hypothesis_ids,
            extra={"module": "insight", "step": "run_pipeline", "run_id": run_id},
        )
        pipe_logger = configure_logging(run_id=run_id)
        result = run_insight_generation(
            settings=settings,
            run_id=run_id,
            selected_ids=hypothesis_ids,
            logger=pipe_logger,
        )

        # Read the generated insight report
        logger.info(
            "[insight] Step 4: Reading generated insight report (%s insights)",
            result.insight_count,
            extra={"module": "insight", "step": "read_report", "run_id": run_id},
        )
        output_path = Path(result.output_path)
        insight_text = output_path.read_text(encoding="utf-8") if output_path.exists() else ""

        logger.info(
            "[insight] ✔ Insight node completed — %s insights generated",
            result.insight_count,
            extra={"module": "insight", "step": "done", "run_id": run_id},
        )
        return {
            "run_id": result.run_id,
            "insight_count": result.insight_count,
            "insight_text": insight_text,
        }
    except Exception as exc:
        logger.exception("[insight] ✘ Insight node failed: %s", exc, extra={"module": "insight", "step": "error"})
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_anomaly_graph() -> StateGraph:
    """Build the LangGraph StateGraph for anomaly detection."""
    graph = StateGraph(AnomalyState)
    graph.add_node("anomaly_node", anomaly_node)
    graph.add_edge(START, "anomaly_node")
    graph.add_edge("anomaly_node", END)
    return graph.compile()


def build_hypothesis_graph() -> StateGraph:
    """Build the LangGraph StateGraph for hypothesis generation."""
    graph = StateGraph(HypothesisState)
    graph.add_node("hypothesis_node", hypothesis_node)
    graph.add_edge(START, "hypothesis_node")
    graph.add_edge("hypothesis_node", END)
    return graph.compile()


def build_insight_graph() -> StateGraph:
    """Build the LangGraph StateGraph for insight generation."""
    graph = StateGraph(InsightState)
    graph.add_node("insight_node", insight_node)
    graph.add_edge(START, "insight_node")
    graph.add_edge("insight_node", END)
    return graph.compile()


# Pre-built graph instances for import
anomaly_graph = build_anomaly_graph()
hypothesis_graph = build_hypothesis_graph()
insight_graph = build_insight_graph()
