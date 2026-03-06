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
    try:
        settings = load_settings_or_raise()
        schema = state.get("schema", "bronze")
        run_id = new_run_id()
        logger = configure_logging(run_id=run_id)

        outcome = run_bronze_anomaly_detection(
            settings=settings,
            run_id=run_id,
            catalog=settings.DATABRICKS_CATALOG,
            schema=schema,
            logger=logger,
        )

        # Read the generated report file
        report_path = Path(outcome.report_path)
        report_text = report_path.read_text(encoding="utf-8") if report_path.exists() else ""

        return {
            "run_id": outcome.run_id,
            "total_anomalies": outcome.total_anomalies,
            "report_text": report_text,
        }
    except Exception as exc:
        return {"error": str(exc)}


def hypothesis_node(state: HypothesisState) -> dict[str, Any]:
    """Run hypothesis generation + metrics table creation.

    Inputs:
        schema — Databricks schema level for metadata (e.g. 'bronze', 'silver')
        domain — business focus areas, comma-separated (e.g. 'sales,marketing')
    """
    try:
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
        logger = configure_logging(run_id=run_id)
        sql_client = DatabricksSQLClient(settings=settings, logger=logger)
        llm_client = AzureOpenAIClient(settings=settings, logger=logger)

        outcome = run_generate_pipeline(
            settings=settings,
            sql_client=sql_client,
            llm_client=llm_client,
            logger=logger,
            domain=schema,            # schema is used as the DB schema filter
            focus_areas=focus_areas,   # domain tokens are used as LLM focus
            top_k=settings.DEFAULT_TOP_K,
            run_id=run_id,
            business_constraints=None,
        )

        # Read back the generated hypotheses.txt
        hypotheses_txt_path = OUTPUT_HYPOTHESES_DIR / outcome.run_id / "hypotheses.txt"
        hypotheses_text = ""
        if hypotheses_txt_path.exists():
            hypotheses_text = hypotheses_txt_path.read_text(encoding="utf-8")

        return {
            "run_id": outcome.run_id,
            "valid_count": outcome.valid_count,
            "invalid_count": outcome.invalid_count,
            "hypotheses_text": hypotheses_text,
            "metrics_tables_created": outcome.valid_count > 0,
        }
    except Exception as exc:
        return {"error": str(exc)}


def insight_node(state: InsightState) -> dict[str, Any]:
    """Run insight generation for selected hypotheses."""
    try:
        settings = load_settings_or_raise()

        # Resolve run_id (use latest if not provided)
        run_id = (state.get("run_id") or "").strip()
        if not run_id:
            run_id = get_latest_run_id() or ""
        if not run_id:
            return {"error": "No run_id provided and no latest run found. Run hypothesis generation first."}

        # Resolve hypothesis IDs (use all if not provided)
        hypothesis_ids = state.get("hypothesis_ids", [])
        if not hypothesis_ids:
            available = list_hypotheses_for_run(run_id)
            if not available:
                return {"error": f"No hypotheses found for run_id={run_id}."}
            hypothesis_ids = [
                int(h["id"].replace("H", ""))
                for h in available
            ]

        logger = configure_logging(run_id=run_id)
        result = run_insight_generation(
            settings=settings,
            run_id=run_id,
            selected_ids=hypothesis_ids,
            logger=logger,
        )

        # Read the generated insight report
        output_path = Path(result.output_path)
        insight_text = output_path.read_text(encoding="utf-8") if output_path.exists() else ""

        return {
            "run_id": result.run_id,
            "insight_count": result.insight_count,
            "insight_text": insight_text,
        }
    except Exception as exc:
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
