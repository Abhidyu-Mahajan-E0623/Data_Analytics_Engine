"""FastAPI server exposing LangGraph-powered generation endpoints."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from api.graphs import anomaly_graph, hypothesis_graph, insight_graph

logger = logging.getLogger("schema_maker.server")


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown events)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Schema Maker API starting up — server ready to accept requests")
    yield
    logger.info("🛑 Schema Maker API shutting down")


app = FastAPI(
    title="Schema Maker API",
    description="LangGraph-powered REST API for Anomaly Detection, Hypothesis Generation, and Insight Generation.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response logging middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every incoming request and outgoing response with timing."""
    start = time.perf_counter()
    client = request.client.host if request.client else "unknown"
    logger.info(
        "→ %s %s from %s",
        request.method,
        request.url.path,
        client,
    )
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "← %s %s → %s (%.0f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class AnomalyRequest(BaseModel):
    schema_name: str = Field(
        default="bronze",
        alias="schema",
        description="Databricks schema to scan for anomalies (default: bronze).",
    )

    model_config = {"populate_by_name": True}


class AnomalyResponse(BaseModel):
    run_id: str
    total_anomalies: int
    report_text: str


class HypothesisRequest(BaseModel):
    schema_name: str = Field(
        ...,
        alias="schema",
        description="Databricks schema level for metadata, e.g. 'bronze' or 'silver'.",
    )
    domain: str = Field(
        ...,
        description="Business domain focus area(s), comma-separated. e.g. 'sales', 'marketing', 'sales,administration'.",
    )

    model_config = {"populate_by_name": True}


class HypothesisResponse(BaseModel):
    run_id: str
    valid_count: int
    invalid_count: int
    hypotheses_text: str
    metrics_tables_created: bool


class InsightRequest(BaseModel):
    run_id: str = Field(
        default="",
        description="Hypothesis run ID. Leave empty to use the latest run.",
    )
    hypothesis_ids: list[int] = Field(
        default_factory=list,
        description="Hypothesis numbers to generate insights for (e.g. [1,4,5,6]). Leave empty for all.",
    )


class InsightResponse(BaseModel):
    run_id: str
    insight_count: int
    insight_text: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post(
    "/api/anomaly",
    response_model=AnomalyResponse,
    summary="Run anomaly detection",
    description="Runs the anomaly detection pipeline on the specified schema and returns the formatted anomaly report.",
)
async def run_anomaly(request: AnomalyRequest = AnomalyRequest()) -> AnomalyResponse:
    """Run anomaly detection and return the formatted report text."""
    result = anomaly_graph.invoke({"schema": request.schema_name})

    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    return AnomalyResponse(
        run_id=result.get("run_id", ""),
        total_anomalies=result.get("total_anomalies", 0),
        report_text=result.get("report_text", ""),
    )


@app.post(
    "/api/hypothesis",
    response_model=HypothesisResponse,
    summary="Generate hypotheses",
    description=(
        "Runs hypothesis generation for the given schema and domain. "
        "Also creates/refreshes metrics tables in Databricks."
    ),
)
async def run_hypothesis(request: HypothesisRequest) -> HypothesisResponse:
    """Generate hypotheses and create metrics tables.

    User provides two inputs:
      - schema: which Databricks schema to scan for table metadata (e.g. 'bronze', 'silver')
      - domain: business focus area(s), comma-separated (e.g. 'sales', 'sales,marketing')
    """
    result = hypothesis_graph.invoke({
        "schema": request.schema_name,
        "domain": request.domain,
    })

    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    return HypothesisResponse(
        run_id=result.get("run_id", ""),
        valid_count=result.get("valid_count", 0),
        invalid_count=result.get("invalid_count", 0),
        hypotheses_text=result.get("hypotheses_text", ""),
        metrics_tables_created=result.get("metrics_tables_created", False),
    )


@app.post(
    "/api/insight",
    response_model=InsightResponse,
    summary="Generate insights",
    description="Generates business insights from selected hypotheses and their metrics tables.",
)
async def run_insight(request: InsightRequest = InsightRequest()) -> InsightResponse:
    """Generate insights from hypotheses.

    If run_id is empty, uses the latest run. If hypothesis_ids is empty, uses all.
    """
    result = insight_graph.invoke({
        "run_id": request.run_id,
        "hypothesis_ids": request.hypothesis_ids,
    })

    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    return InsightResponse(
        run_id=result.get("run_id", ""),
        insight_count=result.get("insight_count", 0),
        insight_text=result.get("insight_text", ""),
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    """Simple health check endpoint."""
    return {"status": "ok"}
