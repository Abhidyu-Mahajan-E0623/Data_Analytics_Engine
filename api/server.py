"""FastAPI server exposing LangGraph-powered generation endpoints."""

from __future__ import annotations

import logging
import time
import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from api.graphs import anomaly_graph, hypothesis_graph, insight_graph

logger = logging.getLogger("schema_maker.server")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
OUTPUT_DIR = PROJECT_ROOT / "Output"


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

# ---------------------------------------------------------------------------
# Frontend & Report Serving
# ---------------------------------------------------------------------------

def _get_latest_file(directory: Path, filename: str) -> Path | None:
    """Find the target file — first in the latest run sub-dir, then directly in the directory."""
    if not directory.exists():
        return None
    # Check inside run sub-directories first (sorted newest-first)
    run_dirs = sorted(
        [d for d in directory.iterdir() if d.is_dir()],
        key=lambda d: d.name,
        reverse=True,
    )
    for run_dir in run_dirs:
        target_file = run_dir / filename
        if target_file.exists():
            return target_file
    # Fall back: check if the file exists directly in the directory
    direct = directory / filename
    if direct.exists():
        return direct
    return None

@app.get("/api/latest_anomaly")
async def get_latest_anomaly():
    """Serve the most recent anomalies.txt file."""
    file_path = _get_latest_file(OUTPUT_DIR / "Anomaly", "anomalies.txt")
    if not file_path:
        raise HTTPException(status_code=404, detail="No anomaly report found")
    return FileResponse(file_path, media_type="text/plain")

@app.get("/api/latest_hypothesis")
async def get_latest_hypothesis():
    """Serve the most recent hypotheses.txt file."""
    file_path = _get_latest_file(OUTPUT_DIR / "hypotheses", "hypotheses.txt")
    if not file_path:
        raise HTTPException(status_code=404, detail="No hypotheses found")
    return FileResponse(file_path, media_type="text/plain")

@app.get("/api/latest_insight")
async def get_latest_insight():
    """Serve the most recent Insight.txt file."""
    file_path = _get_latest_file(OUTPUT_DIR / "Insight", "Insight.txt")
    if not file_path:
        raise HTTPException(status_code=404, detail="No insights found")
    return FileResponse(file_path, media_type="text/plain")


# Static Frontend Routing (Must be at the bottom)
if DASHBOARD_DIR.exists():
    app.mount("/", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="dashboard")
