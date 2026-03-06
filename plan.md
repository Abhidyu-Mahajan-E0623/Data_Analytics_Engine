# LangGraph API — Architecture Plan

## Overview

This plan adds **LangGraph-powered FastAPI endpoints** for the three generation pipelines (Anomaly, Hypothesis, Insight), allowing them to be called as REST APIs from anywhere.

Each pipeline is wrapped as a **LangGraph node** inside a `StateGraph`. A single FastAPI app (`api/server.py`) exposes three endpoints that invoke these graphs.

---

## Simplified Hypothesis Input

The hypothesis generation API will accept **only 2 user inputs**:

| Input    | Description                                                                 | Example Values                      |
|----------|-----------------------------------------------------------------------------|-------------------------------------|
| `schema` | The Databricks schema/level for metadata (table discovery)                  | `bronze`, `silver`                  |
| `domain` | Business domain focus area(s) — guides LLM hypothesis themes               | `sales`, `marketing`, `sales,administration` |

**Mapping to existing code:**
- `schema` → passed to `DatabricksMetadataConnector.fetch_metadata(domain=schema)` as the schema filter
- `domain` → parsed into `focus_areas` list for the LLM prompt
- `top_k` → uses `settings.DEFAULT_TOP_K` from `.env`
- `constraints` → set to `None` (no business constraints)

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  FastAPI Server                   │
│              (api/server.py)                      │
├──────────────────────────────────────────────────┤
│  POST /api/anomaly     → AnomalyGraph.invoke()   │
│  POST /api/hypothesis  → HypothesisGraph.invoke() │
│  POST /api/insight     → InsightGraph.invoke()    │
└──────────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────┐
│              LangGraph StateGraphs               │
│           (api/graphs.py)                        │
├──────────────────────────────────────────────────┤
│  anomaly_graph:  START → anomaly_node → END      │
│  hypothesis_graph: START → hypothesis_node → END │
│  insight_graph:  START → insight_node → END      │
└──────────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────┐
│           Existing Pipeline Functions            │
├──────────────────────────────────────────────────┤
│  src_anomaly.pipeline.run_bronze_anomaly_detection│
│  src.pipeline.generate.run_generate_pipeline      │
│  src.pipeline.metrics_table (auto-triggered)      │
│  src_insight.pipeline.run_insight_generation      │
└──────────────────────────────────────────────────┘
```

---

## New Files

| File              | Purpose                                                      |
|-------------------|--------------------------------------------------------------|
| `api/__init__.py` | Package marker                                               |
| `api/graphs.py`   | LangGraph `StateGraph` definitions + node functions           |
| `api/server.py`   | FastAPI app with 3 POST endpoints                            |
| `plan.md`         | This file — project architecture reference                   |

---

## API Endpoints

### 1. `POST /api/anomaly`

**Request body:** _(none required, optional schema override)_
```json
{
  "schema": "bronze"
}
```

**Response:** Returns the formatted `anomalies.txt` content.
```json
{
  "run_id": "run_20260305T...",
  "total_anomalies": 12,
  "report_text": "======= DATA QUALITY ANOMALY REPORT ======= ..."
}
```

### 2. `POST /api/hypothesis`

**Request body:** _(user provides 2 inputs)_
```json
{
  "schema": "silver",
  "domain": "sales,marketing"
}
```

**Response:** Returns the formatted `hypotheses.txt` content + triggers metrics table creation.
```json
{
  "run_id": "run_20260305T...",
  "valid_count": 10,
  "invalid_count": 0,
  "hypotheses_text": "... formatted hypotheses.txt content ...",
  "metrics_tables_created": true
}
```

### 3. `POST /api/insight`

**Request body:**
```json
{
  "run_id": "",
  "hypothesis_ids": [1, 4, 5, 6]
}
```

**Response:** Returns the formatted `Insight.txt` content.
```json
{
  "run_id": "run_20260305T...",
  "insight_count": 4,
  "insight_text": "======= OVERALL INSIGHTS ======= ..."
}
```

---

## Dependencies Added

```
langgraph>=0.2.0
fastapi>=0.115.0
uvicorn>=0.34.0
```

---

## How to Run

```bash
# From project root (inside venv)
pip install -r requirements.txt
uvicorn api.server:app --host 0.0.0.0 --port 8000 --reload
```

Then call from anywhere:
```bash
curl -X POST http://<host>:8000/api/anomaly
curl -X POST http://<host>:8000/api/hypothesis -H "Content-Type: application/json" -d '{"schema":"silver","domain":"sales"}'
curl -X POST http://<host>:8000/api/insight -H "Content-Type: application/json" -d '{"run_id":"","hypothesis_ids":[1,2,3]}'
```

---

## Note on Hypothesis API User Inputs

Since the hypothesis endpoint requires user input (`schema` and `domain`), these are passed as a JSON request body in the POST request. This lets the API be called programmatically from any client (frontend, Postman, cURL, another service) with full control over the inputs — no interactive prompts needed.
