# Schema Maker (Local, Azure OpenAI + Databricks)

Local, CLI-first Python project that:
- fetches Unity Catalog metadata from Databricks,
- generates 10 testable hypotheses via Azure OpenAI,
- validates schema/catalog/SQL/PII constraints,
- persists artifacts to `Input/` and `Output/`,
- publishes hypotheses to monitoring tables,
- evaluates hypotheses and appends results.

## Architecture

```text
User CLI
  |
  +--> generate
  |      +--> Databricks metadata fetch (Unity Catalog)
  |      +--> Input/metadata snapshot
  |      +--> context selection (domain, top-k, no PII)
  |      +--> Azure OpenAI generation
  |      +--> validation + auto-repair (<=2 attempts)
  |      +--> Output/hypotheses/<run_id> artifacts
  |      +--> Output/reports validation/prompt/context metadata
  |
  +--> create-monitoring-tables
  |      +--> Databricks monitoring schema/table DDL
  |
  +--> publish --run-id
  |      +--> Output/hypotheses/<run_id>/hypotheses.jsonl
  |      +--> monitoring.hypothesis_catalog inserts
  |
  +--> evaluate --run-id
  |      +--> monitoring.hypothesis_catalog reads
  |      +--> metric SQL + threshold checks
  |      +--> monitoring.hypothesis_results appends
  |      +--> Output/reports/summary_<run_id>.txt
  |
  +--> trigger --run-id
         +--> noop (default) or webhook placeholder
```

## Project Structure

```text
.
├─ Input/
│  ├─ metadata/
│  └─ runs/
├─ Output/
│  ├─ hypotheses/
│  ├─ logs/
│  └─ reports/
├─ src/
│  ├─ cli.py
│  ├─ config/
│  ├─ connectors/
│  ├─ llm/
│  ├─ retrieval/
│  ├─ validation/
│  ├─ pipeline/
│  └─ utils/
├─ tests/
├─ .env.example
├─ requirements.txt
├─ Makefile
└─ LICENSE
```

## Prerequisites

- Python 3.10+
- Databricks PAT with Unity Catalog + SQL Warehouse access
- Azure OpenAI resource + chat deployment
- Local shell with `make` (or run Python commands directly)

## Setup

1. Create env file and fill secrets:

```bash
cp .env.example .env
```

2. Create venv and install dependencies:

```bash
make venv && make install
```

3. Activate venv:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

## Environment Variables (`.env`)

```env
# Azure OpenAI
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_API_VERSION=2024-02-15-preview
AZURE_OPENAI_CHAT_DEPLOYMENT=
AZURE_OPENAI_EMBED_DEPLOYMENT=

# Databricks
DATABRICKS_HOST=
DATABRICKS_TOKEN=
DATABRICKS_SQL_WAREHOUSE_ID=
DATABRICKS_CATALOG=dev_analytics
DATABRICKS_SCHEMA_DOMAIN=sales
DATABRICKS_SCHEMA_MONITORING=monitoring

# Behavior
DEFAULT_TOP_K=10
OUTPUT_TIMEZONE=UTC
```

## CLI Commands

Generate hypotheses:

```bash
python -m src.cli generate --domain sales
python -m src.cli generate --domain sales --focus sales,marketing
```

Create monitoring tables:

```bash
python -m src.cli create-monitoring-tables
```

Publish validated hypotheses:

```bash
python -m src.cli publish --run-id <id>
```

Evaluate hypotheses:

```bash
python -m src.cli evaluate --run-id <id>
```

Run trigger action:

```bash
python -m src.cli trigger --run-id <id> --action noop
python -m src.cli trigger --run-id <id> --action webhook --webhook-url https://example.com/hook
```

Version/environment summary:

```bash
python -m src.cli version
```

## Makefile Shortcuts

- `make venv` -> create `.venv`, upgrade `pip`
- `make install` -> install dependencies
- `make run DOMAIN=sales` -> generate -> create-monitoring-tables -> publish latest run
- `make evaluate RUN_ID=<id>` -> evaluate hypotheses
- `make lint` -> syntax compile check
- `make test` -> run unit tests

## Input/Output Contract

### Input
- Databricks metadata snapshot:
  - `Input/metadata/metadata_snapshot_<run_id>.json`
- Run input record:
  - `Input/runs/run_input_<run_id>.json`

### Output
- Run artifacts:
  - `Output/hypotheses/<run_id>/hypotheses.txt`
  - `Output/hypotheses/<run_id>/hypotheses.jsonl` (validated, publish source)
  - `Output/hypotheses/<run_id>/hypotheses_raw.jsonl` (raw model lines)
  - `Output/hypotheses/<run_id>/validation_report.json`
  - `Output/hypotheses/<run_id>/run_meta.json`
- Reports:
  - `Output/reports/validation_report_<run_id>.json`
  - `Output/reports/context_bundle_<run_id>.json`
  - `Output/reports/prompt_record_<run_id>.json`
  - `Output/reports/summary_<run_id>.txt`
- Logs:
  - `Output/logs/<run_id>.log` (JSON structured, rotating)

### Focus-Aware Generation

Use `--focus` to constrain hypothesis intent when a schema spans multiple business areas.

Examples:
- `--focus sales`
- `--focus marketing,administration`

If omitted, focus defaults to the chosen `--domain`.

## Monitoring Tables

Created under:
- `DATABRICKS_CATALOG`.`DATABRICKS_SCHEMA_MONITORING`.`hypothesis_catalog`
- `DATABRICKS_CATALOG`.`DATABRICKS_SCHEMA_MONITORING`.`hypothesis_results`

`hypothesis_catalog` stores validated hypotheses.
`hypothesis_results` is append-only evaluation history.
`metric_<hypothesis_id>_<table_name>` tables are recreated per generate run, for example:
- `monitoring.metric_h01_silver_sales`
- `monitoring.metric_h01_flat_pharma_sales`
- `monitoring.metric_h03_silver_sales`

Each `metric_<hypothesis_id>_<table_name>` table contains:
- base run fields (`run_id`, `domain`, `focus_areas`, `source_table`)
- required/derived metric columns for that single hypothesis + source-table slice
- row-level source data for that hypothesis/table slice

## Validation Rules

- Pydantic schema validation for each JSONL line
- Catalog existence checks for `tables` + `required_columns`
- PII exclusion checks (`pii=true` tags rejected)
- SQL dry-run for each `derived_columns[*].sql_expression` via `EXPLAIN ... LIMIT 0`
- Reference consistency checks: required/derived references must belong to declared `tables`
- Derived SQL guardrails reject unstable patterns (`OVER`, ranking functions, `LIMIT` in expression)
- Derived SQL must be row-level (aggregate wrapper expressions like `SUM(...)` are rejected)
- Auto-repair attempts up to 2 for invalid hypotheses

If fewer than 8 hypotheses are valid, `generate` exits with non-zero code.

## Troubleshooting

- Auth failures (`401/403`):
  - Verify `DATABRICKS_TOKEN`, `DATABRICKS_HOST`, and Azure API key/endpoint.
- SQL warehouse issues:
  - Confirm `DATABRICKS_SQL_WAREHOUSE_ID` and warehouse running state.
- Catalog permission issues (`USE CATALOG` / `CREATE SCHEMA`):
  - Grant access on `DATABRICKS_CATALOG` to the SQL principal used by `DATABRICKS_TOKEN`.
  - Minimum grants for this pipeline are typically `USE CATALOG`, `USE SCHEMA`, `SELECT` on source tables, and `CREATE SCHEMA`/`CREATE TABLE` for monitoring objects.
- Validation failures:
  - Inspect `Output/hypotheses/<run_id>/validation_report.json`.
- Token/context limits:
  - Lower `--top-k` and retry.
- Empty publish:
  - Ensure `hypotheses.jsonl` exists in `Output/hypotheses/<run_id>/`.

## Security Notes

- Secrets are loaded only from `.env` (never hardcoded).
- No OS keyring usage.
- PII-tagged columns are excluded from prompt context and validation rejects PII usage.
- Prompt/context/report artifacts remain local under `Output/`.

## Future Trigger Layer

`trigger` currently supports:
- `noop` (default, no external call)
- `webhook` placeholder for Teams/custom integrations

You can extend `src/pipeline/actions.py` with richer notification providers later.
