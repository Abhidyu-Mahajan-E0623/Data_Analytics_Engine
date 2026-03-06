"""CLI entrypoint for local schema-maker pipeline."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import platform
import sys
from typing import Annotated

import orjson
import typer

from src.config.settings import load_settings_or_raise
from src.connectors.databricks_sql import DatabricksSQLError, DatabricksSQLClient
from src.llm.azure_openai import AzureOpenAIClient
from src.pipeline.actions import run_trigger
from src.pipeline.evaluate import evaluate_hypotheses
from src.pipeline.generate import run_generate_pipeline
from src.pipeline.monitor_tables import create_monitoring_tables, publish_hypotheses_to_catalog
from src.utils.time import new_run_id
from src.utils.io import OUTPUT_REPORTS_DIR
from src.utils.logging import configure_logging
from src_anomaly.pipeline import run_bronze_anomaly_detection
from src_insight.pipeline import (
    get_latest_run_id,
    list_hypotheses_for_run,
    run_insight_generation,
)

app = typer.Typer(help="Local hypothesis generation + monitoring pipeline.")


@app.command("generate")
def generate(
    domain: Annotated[
        str,
        typer.Option("--domain", help="Databricks schema/domain dataset, e.g. pharma_sales."),
    ],
    top_k: Annotated[
        int,
        typer.Option("--top-k", help="Top-K tables for context (defaults from .env)."),
    ] = 0,
    focus: Annotated[
        str,
        typer.Option(
            "--focus",
            help="Comma-separated focus areas, e.g. sales,marketing,administration.",
        ),
    ] = "",
    constraints: Annotated[
        str,
        typer.Option("--constraints", help="Optional business constraints."),
    ] = "",
) -> None:
    """Generate, validate and persist hypotheses."""
    settings = load_settings_or_raise()
    try:
        shared_run_id = new_run_id()
        resolved_top_k = top_k if top_k > 0 else settings.DEFAULT_TOP_K
        logger = configure_logging(run_id=shared_run_id)
        sql_client = DatabricksSQLClient(settings=settings, logger=logger)
        llm_client = AzureOpenAIClient(settings=settings, logger=logger)
        if not focus.strip():
            if sys.stdin.isatty():
                focus = typer.prompt(
                    "Focus areas (comma-separated, e.g. sales,marketing,administration)",
                    default=domain,
                )
            else:
                focus = domain
        focus_areas = _parse_focus_areas(focus, domain)

        with ThreadPoolExecutor(max_workers=2) as executor:
            generate_future = executor.submit(
                run_generate_pipeline,
                settings=settings,
                sql_client=sql_client,
                llm_client=llm_client,
                logger=logger,
                domain=domain,
                focus_areas=focus_areas,
                top_k=resolved_top_k,
                run_id=shared_run_id,
                business_constraints=(constraints or None),
            )
            anomaly_future = executor.submit(
                run_bronze_anomaly_detection,
                settings=settings,
                run_id=shared_run_id,
                catalog=settings.DATABRICKS_CATALOG,
                schema="bronze",
                logger=logger,
            )

            result = generate_future.result()
            anomaly_result = None
            anomaly_error: Exception | None = None
            try:
                anomaly_result = anomaly_future.result()
            except Exception as exc:  # pragma: no cover - integration path
                anomaly_error = exc

        typer.echo(f"run_id={result.run_id}")
        typer.echo(f"valid={result.valid_count} invalid={result.invalid_count}")
        typer.echo(f"artifacts={result.output_dir}")
        if anomaly_result is not None:
            typer.echo(f"anomaly_findings={anomaly_result.total_anomalies}")
            typer.echo(f"anomaly_report={anomaly_result.report_path}")
        if anomaly_error is not None:
            typer.secho(
                f"Anomaly detection warning: {_first_error_line(anomaly_error)}",
                fg=typer.colors.YELLOW,
                err=True,
            )
        if result.valid_count < 8:
            raise typer.Exit(code=1)
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("anomaly-detect")
def anomaly_detect(
    schema: Annotated[
        str,
        typer.Option("--schema", help="Source schema to scan for anomalies."),
    ] = "bronze",
) -> None:
    """Run anomaly detection for bronze-layer tables."""
    settings = load_settings_or_raise()
    try:
        run_id = new_run_id()
        logger = configure_logging(run_id=run_id)
        outcome = run_bronze_anomaly_detection(
            settings=settings,
            run_id=run_id,
            catalog=settings.DATABRICKS_CATALOG,
            schema=schema,
            logger=logger,
        )
        typer.echo(f"run_id={outcome.run_id}")
        typer.echo(f"anomaly_findings={outcome.total_anomalies}")
        typer.echo(f"anomaly_report={outcome.report_path}")
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("generate-insights")
def generate_insights(
    run_id: Annotated[
        str,
        typer.Option("--run-id", help="Hypothesis run ID to use. Defaults to latest."),
    ] = "",
    hypotheses: Annotated[
        str,
        typer.Option(
            "--hypotheses",
            help="Comma-separated hypothesis numbers, e.g. 1,4,5,6.",
        ),
    ] = "",
) -> None:
    """Generate insights from selected hypotheses and their metrics tables."""
    settings = load_settings_or_raise()
    try:
        # Resolve run_id
        resolved_run_id = run_id.strip()
        if not resolved_run_id:
            resolved_run_id = get_latest_run_id() or ""
        if not resolved_run_id:
            typer.secho(
                "No run_id provided and no latest run found. "
                "Run 'generate' first or provide --run-id.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)

        typer.echo(f"Using run_id: {resolved_run_id}")

        # Display available hypotheses
        available = list_hypotheses_for_run(resolved_run_id)
        if not available:
            typer.secho(
                f"No hypotheses found for run_id={resolved_run_id}.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)

        typer.echo("")
        typer.echo("Available Hypotheses:")
        for h in available:
            num = h["id"].replace("H", "").lstrip("0") or "0"
            typer.echo(f"  {num:>2}. [{h['id']}] {h['statement']}")
        typer.echo("")

        # Resolve selected IDs
        raw_selection = hypotheses.strip()
        if not raw_selection:
            if sys.stdin.isatty():
                raw_selection = typer.prompt(
                    "Enter hypothesis numbers (comma-separated, e.g. 1,4,5,6)",
                    default=",".join(
                        str(int(h["id"].replace("H", ""))) for h in available
                    ),
                )
            else:
                raw_selection = ",".join(
                    str(int(h["id"].replace("H", ""))) for h in available
                )

        selected_ids = [
            int(x.strip())
            for x in raw_selection.split(",")
            if x.strip().isdigit()
        ]
        if not selected_ids:
            typer.secho("No valid hypothesis numbers provided.", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)

        typer.echo(f"Selected hypotheses: {selected_ids}")
        logger = configure_logging(run_id=resolved_run_id)
        result = run_insight_generation(
            settings=settings,
            run_id=resolved_run_id,
            selected_ids=selected_ids,
            logger=logger,
        )
        typer.echo(f"insights_generated={result.insight_count}")
        typer.echo(f"output={result.output_path}")
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("create-monitoring-tables")
def create_monitoring_tables_cmd() -> None:
    """Create monitoring tables in Databricks."""
    settings = load_settings_or_raise()
    try:
        logger = configure_logging()
        sql_client = DatabricksSQLClient(settings=settings, logger=logger)
        create_monitoring_tables(sql_client=sql_client, settings=settings)
        typer.echo("Monitoring tables ensured.")
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("publish")
def publish(
    run_id: Annotated[str, typer.Option("--run-id", help="Run ID to publish.")]
) -> None:
    """Publish validated hypotheses into monitoring.hypothesis_catalog."""
    settings = load_settings_or_raise()
    try:
        logger = configure_logging(run_id=run_id)
        sql_client = DatabricksSQLClient(settings=settings, logger=logger)
        domain = _read_run_domain(run_id) or settings.DATABRICKS_SCHEMA_DOMAIN
        inserted = publish_hypotheses_to_catalog(
            run_id=run_id,
            domain=domain,
            sql_client=sql_client,
            settings=settings,
        )
        typer.echo(f"Published {inserted} hypotheses for run_id={run_id}.")
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("evaluate")
def evaluate(
    run_id: Annotated[str, typer.Option("--run-id", help="Run ID to evaluate.")],
    window: Annotated[
        str,
        typer.Option("--window", help="Optional window override (e.g. 7d)."),
    ] = "",
) -> None:
    """Evaluate hypotheses and append status rows to monitoring.hypothesis_results."""
    settings = load_settings_or_raise()
    try:
        logger = configure_logging(run_id=run_id)
        sql_client = DatabricksSQLClient(settings=settings, logger=logger)
        result = evaluate_hypotheses(
            run_id=run_id,
            settings=settings,
            sql_client=sql_client,
            window_override=(window or None),
        )
        typer.echo(
            f"Evaluated {result['evaluated']} hypotheses, alerts={result['alerts']}, summary={result['summary_path']}"
        )
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("trigger")
def trigger(
    run_id: Annotated[str, typer.Option("--run-id", help="Run ID.")],
    action: Annotated[str, typer.Option("--action", help="noop|webhook")] = "noop",
    webhook_url: Annotated[
        str,
        typer.Option("--webhook-url", help="Webhook URL when action=webhook."),
    ] = "",
) -> None:
    """Run pluggable action layer after evaluation."""
    settings = load_settings_or_raise()
    try:
        logger = configure_logging(run_id=run_id)
        sql_client = DatabricksSQLClient(settings=settings, logger=logger)
        payload = run_trigger(
            run_id=run_id,
            action=action,  # type: ignore[arg-type]
            settings=settings,
            sql_client=sql_client,
            webhook_url=(webhook_url or None),
        )
        typer.echo(
            f"Trigger action={payload['action']} alert_count={payload['alert_count']} report={payload['report_path']}"
        )
    except DatabricksSQLError as exc:
        _handle_databricks_error(exc, settings.DATABRICKS_CATALOG)


@app.command("version")
def version() -> None:
    """Show version and environment configuration summary."""
    settings = load_settings_or_raise()
    checks = {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "app_version": settings.APP_VERSION,
        "catalog": settings.DATABRICKS_CATALOG,
        "domain_schema_default": settings.DATABRICKS_SCHEMA_DOMAIN,
        "monitoring_schema": settings.DATABRICKS_SCHEMA_MONITORING,
        "warehouse_http_path": settings.databricks_http_path,
    }
    typer.echo(orjson.dumps(checks, option=orjson.OPT_INDENT_2).decode())


def _read_run_domain(run_id: str) -> str | None:
    path = Path("Output") / "hypotheses" / run_id / "run_meta.json"
    if not path.exists():
        fallback_path = OUTPUT_REPORTS_DIR / f"run_meta_{run_id}.json"
        path = fallback_path if fallback_path.exists() else path
    if not path.exists():
        return None
    payload = orjson.loads(path.read_bytes())
    return payload.get("domain")


def _parse_focus_areas(raw_focus: str, domain: str) -> list[str]:
    """Parse comma-separated focus areas with stable defaults."""
    tokens = [token.strip().lower() for token in raw_focus.split(",") if token.strip()]
    if not tokens:
        return [domain.strip().lower()]
    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token not in seen:
            seen.add(token)
            deduped.append(token)
    return deduped


def _handle_databricks_error(exc: DatabricksSQLError, catalog: str) -> None:
    """Render concise Databricks failures with actionable permission hints."""
    message = str(exc).strip()
    first_line = message.splitlines()[0] if message else "Unknown Databricks error"
    typer.secho(f"Databricks error: {first_line}", fg=typer.colors.RED, err=True)
    lower_msg = message.lower()
    if "use catalog" in lower_msg or "permission_denied" in lower_msg or "insufficient privileges" in lower_msg:
        typer.secho(
            (
                f"Permission issue for catalog `{catalog}`. Ask your Databricks admin to grant this token principal:\n"
                "- USE CATALOG on the catalog\n"
                "- USE SCHEMA on required source/monitoring schemas\n"
                "- SELECT on source tables/views\n"
                "- CREATE SCHEMA and CREATE TABLE (for monitoring tables)"
            ),
            fg=typer.colors.YELLOW,
            err=True,
        )
    raise typer.Exit(code=1)


def _first_error_line(exc: Exception) -> str:
    message = str(exc).strip()
    return message.splitlines()[0] if message else exc.__class__.__name__


if __name__ == "__main__":
    app()
