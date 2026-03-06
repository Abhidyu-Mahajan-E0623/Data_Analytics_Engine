"""Pluggable post-evaluation trigger actions."""

from __future__ import annotations

from typing import Any, Literal

import requests

from src.config.settings import Settings
from src.connectors.databricks_sql import DatabricksSQLClient, sql_quote
from src.utils.io import OUTPUT_REPORTS_DIR, atomic_write_json
from src.utils.time import utc_iso

ActionType = Literal["noop", "webhook"]


def run_trigger(
    run_id: str,
    action: ActionType,
    settings: Settings,
    sql_client: DatabricksSQLClient,
    webhook_url: str | None = None,
) -> dict[str, Any]:
    """Execute configured trigger action."""
    alerts = fetch_top_alerts(run_id=run_id, settings=settings, sql_client=sql_client)
    payload = {
        "run_id": run_id,
        "action": action,
        "triggered_at": utc_iso(),
        "alert_count": len(alerts),
        "alerts": alerts,
    }

    if action == "webhook":
        if not webhook_url:
            raise ValueError("--webhook-url is required when action=webhook")
        response = requests.post(webhook_url, json=payload, timeout=20)
        response.raise_for_status()
        payload["webhook_status"] = response.status_code
    elif action != "noop":
        raise ValueError(f"Unsupported action: {action}")

    report_path = OUTPUT_REPORTS_DIR / f"trigger_{run_id}.json"
    atomic_write_json(report_path, payload)
    payload["report_path"] = str(report_path)
    return payload


def fetch_top_alerts(
    run_id: str,
    settings: Settings,
    sql_client: DatabricksSQLClient,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Get recent alerts for a run from monitoring.hypothesis_results."""
    table = f"`{settings.DATABRICKS_CATALOG}`.`{settings.DATABRICKS_SCHEMA_MONITORING}`.`hypothesis_results`"
    query = f"""
    SELECT run_id, hypothesis_id, domain, eval_value, comparison_value, status, explanation, computed_at
    FROM {table}
    WHERE run_id = {sql_quote(run_id)}
      AND status = 'ALERT'
    ORDER BY computed_at DESC
    LIMIT {int(limit)}
    """
    return sql_client.fetch_all(query)
