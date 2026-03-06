"""Create and publish monitoring tables in Databricks."""

from __future__ import annotations

import json
from typing import Any

from src.config.settings import Settings
from src.connectors.databricks_sql import (
    DatabricksSQLClient,
    sql_array,
    sql_bool,
    sql_map,
    sql_quote,
)
from src.pipeline.persist import load_validated_hypotheses
from src.validation.schema_models import Hypothesis


def create_monitoring_tables(sql_client: DatabricksSQLClient, settings: Settings) -> None:
    """Create monitoring schema/tables if they do not already exist."""
    catalog = settings.DATABRICKS_CATALOG
    schema = settings.DATABRICKS_SCHEMA_MONITORING
    sql_client.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    catalog_table = _table_fqn(settings, "hypothesis_catalog")
    results_table = _table_fqn(settings, "hypothesis_results")

    sql_client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_table} (
            run_id STRING,
            hypothesis_id STRING,
            domain STRING,
            statement STRING,
            tables ARRAY<STRING>,
            required_columns ARRAY<STRING>,
            derived_columns MAP<STRING, STRING>,
            derived_types MAP<STRING, STRING>,
            window STRING,
            granularity STRING,
            threshold STRING,
            priority STRING,
            notes STRING,
            requires_new_source BOOLEAN,
            created_at TIMESTAMP,
            created_by STRING,
            status STRING
        )
        USING DELTA
        """
    )

    sql_client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {results_table} (
            run_id STRING,
            hypothesis_id STRING,
            domain STRING,
            eval_window_start TIMESTAMP,
            eval_window_end TIMESTAMP,
            eval_value DOUBLE,
            comparison_value DOUBLE,
            status STRING,
            explanation STRING,
            computed_at TIMESTAMP
        )
        USING DELTA
        """
    )


def publish_hypotheses_to_catalog(
    run_id: str,
    domain: str,
    sql_client: DatabricksSQLClient,
    settings: Settings,
    created_by: str = "local_user",
) -> int:
    """Insert validated run hypotheses into monitoring.hypothesis_catalog."""
    hypotheses = load_validated_hypotheses(run_id)
    table = _table_fqn(settings, "hypothesis_catalog")
    inserted = 0
    for hypothesis in hypotheses:
        query = _build_catalog_insert_sql(
            table=table,
            run_id=run_id,
            domain=domain,
            hypothesis=hypothesis,
            created_by=created_by,
        )
        sql_client.execute(query)
        inserted += 1
    return inserted


def _build_catalog_insert_sql(
    table: str,
    run_id: str,
    domain: str,
    hypothesis: Hypothesis,
    created_by: str,
) -> str:
    derived_expr = {item.name: item.sql_expression for item in hypothesis.derived_columns}
    derived_types = {item.name: item.data_type for item in hypothesis.derived_columns}
    threshold_json = json.dumps(hypothesis.threshold.model_dump())
    notes = hypothesis.notes or ""

    return f"""
    INSERT INTO {table}
    (
        run_id, hypothesis_id, domain, statement, tables, required_columns, derived_columns,
        derived_types, window, granularity, threshold, priority, notes, requires_new_source,
        created_at, created_by, status
    )
    VALUES
    (
        {sql_quote(run_id)},
        {sql_quote(hypothesis.hypothesis_id)},
        {sql_quote(domain)},
        {sql_quote(hypothesis.statement)},
        {sql_array(hypothesis.tables)},
        {sql_array(hypothesis.required_columns)},
        {sql_map(derived_expr)},
        {sql_map(derived_types)},
        {sql_quote(hypothesis.window)},
        {sql_quote(hypothesis.granularity)},
        {sql_quote(threshold_json)},
        {sql_quote(hypothesis.priority)},
        {sql_quote(notes)},
        {sql_bool(hypothesis.requires_new_source)},
        current_timestamp(),
        {sql_quote(created_by)},
        'active'
    )
    """


def _table_fqn(settings: Settings, table_name: str) -> str:
    return f"`{settings.DATABRICKS_CATALOG}`.`{settings.DATABRICKS_SCHEMA_MONITORING}`.`{table_name}`"
