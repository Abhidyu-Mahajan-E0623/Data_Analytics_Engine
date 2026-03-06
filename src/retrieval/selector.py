"""Domain-aware metadata selection for prompt context."""

from __future__ import annotations

from typing import Any

from src.connectors.databricks_meta import MetadataSnapshot, TableMetadata


def select_context_bundle(
    snapshot: MetadataSnapshot,
    domain: str,
    top_k: int,
    focus_areas: list[str] | None = None,
) -> dict[str, Any]:
    """Build compact context from metadata snapshot."""
    domain_lower = domain.strip().lower()
    focus_terms = [item.strip().lower() for item in (focus_areas or []) if item.strip()]

    scored_tables = []
    for table in snapshot.tables:
        score = _table_score(table, domain_lower, focus_terms)
        scored_tables.append((score, table))
    scored_tables.sort(key=lambda item: item[0], reverse=True)
    selected_tables = [table for _, table in scored_tables[:top_k]]

    serialized_tables = []
    total_columns = 0
    for table in selected_tables:
        columns = []
        excluded_pii = []
        for column in table.columns:
            if column.pii:
                excluded_pii.append(column.name)
                continue
            columns.append(
                {
                    "name": column.name,
                    "data_type": column.data_type,
                    "description": column.description,
                    "tags": column.tags,
                }
            )
        total_columns += len(columns)
        serialized_tables.append(
            {
                "fqn": table.fqn,
                "catalog": table.catalog,
                "schema": table.schema,
                "table": table.table,
                "description": table.description,
                "tags": table.tags,
                "excluded_pii_columns": excluded_pii,
                "columns": columns,
            }
        )

    return {
        "domain": domain,
        "focus_areas": focus_terms,
        "top_k": top_k,
        "selected_table_count": len(serialized_tables),
        "selected_column_count": total_columns,
        "tables": serialized_tables,
    }


def _table_score(table: TableMetadata, domain: str, focus_terms: list[str]) -> int:
    score = 0
    table_domain = table.tags.get("domain", "").lower()
    quality = (
        table.tags.get("quality_tier", "")
        or table.tags.get("quality", "")
        or table.tags.get("tier", "")
    ).lower()
    if table_domain == domain and domain:
        score += 10
    if domain and domain in table.schema.lower():
        score += 5
    if quality == "gold":
        score += 4
    elif quality == "silver":
        score += 2

    table_layer_text = f"{table.schema.lower()}.{table.table.lower()}"
    if ".bronze" in table_layer_text or "bronze_" in table_layer_text or "_bronze" in table_layer_text:
        score += 8
    elif ".silver" in table_layer_text or "silver_" in table_layer_text or "_silver" in table_layer_text:
        score -= 2

    if focus_terms:
        table_text = " ".join(
            [
                table.schema.lower(),
                table.table.lower(),
                table.description.lower(),
                " ".join(str(v).lower() for v in table.tags.values()),
            ]
        )
        table_focus_hits = sum(1 for token in focus_terms if token in table_text)
        score += table_focus_hits * 6

        column_focus_hits = 0
        for column in table.columns:
            col_text = " ".join(
                [
                    column.name.lower(),
                    column.description.lower(),
                    " ".join(str(v).lower() for v in column.tags.values()),
                ]
            )
            if any(token in col_text for token in focus_terms):
                column_focus_hits += 1
        score += min(column_focus_hits, 10) * 2

    score += min(len(table.columns), 20)
    return score
