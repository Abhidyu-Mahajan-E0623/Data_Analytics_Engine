"""Unity Catalog metadata retrieval for prompt context and validation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import logging
from typing import Any

from src.connectors.databricks_sql import DatabricksSQLError, DatabricksSQLClient
from src.utils.time import utc_iso


@dataclass
class ColumnMetadata:
    """Column-level metadata."""

    name: str
    data_type: str
    description: str
    tags: dict[str, str] = field(default_factory=dict)
    pii: bool = False


@dataclass
class TableMetadata:
    """Table-level metadata."""

    catalog: str
    schema: str
    table: str
    description: str
    tags: dict[str, str] = field(default_factory=dict)
    columns: list[ColumnMetadata] = field(default_factory=list)

    @property
    def fqn(self) -> str:
        """Fully qualified table name."""
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"


@dataclass
class MetadataSnapshot:
    """Fetched metadata snapshot used by a run."""

    fetched_at: str
    catalog: str
    domain_filter: str
    quality_preference: str
    tables: list[TableMetadata]

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON persistence."""
        return asdict(self)


class DatabricksMetadataConnector:
    """Fetch Unity Catalog metadata and tags for table/column context."""

    def __init__(self, sql_client: DatabricksSQLClient, logger: logging.Logger | None = None) -> None:
        self._sql = sql_client
        self._logger = logger

    def fetch_metadata(
        self,
        catalog: str,
        domain: str,
        quality_preference: str = "gold",
        quality_tier: str | None = None,
        table_name_prefix: str = "snr",
    ) -> MetadataSnapshot:
        """Fetch and assemble metadata from Unity Catalog information_schema.

        Args:
            table_name_prefix: Only include tables whose name starts with this
                prefix (case-insensitive). Default ``"snr"`` so that only SNR
                tables are used for hypothesis generation. Pass ``""`` to
                disable the filter.
        """
        columns = self._fetch_columns(catalog)
        table_tags = self._fetch_table_tags(catalog)
        column_tags = self._fetch_column_tags(catalog)

        table_map: dict[tuple[str, str, str], TableMetadata] = {}
        requested_domain = domain.strip().lower()
        requested_tier = quality_tier.strip().lower() if quality_tier else None
        prefix_lower = table_name_prefix.strip().lower() if table_name_prefix else ""

        for row in columns:
            table_key = (row["catalog"], row["schema_name"], row["table_name"])
            tags = table_tags.get(table_key, {})
            table_domain = tags.get("domain", "").strip().lower()
            schema_name = row["schema_name"].lower()
            table_quality = _resolve_quality_tier(tags)

            # SNR table name prefix filter
            if prefix_lower and not row["table_name"].lower().startswith(prefix_lower):
                continue

            if requested_domain:
                schema_match = requested_domain == schema_name or requested_domain in schema_name
                tag_match = bool(table_domain and table_domain == requested_domain)
                if not schema_match and not tag_match:
                    continue
            if requested_tier and table_quality and table_quality != requested_tier:
                continue

            if table_key not in table_map:
                table_map[table_key] = TableMetadata(
                    catalog=row["catalog"],
                    schema=row["schema_name"],
                    table=row["table_name"],
                    description=row["table_description"],
                    tags=tags,
                    columns=[],
                )

            col_key = (row["catalog"], row["schema_name"], row["table_name"], row["column_name"])
            col_tags = column_tags.get(col_key, {})
            pii = col_tags.get("pii", "false").lower() == "true"
            table_map[table_key].columns.append(
                ColumnMetadata(
                    name=row["column_name"],
                    data_type=row["data_type"],
                    description=row["column_description"],
                    tags=col_tags,
                    pii=pii,
                )
            )

        tables = list(table_map.values())
        if self._logger:
            self._logger.info("Fetched metadata tables=%s (prefix_filter=%s)", len(tables), prefix_lower or "none")

        return MetadataSnapshot(
            fetched_at=utc_iso(),
            catalog=catalog,
            domain_filter=domain,
            quality_preference=quality_preference,
            tables=tables,
        )

    def _fetch_columns(self, catalog: str) -> list[dict[str, Any]]:
        query = f"""
        SELECT
            c.table_catalog AS catalog,
            c.table_schema AS schema_name,
            c.table_name AS table_name,
            c.column_name AS column_name,
            COALESCE(c.full_data_type, c.data_type) AS data_type,
            COALESCE(c.comment, '') AS column_description,
            COALESCE(t.comment, '') AS table_description
        FROM `{catalog}`.information_schema.columns c
        LEFT JOIN `{catalog}`.information_schema.tables t
            ON c.table_catalog = t.table_catalog
           AND c.table_schema = t.table_schema
           AND c.table_name = t.table_name
        WHERE c.table_schema <> 'information_schema'
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
        return self._sql.fetch_all(query)

    def _fetch_table_tags(self, catalog: str) -> dict[tuple[str, str, str], dict[str, str]]:
        query = f"""
        SELECT
            catalog_name AS catalog,
            schema_name AS schema_name,
            table_name AS table_name,
            tag_name AS tag_name,
            tag_value AS tag_value
        FROM `{catalog}`.information_schema.table_tags
        """
        try:
            rows = self._sql.fetch_all(query)
        except DatabricksSQLError:
            if self._logger:
                self._logger.warning("Table tags unavailable; continuing without tags.")
            return {}

        index: dict[tuple[str, str, str], dict[str, str]] = {}
        for row in rows:
            key = (row["catalog"], row["schema_name"], row["table_name"])
            index.setdefault(key, {})
            index[key][row["tag_name"].lower()] = str(row["tag_value"])
        return index

    def _fetch_column_tags(self, catalog: str) -> dict[tuple[str, str, str, str], dict[str, str]]:
        query = f"""
        SELECT
            catalog_name AS catalog,
            schema_name AS schema_name,
            table_name AS table_name,
            column_name AS column_name,
            tag_name AS tag_name,
            tag_value AS tag_value
        FROM `{catalog}`.information_schema.column_tags
        """
        try:
            rows = self._sql.fetch_all(query)
        except DatabricksSQLError:
            if self._logger:
                self._logger.warning("Column tags unavailable; continuing without tags.")
            return {}

        index: dict[tuple[str, str, str, str], dict[str, str]] = {}
        for row in rows:
            key = (row["catalog"], row["schema_name"], row["table_name"], row["column_name"])
            index.setdefault(key, {})
            index[key][row["tag_name"].lower()] = str(row["tag_value"])
        return index


def _resolve_quality_tier(tags: dict[str, str]) -> str:
    """Extract a quality tier from common tag names."""
    for key in ("quality_tier", "quality", "tier"):
        if key in tags and tags[key]:
            return tags[key].strip().lower()
    return ""
