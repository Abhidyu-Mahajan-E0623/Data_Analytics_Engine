"""Databricks SQL execution helpers."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import logging
from typing import Any, Generator, Mapping, Sequence

from databricks import sql as dbsql

from src.config.settings import Settings


class DatabricksSQLError(RuntimeError):
    """Raised when Databricks SQL execution fails."""


@dataclass
class QueryResult:
    """Result set wrapper."""

    rows: list[dict[str, Any]]


class DatabricksSQLClient:
    """Thin wrapper for Databricks SQL connector."""

    def __init__(self, settings: Settings, logger: logging.Logger | None = None) -> None:
        self._settings = settings
        self._logger = logger

    @contextmanager
    def _connection(self) -> Generator[Any, None, None]:
        conn = dbsql.connect(
            server_hostname=self._settings.databricks_server_hostname,
            http_path=self._settings.databricks_http_path,
            access_token=self._settings.DATABRICKS_TOKEN,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, query: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        """Execute a SQL statement without returning rows."""
        try:
            with self._connection() as conn:
                with conn.cursor() as cursor:
                    if parameters is None:
                        cursor.execute(query)
                    else:
                        cursor.execute(query, parameters)
        except Exception as exc:  # pragma: no cover - network/integration path
            if self._logger:
                if _is_permission_error(exc):
                    self._logger.error("Databricks execute failed: %s", _first_error_line(exc))
                else:
                    self._logger.exception("Databricks execute failed")
            raise DatabricksSQLError(str(exc)) from exc

    def fetch_all(
        self, query: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a SQL query and return rows as dictionaries."""
        try:
            with self._connection() as conn:
                with conn.cursor() as cursor:
                    if parameters is None:
                        cursor.execute(query)
                    else:
                        cursor.execute(query, parameters)
                    columns = [item[0].lower() for item in cursor.description]
                    rows = cursor.fetchall()
        except Exception as exc:  # pragma: no cover - network/integration path
            if self._logger:
                if _is_permission_error(exc):
                    self._logger.error("Databricks fetch_all failed: %s", _first_error_line(exc))
                else:
                    self._logger.exception("Databricks fetch_all failed")
            raise DatabricksSQLError(str(exc)) from exc
        return [dict(zip(columns, row)) for row in rows]

    def fetch_one(
        self, query: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Return first row from query or None."""
        rows = self.fetch_all(query, parameters)
        return rows[0] if rows else None


def sql_quote(value: str | None) -> str:
    """Escape SQL string literals."""
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def sql_bool(value: bool) -> str:
    """Render SQL booleans."""
    return "TRUE" if value else "FALSE"


def sql_array(values: Sequence[str]) -> str:
    """Render ARRAY<STRING> SQL literal expression."""
    if not values:
        return "array()"
    items = ", ".join(sql_quote(v) for v in values)
    return f"array({items})"


def sql_map(values: Mapping[str, str]) -> str:
    """Render MAP<STRING,STRING> SQL literal expression."""
    if not values:
        return "map_from_arrays(array(), array())"
    pairs = []
    for key, value in values.items():
        pairs.append(sql_quote(key))
        pairs.append(sql_quote(value))
    return f"map({', '.join(pairs)})"


def _first_error_line(exc: Exception) -> str:
    message = str(exc).strip()
    return message.splitlines()[0] if message else exc.__class__.__name__


def _is_permission_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "insufficient permissions" in message
        or "insufficient privileges" in message
        or "permission_denied" in message
        or "use catalog" in message
    )
