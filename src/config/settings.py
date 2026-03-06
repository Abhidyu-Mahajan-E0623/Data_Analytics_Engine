"""Application settings loaded from .env."""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Azure OpenAI
    AZURE_OPENAI_ENDPOINT: str
    AZURE_OPENAI_API_KEY: str
    AZURE_OPENAI_API_VERSION: str = "2024-02-15-preview"
    AZURE_OPENAI_CHAT_DEPLOYMENT: str
    AZURE_OPENAI_EMBED_DEPLOYMENT: str | None = None

    # Databricks
    DATABRICKS_HOST: str
    DATABRICKS_TOKEN: str
    DATABRICKS_SQL_WAREHOUSE_ID: str = Field(min_length=1)
    DATABRICKS_CATALOG: str = "dev_analytics"
    DATABRICKS_SCHEMA_DOMAIN: str = "sales"
    DATABRICKS_SCHEMA_MONITORING: str = "monitoring"

    # Behavior
    DEFAULT_TOP_K: int = 10
    OUTPUT_TIMEZONE: str = "UTC"

    APP_NAME: str = "schema-maker"
    APP_VERSION: str = "0.1.0"

    @field_validator("AZURE_OPENAI_ENDPOINT", "DATABRICKS_HOST")
    @classmethod
    def validate_https_url(cls, value: str) -> str:
        """Ensure endpoint-like settings are https URLs."""
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("must be a valid https URL")
        return value.rstrip("/")

    @field_validator("DEFAULT_TOP_K")
    @classmethod
    def validate_top_k(cls, value: int) -> int:
        """Require sane top-k values."""
        if value < 1:
            raise ValueError("DEFAULT_TOP_K must be >= 1")
        return value

    @property
    def databricks_server_hostname(self) -> str:
        """Databricks SQL connector expects hostname without scheme."""
        parsed = urlparse(self.DATABRICKS_HOST)
        return parsed.netloc

    @property
    def databricks_http_path(self) -> str:
        """Build SQL warehouse http path."""
        warehouse = self.DATABRICKS_SQL_WAREHOUSE_ID.strip()
        if warehouse.startswith("/"):
            return warehouse
        return f"/sql/1.0/warehouses/{warehouse}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache settings."""
    return Settings()


def load_settings_or_raise() -> Settings:
    """Load settings with a short error message."""
    try:
        return get_settings()
    except ValidationError as exc:  # pragma: no cover - exercised via CLI
        raise RuntimeError(
            "Failed to load settings from .env. Run `cp .env.example .env` and fill values."
        ) from exc
