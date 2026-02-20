"""
Configuration management using Pydantic Settings.

Loads configuration from environment variables with validation.
"""

import os
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresConfig(BaseSettings):
    """PostgreSQL connection configuration."""

    pg_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    pg_port: int = Field(default=5432, alias="POSTGRES_PORT")
    pg_user: str = Field(alias="POSTGRES_USER")
    pg_password: str = Field(alias="POSTGRES_PASSWORD")
    pg_database: str = Field(alias="POSTGRES_DB")

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("pg_port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f"Invalid port: {v}")
        return v


class BackupConfig(BaseSettings):
    """Backup configuration."""

    backup_base_name: str = Field(default="postgres_db", alias="BACKUP_BASE_NAME")
    compression_level: int = Field(
        default=9, ge=0, le=9, alias="BACKUP_COMPRESSION_LEVEL"
    )
    retention_daily: int = Field(default=7, ge=1, alias="BACKUP_RETENTION_DAILY")
    retention_weekly: int = Field(default=4, ge=1, alias="BACKUP_RETENTION_WEEKLY")
    backup_dir: Path = Field(default=Path("/backups"), alias="BACKUP_DIR")

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def daily_dir(self) -> Path:
        """Daily backup directory."""
        return self.backup_dir / "daily"

    @property
    def weekly_dir(self) -> Path:
        """Weekly backup directory."""
        return self.backup_dir / "weekly"

    @property
    def manual_dir(self) -> Path:
        """Manual backup directory."""
        return self.backup_dir / "manual"


class GCSConfig(BaseSettings):
    """Google Cloud Storage configuration."""

    gcs_bucket_name: str = Field(alias="GCS_BUCKET_NAME", default="")
    gcs_credentials_path: Path = Field(
        default=Path("/gcs-credentials/credentials.json"), alias="GCS_CREDENTIALS_PATH"
    )
    gcs_backup_prefix: str = Field(
        default="backups/postgres", alias="GCS_BACKUP_PREFIX"
    )
    gcs_upload_retry_max: int = Field(default=3, ge=1, alias="GCS_UPLOAD_RETRY_MAX")

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def enabled(self) -> bool:
        """Check if GCS is configured."""
        return bool(self.gcs_bucket_name)


class LoggingConfig(BaseSettings):
    """Logging configuration."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", alias="LOG_LEVEL"
    )
    log_file: Path | None = Field(default=None, alias="LOG_FILE")
    log_max_bytes: int = Field(default=10 * 1024 * 1024, alias="LOG_MAX_BYTES")
    log_backup_count: int = Field(default=5, alias="LOG_BACKUP_COUNT")

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class Settings:
    """
    Main settings container.

    Aggregates all configuration sections and provides validation.
    """

    def __init__(self) -> None:
        """Initialize settings from environment variables."""
        self.postgres = PostgresConfig()
        self.backup = BackupConfig()
        self.gcs = GCSConfig()
        self.logging = LoggingConfig()
        self._validate()

    def _validate(self) -> None:
        """Validate required settings."""
        # Validate PostgreSQL settings
        if not self.postgres.pg_user:
            raise ValueError("POSTGRES_USER is required")
        if not self.postgres.pg_password:
            raise ValueError("POSTGRES_PASSWORD is required")
        if not self.postgres.pg_database:
            raise ValueError("POSTGRES_DATABASE is required")

        # Validate backup directories can be created
        self.backup.backup_dir.mkdir(parents=True, exist_ok=True)
        self.backup.daily_dir.mkdir(parents=True, exist_ok=True)
        self.backup.weekly_dir.mkdir(parents=True, exist_ok=True)
        self.backup.manual_dir.mkdir(parents=True, exist_ok=True)

        # Validate GCS credentials if enabled
        if self.gcs.enabled:
            if not self.gcs.gcs_credentials_path.exists():
                raise ValueError(
                    f"GCS credentials file not found: {self.gcs.gcs_credentials_path}"
                )


def load_settings() -> Settings:
    """
    Load settings from environment variables.

    Returns:
        Settings: Validated settings instance

    Raises:
        ValueError: If required settings are missing or invalid
    """
    return Settings()
