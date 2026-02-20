"""
PostgreSQL backup manager.

Handles backup creation using pg_dump via subprocess.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from backup_postgres.config.settings import PostgresConfig
from backup_postgres.core.metadata import (
    generate_backup_filename,
    generate_metadata_dict,
    save_metadata,
)
from backup_postgres.core.models import (
    BackupInfo,
    BackupMetadata,
    BackupResult,
    MigrationInfo,
    TableCounts,
)
from backup_postgres.core.retention import RetentionPolicy
from backup_postgres.utils.exceptions import BackupError
from backup_postgres.utils.subprocess import run_pg_dump, run_psql
from backup_postgres.utils.logging import log_execution_time

logger = logging.getLogger(__name__)


class BackupManager:
    """
    Manages PostgreSQL backup creation.

    Responsibilities:
    - Execute pg_dump via subprocess
    - Generate metadata JSON
    - Enforce retention policies
    """

    def __init__(
        self,
        postgres_config: PostgresConfig,
        backup_dir: Path,
        retention_daily: int = 7,
        retention_weekly: int = 4,
        compression_level: int = 9,
        backup_base_name: str = "postgres_db",
    ) -> None:
        """
        Initialize backup manager.

        Args:
            postgres_config: PostgreSQL connection configuration
            backup_dir: Base directory for backups
            retention_daily: Number of daily backups to keep
            retention_weekly: Number of weekly backups to keep
            compression_level: pg_dump compression level (0-9)
            backup_base_name: Base name for backup files (default: "postgres_db")
        """
        self.pg_config = postgres_config
        self.backup_dir = backup_dir
        self.compression_level = compression_level
        self.backup_base_name = backup_base_name
        self.retention = RetentionPolicy(
            type("obj", (object,), {
                "daily_dir": backup_dir / "daily",
                "weekly_dir": backup_dir / "weekly",
                "manual_dir": backup_dir / "manual",
                "retention_daily": retention_daily,
                "retention_weekly": retention_weekly,
            })
        )

    @log_execution_time
    def create_backup(
        self,
        backup_type: Literal["daily", "weekly", "manual"],
    ) -> BackupResult:
        """
        Create a complete backup (dump + metadata).

        Args:
            backup_type: Type of backup to create

        Returns:
            BackupResult with paths and metadata

        Raises:
            BackupError: If backup creation fails
        """
        logger.info(f"Creating {backup_type} backup for database: {self.pg_config.pg_database}")

        try:
            # 1. Get migration version from database
            migration_info = self._get_migration_info()
            logger.info(f"Migration version: {migration_info.version}")

            # 2. Generate filename
            dump_name, json_name = generate_backup_filename(
                self.backup_base_name, backup_type, migration_info.version
            )

            # Determine output directory
            if backup_type == "daily":
                output_dir = self.backup_dir / "daily"
            elif backup_type == "weekly":
                output_dir = self.backup_dir / "weekly"
            else:
                output_dir = self.backup_dir / "manual"

            output_dir.mkdir(parents=True, exist_ok=True)
            backup_path = output_dir / dump_name
            metadata_path = output_dir / json_name

            # 3. Get table counts before backup
            table_counts = self._get_table_counts()

            # 4. Run pg_dump
            logger.info(f"Running pg_dump to: {backup_path}")
            run_pg_dump(
                self.pg_config,
                backup_path,
                compression_level=self.compression_level,
                verbose=True,
            )

            # Verify backup was created
            if not backup_path.exists():
                raise BackupError(f"Backup file was not created: {backup_path}")

            # 5. Generate and save metadata
            backup_info = BackupInfo(
                timestamp=datetime.now(UTC),
                type=backup_type,
                database=self.pg_config.pg_database,
                filename=dump_name,
                size_bytes=backup_path.stat().st_size,
            )

            metadata_dict = generate_metadata_dict(
                backup_info=backup_info,
                migration_info=migration_info,
                table_counts=table_counts,
                checksum="",  # Will be filled by save_metadata
            )
            save_metadata(metadata_path, metadata_dict)

            # 6. Run retention cleanup
            logger.info("Running retention policy enforcement")
            self.retention.enforce_retention()

            logger.info(f"Backup completed successfully: {backup_path}")

            return BackupResult(
                success=True,
                backup_path=backup_path,
                metadata_path=metadata_path,
                backup_info=backup_info,
                checksum=metadata_dict["backup_info"]["checksum_sha256"],
            )

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return BackupResult(
                success=False,
                backup_path=Path(""),
                metadata_path=Path(""),
                backup_info=BackupInfo(
                    timestamp=datetime.now(UTC),
                    type=backup_type,
                    database=self.pg_config.pg_database,
                    filename="",
                    size_bytes=0,
                ),
                checksum="",
                error=str(e),
            )

    def _get_migration_info(self) -> MigrationInfo:
        """
        Query migration version from schema_migrations table.

        Returns:
            MigrationInfo with version and dirty flag

        Raises:
            BackupError: If query fails
        """
        query = """
            SELECT version, dirty
            FROM schema_migrations
            LIMIT 1;
        """

        try:
            result = run_psql(self.pg_config, query)
            if result.success and result.stdout.strip():
                parts = result.stdout.strip().split("|")
                if len(parts) == 2:
                    version = int(parts[0].strip())
                    dirty = parts[1].strip().lower() == "t"
                    return MigrationInfo(version=version, dirty=dirty)

            # Default if table doesn't exist or is empty
            logger.warning("No migration info found, using default")
            return MigrationInfo(version=0, dirty=False)

        except Exception as e:
            logger.warning(f"Failed to get migration info: {e}, using default")
            return MigrationInfo(version=0, dirty=False)

    def _get_table_counts(self) -> TableCounts:
        """
        Get row counts for all known tables.

        Returns:
            TableCounts with row counts

        Raises:
            BackupError: If query fails
        """
        tables = [
            "clients",
            "users",
            "ioc",
            "group_scans",
            "ioc_scans",
            "virustotal_scan_results",
            "scan_results_generic",
            "firewalls",
            "action_logs",
        ]

        counts = TableCounts()

        for table in tables:
            try:
                query = f"SELECT COUNT(*) FROM \"{table}\";"
                result = run_psql(self.pg_config, query)
                if result.success and result.stdout.strip():
                    count = int(result.stdout.strip())
                    setattr(counts, table, count)
                    logger.debug(f"Table {table}: {count} rows")
            except Exception as e:
                # Table might not exist, continue with others
                logger.debug(f"Could not get count for {table}: {e}")

        return counts

    def list_backups(self, backup_type: str) -> list[Path]:
        """
        List backups of a given type.

        Args:
            backup_type: Type of backup ("daily", "weekly", "manual")

        Returns:
            Sorted list of backup file paths
        """
        if backup_type == "daily":
            directory = self.backup_dir / "daily"
        elif backup_type == "weekly":
            directory = self.backup_dir / "weekly"
        elif backup_type == "manual":
            directory = self.backup_dir / "manual"
        else:
            raise ValueError(f"Invalid backup type: {backup_type}")

        if not directory.exists():
            return []

        return sorted(directory.glob("*.dump"))

    def get_latest_backup(self, backup_type: str) -> Path | None:
        """
        Get the latest backup of a given type.

        Args:
            backup_type: Type of backup ("daily", "weekly", "manual")

        Returns:
            Path to latest backup, or None if no backups exist
        """
        backups = self.list_backups(backup_type)
        return backups[-1] if backups else None
