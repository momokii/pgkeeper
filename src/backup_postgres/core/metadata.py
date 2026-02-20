"""
Metadata generation for PostgreSQL backups.

Generates JSON metadata matching the exact format from the current system.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from backup_postgres.core.models import (
    BackupInfo,
    BackupMetadata,
    MigrationInfo,
    TableCounts,
)
from backup_postgres.utils.checksum import calculate_sha256

logger = logging.getLogger(__name__)


def generate_backup_filename(
    base_name: str,
    backup_type: str,
    migration_version: int,
    timestamp: datetime | None = None,
) -> tuple[str, str]:
    """
    Generate backup filename and metadata filename.

    Format: {BASE_NAME}_{TIMESTAMP}_v{MIGRATION_VERSION}_{TYPE}.dump

    Example: postgres_db_20260211_030316_v7_daily.dump

    Args:
        base_name: Base name for the backup (e.g., "postgres_db")
        backup_type: Type of backup ("daily", "weekly", "manual")
        migration_version: Migration schema version
        timestamp: Timestamp to use (defaults to now)

    Returns:
        Tuple of (dump_filename, json_filename)
    """
    if timestamp is None:
        timestamp = datetime.now(UTC)

    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    base = f"{base_name}_{ts_str}_v{migration_version}_{backup_type}"

    return f"{base}.dump", f"{base}.json"


def generate_metadata_dict(
    backup_info: BackupInfo,
    migration_info: MigrationInfo,
    table_counts: TableCounts,
    checksum: str,
) -> dict:
    """
    Generate metadata dictionary matching EXACT schema.

    Maintains 100% compatibility with current metadata format.

    Args:
        backup_info: Backup information
        migration_info: Migration information
        table_counts: Table row counts
        checksum: SHA-256 checksum of backup file

    Returns:
        Dictionary with metadata structure
    """
    return {
        "backup_info": {
            "timestamp": backup_info.timestamp.isoformat() + "Z",
            "type": backup_info.type,
            "database": backup_info.database,
            "filename": backup_info.filename,
            "size_bytes": backup_info.size_bytes,
            "checksum_sha256": checksum,
        },
        "migration_info": {
            "version": migration_info.version,
            "dirty": migration_info.dirty,
        },
        "table_counts": table_counts.to_dict(),
        "enum_types": [
            "ioc_type",
            "scanner_type",
            "scan_policy_type",
            "users_role",
            "firewall_types",
            "action_types",
        ],
    }


def save_metadata(metadata_path: Path, metadata_dict: dict) -> None:
    """
    Save metadata to JSON file.

    Args:
        metadata_path: Path where metadata will be saved
        metadata_dict: Metadata dictionary to save

    Raises:
        IOError: If file cannot be written
    """
    try:
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, "w") as f:
            json.dump(metadata_dict, f, indent=2)
        logger.info(f"Metadata saved to: {metadata_path}")
    except IOError as e:
        logger.error(f"Failed to save metadata to {metadata_path}: {e}")
        raise


def load_metadata(metadata_path: Path) -> dict | None:
    """
    Load metadata from JSON file.

    Args:
        metadata_path: Path to metadata file

    Returns:
        Metadata dictionary, or None if file doesn't exist

    Raises:
        IOError: If file cannot be read
        json.JSONDecodeError: If file is not valid JSON
    """
    if not metadata_path.exists():
        logger.warning(f"Metadata file not found: {metadata_path}")
        return None

    try:
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        logger.info(f"Metadata loaded from: {metadata_path}")
        return metadata
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in metadata file {metadata_path}: {e}")
        raise
    except IOError as e:
        logger.error(f"Failed to load metadata from {metadata_path}: {e}")
        raise


def calculate_file_size(file_path: Path) -> int:
    """
    Calculate file size in bytes.

    Args:
        file_path: Path to file

    Returns:
        File size in bytes

    Raises:
        IOError: If file cannot be accessed
    """
    try:
        return file_path.stat().st_size
    except OSError as e:
        logger.error(f"Failed to get file size for {file_path}: {e}")
        raise


def create_backup_metadata(
    backup_path: Path,
    backup_type: str,
    database: str,
    migration_info: MigrationInfo,
    table_counts: TableCounts,
) -> BackupMetadata:
    """
    Create complete backup metadata for a backup file.

    Args:
        backup_path: Path to backup .dump file
        backup_type: Type of backup
        database: Database name
        migration_info: Migration information
        table_counts: Table row counts

    Returns:
        BackupMetadata object
    """
    checksum = calculate_sha256(backup_path)
    size_bytes = calculate_file_size(backup_path)
    timestamp = datetime.now(UTC)

    backup_info = BackupInfo(
        timestamp=timestamp,
        type=backup_type,
        database=database,
        filename=backup_path.name,
        size_bytes=size_bytes,
    )

    return BackupMetadata(
        backup_info=backup_info,
        migration_info=migration_info,
        table_counts=table_counts,
        checksum_sha256=checksum,
        enum_types=[
            "ioc_type",
            "scanner_type",
            "scan_policy_type",
            "users_role",
            "firewall_types",
            "action_types",
        ],
    )
