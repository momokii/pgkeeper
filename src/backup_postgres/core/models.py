"""
Data models for backup operations.

Defines data structures for backup results, metadata, and validation results.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class MigrationInfo:
    """Migration information from schema_migrations table."""

    version: int
    dirty: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {"version": self.version, "dirty": self.dirty}


@dataclass
class BackupInfo:
    """Information about a backup."""

    timestamp: datetime
    type: str  # "daily", "weekly", "manual"
    database: str
    filename: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp.isoformat() + "Z",
            "type": self.type,
            "database": self.database,
            "filename": self.filename,
            "size_bytes": self.size_bytes,
        }


@dataclass
class BackupResult:
    """Result of a backup operation."""

    success: bool
    backup_path: Path
    metadata_path: Path
    backup_info: BackupInfo
    checksum: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "backup_path": str(self.backup_path),
            "metadata_path": str(self.metadata_path),
            "backup_info": self.backup_info.to_dict(),
            "checksum": self.checksum,
            "error": self.error,
        }


@dataclass
class RestoreResult:
    """Result of a restore operation."""

    success: bool
    backup_file: Path
    validation_passed: bool
    validation_errors: list[str]
    duration_seconds: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "backup_file": str(self.backup_file),
            "validation_passed": self.validation_passed,
            "validation_errors": self.validation_errors,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
        }


@dataclass
class ValidationResult:
    """Result of a validation check."""

    check_name: str
    passed: bool
    details: str
    expected: Any = None
    actual: Any = None


@dataclass
class TableCounts:
    """Row counts for database tables."""

    clients: int = 0
    users: int = 0
    ioc: int = 0
    group_scans: int = 0
    ioc_scans: int = 0
    virustotal_scan_results: int = 0
    scan_results_generic: int = 0
    firewalls: int = 0
    action_logs: int = 0

    def to_dict(self) -> dict[str, int]:
        """Convert to dictionary for JSON serialization."""
        return {
            "clients": self.clients,
            "users": self.users,
            "ioc": self.ioc,
            "group_scans": self.group_scans,
            "ioc_scans": self.ioc_scans,
            "virustotal_scan_results": self.virustotal_scan_results,
            "scan_results_generic": self.scan_results_generic,
            "firewalls": self.firewalls,
            "action_logs": self.action_logs,
        }


@dataclass
class BackupMetadata:
    """Complete backup metadata matching current system format."""

    backup_info: BackupInfo
    migration_info: MigrationInfo
    table_counts: TableCounts
    checksum_sha256: str
    enum_types: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "backup_info": self.backup_info.to_dict(),
            "migration_info": self.migration_info.to_dict(),
            "table_counts": self.table_counts.to_dict(),
            "enum_types": self.enum_types,
        }
