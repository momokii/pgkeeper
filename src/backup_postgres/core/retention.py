"""
Backup retention policy enforcement.

Maintains the same retention logic as cleanup.sh:
- Daily: Keep 7 most recent
- Weekly: Keep 4 most recent
- Manual: No automatic cleanup
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from backup_postgres.config.settings import BackupConfig
from backup_postgres.utils.exceptions import RetentionError

logger = logging.getLogger(__name__)


@dataclass
class RetentionReport:
    """Report of retention policy enforcement."""

    removed_daily: List[Path] = field(default_factory=list)
    removed_weekly: List[Path] = field(default_factory=list)
    kept_daily: int = 0
    kept_weekly: int = 0

    @property
    def total_removed(self) -> int:
        """Total number of files removed."""
        return len(self.removed_daily) + len(self.removed_weekly)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "removed_daily": [str(p) for p in self.removed_daily],
            "removed_weekly": [str(p) for p in self.removed_weekly],
            "kept_daily": self.kept_daily,
            "kept_weekly": self.kept_weekly,
            "total_removed": self.total_removed,
        }


class RetentionPolicy:
    """
    Manages backup retention enforcement.

    Maintains the same retention logic as cleanup.sh:
    - Daily: Keep 7 most recent
    - Weekly: Keep 4 most recent
    - Manual: No automatic cleanup
    """

    def __init__(self, config: BackupConfig) -> None:
        """
        Initialize retention policy.

        Args:
            config: Backup configuration
        """
        self.config = config
        self.retention_daily = config.retention_daily
        self.retention_weekly = config.retention_weekly
        self.daily_dir = config.daily_dir
        self.weekly_dir = config.weekly_dir
        self.manual_dir = config.manual_dir

    def enforce_retention(self) -> RetentionReport:
        """
        Enforce retention policy on local backups.

        Removes oldest backups exceeding retention limits.
        Both .dump and .json files are removed together.

        Returns:
            RetentionReport with details of actions taken

        Raises:
            RetentionError: If cleanup fails
        """
        logger.info("Enforcing retention policy")

        report = RetentionReport()

        # Clean daily backups
        try:
            removed = self._cleanup_directory(self.daily_dir, self.retention_daily)
            report.removed_daily = removed
            report.kept_daily = self._count_backups(self.daily_dir) - len(removed) // 2
            logger.info(f"Daily retention: removed {len(removed) // 2} backups")
        except Exception as e:
            logger.error(f"Failed to clean daily backups: {e}")
            raise RetentionError(f"Daily cleanup failed: {e}") from e

        # Clean weekly backups
        try:
            removed = self._cleanup_directory(self.weekly_dir, self.retention_weekly)
            report.removed_weekly = removed
            report.kept_weekly = self._count_backups(self.weekly_dir) - len(removed) // 2
            logger.info(f"Weekly retention: removed {len(removed) // 2} backups")
        except Exception as e:
            logger.error(f"Failed to clean weekly backups: {e}")
            raise RetentionError(f"Weekly cleanup failed: {e}") from e

        logger.info(f"Retention enforcement complete: {report.total_removed // 2} backups removed")
        return report

    def _cleanup_directory(self, directory: Path, retention: int) -> List[Path]:
        """
        Remove oldest backups exceeding retention limit.

        Args:
            directory: Directory to clean
            retention: Number of backups to keep

        Returns:
            List of removed file paths (both .dump and .json)
        """
        if not directory.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return []

        # Get all .dump files sorted by name (which includes timestamp)
        dump_files = sorted(directory.glob("*.dump"))

        if len(dump_files) <= retention:
            logger.debug(f"No cleanup needed for {directory}: {len(dump_files)} <= {retention}")
            return []

        # Identify oldest files to remove
        to_remove = dump_files[:-retention]
        removed: List[Path] = []

        for dump_file in to_remove:
            try:
                # Remove .dump file
                dump_file.unlink()
                removed.append(dump_file)
                logger.debug(f"Removed: {dump_file}")

                # Remove corresponding .json file
                json_file = dump_file.with_suffix(".json")
                if json_file.exists():
                    json_file.unlink()
                    removed.append(json_file)
                    logger.debug(f"Removed: {json_file}")
                else:
                    logger.warning(f"Metadata file not found: {json_file}")

            except OSError as e:
                logger.error(f"Failed to remove {dump_file}: {e}")
                # Continue with other files

        return removed

    def _count_backups(self, directory: Path) -> int:
        """
        Count number of backups in directory.

        Args:
            directory: Directory to count

        Returns:
            Number of .dump files
        """
        if not directory.exists():
            return 0
        return len(list(directory.glob("*.dump")))

    def get_backup_count(self, backup_type: str) -> int:
        """
        Get number of backups for a given type.

        Args:
            backup_type: Type of backup ("daily", "weekly", "manual")

        Returns:
            Number of backups

        Raises:
            ValueError: If backup_type is invalid
        """
        if backup_type == "daily":
            return self._count_backups(self.daily_dir)
        elif backup_type == "weekly":
            return self._count_backups(self.weekly_dir)
        elif backup_type == "manual":
            return self._count_backups(self.manual_dir)
        else:
            raise ValueError(f"Invalid backup type: {backup_type}")

    def list_backups(self, backup_type: str) -> List[Path]:
        """
        List backups for a given type.

        Args:
            backup_type: Type of backup ("daily", "weekly", "manual")

        Returns:
            Sorted list of backup file paths

        Raises:
            ValueError: If backup_type is invalid
        """
        if backup_type == "daily":
            directory = self.daily_dir
        elif backup_type == "weekly":
            directory = self.weekly_dir
        elif backup_type == "manual":
            directory = self.manual_dir
        else:
            raise ValueError(f"Invalid backup type: {backup_type}")

        if not directory.exists():
            return []

        return sorted(directory.glob("*.dump"))
