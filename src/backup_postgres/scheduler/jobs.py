"""
Job scheduler using APScheduler.

Manages scheduled backup jobs and cloud sync operations.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backup_postgres.cloud.gcs_storage import CloudStorageManager
from backup_postgres.cloud.registry import UploadRegistry
from backup_postgres.config.settings import Settings
from backup_postgres.core.backup import BackupManager
from backup_postgres.core.metadata import load_metadata

logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Manages scheduled backup jobs using APScheduler.

    Replaces cron daemon with Python-based scheduling.
    """

    def __init__(
        self,
        settings: Settings,
        backup_manager: BackupManager,
        cloud_manager: CloudStorageManager | None = None,
        registry: UploadRegistry | None = None,
    ) -> None:
        """
        Initialize job scheduler.

        Args:
            settings: Application settings
            backup_manager: Backup manager for backup operations
            cloud_manager: Optional cloud storage manager
            registry: Optional upload registry
        """
        self.settings = settings
        self.backup_manager = backup_manager
        self.cloud_manager = cloud_manager
        self.registry = registry

        # Configure scheduler with memory jobstore
        # Jobs are recreated on each startup, so persistence isn't needed
        self.scheduler = BackgroundScheduler(timezone=UTC)

        logger.info("Job scheduler initialized")

    def start(self) -> None:
        """Start the scheduler with configured jobs."""
        logger.info("Starting scheduler...")

        # Daily backup at 2 AM UTC (matching current cron: 0 2 * * *)
        self.scheduler.add_job(
            func=self._daily_backup_with_upload,
            trigger=CronTrigger(hour=2, minute=0, timezone=UTC),
            id="daily_backup",
            name="Daily PostgreSQL Backup",
            replace_existing=True,
        )
        logger.info("Scheduled: Daily backup at 02:00 UTC")

        # Weekly backup Sunday at 3 AM UTC (matching current cron: 0 3 * * 0)
        self.scheduler.add_job(
            func=self._weekly_backup_with_upload,
            trigger=CronTrigger(day_of_week="sun", hour=3, minute=0, timezone=UTC),
            id="weekly_backup",
            name="Weekly PostgreSQL Backup",
            replace_existing=True,
        )
        logger.info("Scheduled: Weekly backup on Sunday at 03:00 UTC")

        # Cloud retention cleanup daily at 4 AM UTC (optional)
        if self.cloud_manager and self.settings.gcs.cloud_retention_enabled:
            self.scheduler.add_job(
                func=self._cloud_retention_cleanup,
                trigger=CronTrigger(hour=4, minute=0, timezone=UTC),
                id="cloud_retention",
                name="Cloud Retention Cleanup",
                replace_existing=True,
            )
            logger.info("Scheduled: Cloud retention cleanup daily at 04:00 UTC")
        elif self.cloud_manager:
            logger.info("Cloud retention cleanup: DISABLED (set GCS_RETENTION_ENABLED=true to enable)")

        # Cloud sync at configurable interval (default: 30 minutes)
        if self.cloud_manager and self.registry:
            self.scheduler.add_job(
                func=self._sync_to_cloud,
                trigger="interval",
                seconds=self.settings.scheduler.sync_interval_seconds,
                id="cloud_sync",
                name="Cloud Upload Sync",
                replace_existing=True,
            )
            logger.info(
                f"Scheduled: Cloud sync every {self.settings.scheduler.sync_interval_seconds} seconds"
            )

        # Initial cloud sync on startup
        if self.cloud_manager and self.registry:
            # Run once after a short delay
            self.scheduler.add_job(
                func=self._sync_to_cloud,
                trigger="date",
                run_date=datetime.now(UTC).replace(microsecond=0),
                id="initial_cloud_sync",
                name="Initial Cloud Sync",
            )

        self.scheduler.start()
        logger.info("Scheduler started successfully")

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the scheduler.

        Args:
            wait: If True, wait for jobs to complete
        """
        logger.info("Shutting down scheduler...")
        self.scheduler.shutdown(wait=wait)
        logger.info("Scheduler stopped")

    def _daily_backup_with_upload(self) -> None:
        """Execute daily backup and upload."""
        logger.info("=" * 50)
        logger.info("Starting DAILY backup")
        logger.info("=" * 50)

        try:
            # Create backup
            result = self.backup_manager.create_backup("daily")

            if result.success:
                logger.info(f"Daily backup created: {result.backup_path}")

                # Upload to cloud if configured
                if self.cloud_manager and self.registry:
                    self._upload_backup(result.backup_path, result.metadata_path, "daily")
            else:
                logger.error(f"Daily backup failed: {result.error}")

        except Exception as e:
            logger.error(f"Daily backup job failed: {e}", exc_info=True)

    def _weekly_backup_with_upload(self) -> None:
        """Execute weekly backup and upload."""
        logger.info("=" * 50)
        logger.info("Starting WEEKLY backup")
        logger.info("=" * 50)

        try:
            # Create backup
            result = self.backup_manager.create_backup("weekly")

            if result.success:
                logger.info(f"Weekly backup created: {result.backup_path}")

                # Upload to cloud if configured
                if self.cloud_manager and self.registry:
                    self._upload_backup(result.backup_path, result.metadata_path, "weekly")
            else:
                logger.error(f"Weekly backup failed: {result.error}")

        except Exception as e:
            logger.error(f"Weekly backup job failed: {e}", exc_info=True)

    def _sync_to_cloud(self) -> None:
        """Sync all local backups to cloud."""
        if not self.cloud_manager or not self.registry:
            logger.debug("Cloud sync skipped (not configured)")
            return

        logger.debug("Starting cloud sync...")

        try:
            sync_count = 0
            error_count = 0

            # Check each backup type
            for backup_type in ["daily", "weekly", "manual"]:
                backups = self.backup_manager.list_backups(backup_type)

                for backup_path in backups:
                    metadata_path = backup_path.with_suffix(".json")

                    if not metadata_path.exists():
                        logger.warning(f"No metadata for: {backup_path}")
                        continue

                    # Get checksum from metadata
                    try:
                        metadata = load_metadata(metadata_path)
                        if not metadata:
                            continue

                        checksum = metadata["backup_info"]["checksum_sha256"]
                        filename = backup_path.name

                        # Check if already uploaded
                        if self.registry.is_uploaded(backup_type, filename, checksum):
                            logger.debug(f"Already uploaded: {filename}")
                            continue

                        # Upload
                        if self._upload_backup(backup_path, metadata_path, backup_type):
                            sync_count += 1
                        else:
                            error_count += 1

                    except Exception as e:
                        logger.error(f"Failed to process {backup_path}: {e}")
                        error_count += 1

            if sync_count > 0:
                logger.info(f"Cloud sync completed: {sync_count} uploaded, {error_count} errors")
            else:
                logger.debug("Cloud sync completed: no new uploads")

        except Exception as e:
            logger.error(f"Cloud sync failed: {e}", exc_info=True)

    def _cloud_retention_cleanup(self) -> None:
        """Execute cloud retention cleanup for all backup types."""
        if not self.cloud_manager:
            logger.debug("Cloud retention cleanup skipped (not configured)")
            return

        logger.info("=" * 50)
        logger.info("Starting CLOUD retention cleanup")
        logger.info("=" * 50)

        try:
            total_deleted = 0

            # Clean each backup type
            for backup_type in ["daily", "weekly"]:
                # Get retention limit from settings
                if backup_type == "daily":
                    retention = self.settings.gcs.cloud_retention_daily
                else:  # weekly
                    retention = self.settings.gcs.cloud_retention_weekly

                # Enforce retention
                deleted = self.cloud_manager.enforce_retention(backup_type, retention)
                total_deleted += len(deleted)

            if total_deleted > 0:
                logger.info(f"Cloud retention cleanup complete: {total_deleted // 2} backups deleted")
            else:
                logger.debug("Cloud retention cleanup complete: no backups to delete")

        except Exception as e:
            logger.error(f"Cloud retention cleanup failed: {e}", exc_info=True)

    def _upload_backup(
        self,
        backup_path: Path,
        metadata_path: Path,
        backup_type: str,
    ) -> bool:
        """
        Upload a single backup to cloud storage.

        Args:
            backup_path: Path to .dump file
            metadata_path: Path to .json file
            backup_type: Type of backup

        Returns:
            True if upload successful, False otherwise
        """
        if not self.cloud_manager or not self.registry:
            return False

        try:
            # Load metadata for checksum
            metadata = load_metadata(metadata_path)
            if not metadata:
                logger.error(f"Failed to load metadata: {metadata_path}")
                return False

            checksum = metadata["backup_info"]["checksum_sha256"]
            filename = backup_path.name

            # Check if already uploaded
            existing = self.registry.is_uploaded(backup_type, filename, checksum)
            if existing:
                logger.debug(f"Already uploaded: {filename}")
                return True

            # Generate GCS keys
            prefix = self.settings.gcs.gcs_backup_prefix
            gcs_key = f"{prefix}/{backup_type}/{filename}"
            metadata_key = f"{prefix}/{backup_type}/{metadata_path.name}"

            # Upload backup file
            logger.info(f"Uploading: {filename}")
            result = self.cloud_manager.upload_file(
                backup_path,
                gcs_key,
            )

            if not result.success:
                logger.error(f"Upload failed: {result.error}")
                return False

            # Upload metadata file
            metadata_result = self.cloud_manager.upload_file(
                metadata_path,
                metadata_key,
            )

            if not metadata_result.success:
                logger.error(f"Metadata upload failed: {metadata_result.error}")
                return False

            # Mark as uploaded
            self.registry.mark_uploaded(backup_type, filename, checksum, gcs_key)
            logger.info(f"Upload complete: {filename}")

            return True

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False

    def get_next_run_time(self, job_id: str) -> datetime | None:
        """
        Get next run time for a job.

        Args:
            job_id: Job identifier (e.g., "daily_backup", "weekly_backup")

        Returns:
            Next run time, or None if job not found
        """
        job = self.scheduler.get_job(job_id)
        if job:
            return job.next_run_time
        return None

    def list_jobs(self) -> list[dict]:
        """
        List all scheduled jobs.

        Returns:
            List of job information dictionaries
        """
        jobs = []

        for job in self.scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                }
            )

        return jobs

    def trigger_backup(self, backup_type: str) -> bool:
        """
        Manually trigger a backup job.

        Args:
            backup_type: Type of backup to trigger ("daily", "weekly", "manual")

        Returns:
            True if triggered successfully, False otherwise
        """
        logger.info(f"Manual trigger: {backup_type} backup")

        try:
            result = self.backup_manager.create_backup(backup_type)

            if result.success:
                logger.info(f"Manual backup completed: {result.backup_path}")

                # Upload to cloud if configured
                if self.cloud_manager and self.registry:
                    self._upload_backup(result.backup_path, result.metadata_path, backup_type)

                return True
            else:
                logger.error(f"Manual backup failed: {result.error}")
                return False

        except Exception as e:
            logger.error(f"Manual backup failed: {e}")
            return False
