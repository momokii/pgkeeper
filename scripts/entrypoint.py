#!/usr/bin/env python3
"""
Main entry point for the PostgreSQL backup service.

Runs the scheduler and handles graceful shutdown.
"""

import signal
import sys
import logging

from backup_postgres.config.settings import load_settings, Settings
from backup_postgres.utils.logging import setup_logging, get_logger

# Add src to path for imports
sys.path.insert(0, "/app/src")

from backup_postgres.core.backup import BackupManager
from backup_postgres.cloud.gcs_storage import CloudStorageManager
from backup_postgres.cloud.registry import UploadRegistry
from backup_postgres.scheduler.jobs import JobScheduler

logger = get_logger(__name__)


class BackupService:
    """
    Main backup service.

    Manages the scheduler and handles graceful shutdown.
    """

    def __init__(self) -> None:
        """Initialize the backup service."""
        # Load settings
        self.settings = load_settings()
        logger.info("Backup service initializing...")

        # Setup logging
        setup_logging(self.settings, use_json=True)
        logger.info(f"Log level: {self.settings.logging.log_level}")

        # Initialize components
        self.backup_manager = BackupManager(
            postgres_config=self.settings.postgres,
            backup_dir=self.settings.backup.backup_dir,
            retention_daily=self.settings.backup.retention_daily,
            retention_weekly=self.settings.backup.retention_weekly,
            compression_level=self.settings.backup.compression_level,
            backup_base_name=self.settings.backup.backup_base_name,
        )

        # Initialize cloud components if configured
        self.cloud_manager = None
        self.registry = None

        if self.settings.gcs.enabled:
            try:
                self.cloud_manager = CloudStorageManager(self.settings.gcs)
                self.registry = UploadRegistry()

                # Test GCS connection
                if self.cloud_manager.test_connection():
                    logger.info("GCS connection verified")
                else:
                    logger.warning("GCS connection test failed, cloud operations may fail")

            except Exception as e:
                logger.error(f"Failed to initialize cloud storage: {e}")
                logger.warning("Continuing without cloud storage support")
        else:
            logger.info("Cloud storage not configured, local backups only")

        # Initialize scheduler
        self.scheduler = JobScheduler(
            settings=self.settings,
            backup_manager=self.backup_manager,
            cloud_manager=self.cloud_manager,
            registry=self.registry,
        )

        # Setup signal handlers for graceful shutdown
        self._shutdown = False
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown = True

    def start(self) -> None:
        """Start the backup service."""
        try:
            # Start scheduler
            self.scheduler.start()

            # Log scheduled jobs
            jobs = self.scheduler.list_jobs()
            logger.info(f"Active jobs: {len(jobs)}")
            for job in jobs:
                logger.info(f"  - {job['name']}: {job.get('next_run_time', 'N/A')}")

            logger.info("Backup service started")
            logger.info("-" * 50)

            # Keep running until shutdown signal
            import time

            while not self._shutdown:
                time.sleep(1)

        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            sys.exit(1)

        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the backup service gracefully."""
        logger.info("Stopping backup service...")
        self.scheduler.shutdown(wait=True)
        logger.info("Backup service stopped")


def main() -> int:
    """
    Main entry point.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        service = BackupService()
        service.start()
        return 0

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
