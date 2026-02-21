#!/usr/bin/env python3
"""
Restore test script for isolated restore validation.

This script restores a PostgreSQL backup and runs validation checks.
Designed for use in the isolated restore-test-setup environment.
"""

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, "/app/src")

from backup_postgres.config.settings import load_settings
from backup_postgres.utils.logging import setup_logging, get_logger
from backup_postgres.core.restore import RestoreManager
from backup_postgres.core.metadata import load_metadata

logger = get_logger(__name__)


def main() -> int:
    """
    Main entry point for restore test.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    parser = argparse.ArgumentParser(
        description="PostgreSQL Restore Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--backup-file",
        required=True,
        help="Path to backup .dump file",
    )

    parser.add_argument(
        "--output",
        "-o",
        default="/results/validation-report.json",
        help="Output path for validation report",
    )

    parser.add_argument(
        "--no-drop-schema",
        action="store_true",
        help="Don't drop schema before restore",
    )

    args = parser.parse_args()

    # Setup logging
    settings = load_settings()
    setup_logging(settings, use_json=False)

    # Create output directory
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    backup_path = Path(args.backup_file)

    if not backup_path.exists():
        logger.error(f"Backup file not found: {backup_path}")
        return 1

    logger.info("=" * 60)
    logger.info("RESTORE TEST")
    logger.info("=" * 60)
    logger.info(f"Backup: {backup_path}")
    logger.info(f"Output: {output_path}")
    logger.info(f"Database: {settings.postgres.pg_database}")
    logger.info(f"Host: {settings.postgres.pg_host}")
    logger.info("")

    # Initialize restore manager
    restore_manager = RestoreManager(settings.postgres)

    # Find metadata file
    metadata_path = backup_path.with_suffix(".json")

    # Execute restore
    logger.info("Starting restore...")
    result = restore_manager.restore_backup(
        backup_path=backup_path,
        metadata_path=metadata_path if metadata_path.exists() else None,
        drop_schema=not args.no_drop_schema,
    )

    # Generate report
    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "backup_file": str(backup_path),
        "database": settings.postgres.pg_database,
        "restore_success": result.success,
        "restore_duration_seconds": result.duration_seconds,
        "validation_passed": result.validation_passed,
        "validation_errors": result.validation_errors,
        "error": result.error,
    }

    if result.success:
        logger.info(f"Restore completed in {result.duration_seconds:.2f}s")

        if result.validation_passed:
            logger.info("Validation: PASSED")
        else:
            logger.warning("Validation: FAILED")
            for error in result.validation_errors:
                logger.warning(f"  - {error}")

        # Write report
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"Report written to: {output_path}")

        # Return 0 if validation passed, 1 if failed
        return 0 if result.validation_passed else 1

    else:
        logger.error(f"Restore failed: {result.error}")

        # Write failure report
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)

        return 1


if __name__ == "__main__":
    sys.exit(main())
