#!/usr/bin/env python3
"""
CLI commands for PostgreSQL backup system.

Provides manual commands for backup, restore, and listing operations.
"""

import argparse
import json
import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, "/app/src")

from backup_postgres.config.settings import load_settings
from backup_postgres.utils.logging import setup_logging, get_logger
from backup_postgres.core.backup import BackupManager
from backup_postgres.core.restore import RestoreManager
from backup_postgres.core.metadata import load_metadata
from backup_postgres.cloud.gcs_storage import CloudStorageManager
from backup_postgres.cloud.registry import UploadRegistry

logger = get_logger(__name__)


def cmd_backup(args) -> int:
    """Create a backup."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        backup_manager = BackupManager(
            postgres_config=settings.postgres,
            backup_dir=settings.backup.backup_dir,
            retention_daily=settings.backup.retention_daily,
            retention_weekly=settings.backup.retention_weekly,
            compression_level=settings.backup.compression_level,
            backup_base_name=settings.backup.backup_base_name,
        )

        result = backup_manager.create_backup(args.type)

        if result.success:
            print(f"Backup created: {result.backup_path}")
            print(f"Metadata: {result.metadata_path}")
            print(f"Size: {result.backup_info.size_bytes} bytes")
            print(f"Checksum: {result.checksum}")
            return 0
        else:
            print(f"Backup failed: {result.error}", file=sys.stderr)
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Backup failed: {e}", exc_info=True)
        return 1


def cmd_restore(args) -> int:
    """Restore from a backup."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        restore_manager = RestoreManager(settings.postgres)

        backup_path = Path(args.backup_file)
        metadata_path = None

        # Try to find metadata file
        if backup_path.with_suffix(".json").exists():
            metadata_path = backup_path.with_suffix(".json")

        result = restore_manager.restore_backup(
            backup_path=backup_path,
            metadata_path=metadata_path,
            drop_schema=not args.no_drop_schema,
        )

        if result.success:
            print(f"Restore completed in {result.duration_seconds:.2f}s")

            if result.validation_passed:
                print("Validation: PASSED")
            else:
                print("Validation: FAILED")
                for error in result.validation_errors:
                    print(f"  - {error}")
                return 1

            return 0
        else:
            print(f"Restore failed: {result.error}", file=sys.stderr)
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Restore failed: {e}", exc_info=True)
        return 1


def cmd_list(args) -> int:
    """List backups."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        if args.cloud:
            # List cloud backups
            if not settings.gcs.enabled:
                print("Cloud storage not configured", file=sys.stderr)
                return 1

            cloud_manager = CloudStorageManager(settings.gcs)
            backups = cloud_manager.list_backups(args.type)

            if args.json:
                data = [b.to_dict() for b in backups]
                print(json.dumps(data, indent=2))
            else:
                print(f"Cloud backups ({len(backups)}):")
                for backup in backups[:args.limit]:
                    size_mb = backup.size_bytes / (1024 * 1024)
                    print(f"  {backup.filename}")
                    print(f"    Type: {backup.backup_type}")
                    print(f"    Size: {size_mb:.2f} MB")
                    print(f"    Date: {backup.last_modified}")
                    print(f"    Key: {backup.key}")
        else:
            # List local backups
            backup_manager = BackupManager(
                postgres_config=settings.postgres,
                backup_dir=settings.backup.backup_dir,
                backup_base_name=settings.backup.backup_base_name,
            )

            backups = backup_manager.list_backups(args.type or "daily")

            if args.json:
                data = [str(b) for b in backups]
                print(json.dumps(data, indent=2))
            else:
                backup_type_str = args.type or "daily"
                print(f"Local {backup_type_str} backups ({len(backups)}):")
                for backup in backups:
                    size_mb = backup.stat().st_size / (1024 * 1024)
                    print(f"  {backup.name} - {size_mb:.2f} MB")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"List failed: {e}", exc_info=True)
        return 1


def cmd_upload(args) -> int:
    """Upload backups to cloud storage."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        if not settings.gcs.enabled:
            print("Cloud storage not configured", file=sys.stderr)
            return 1

        cloud_manager = CloudStorageManager(settings.gcs)
        registry = UploadRegistry()

        backup_manager = BackupManager(
            postgres_config=settings.postgres,
            backup_dir=settings.backup.backup_dir,
            backup_base_name=settings.backup.backup_base_name,
        )

        # Upload specified backup or all pending
        if args.file:
            backup_path = Path(args.file)
            metadata_path = backup_path.with_suffix(".json")

            if not backup_path.exists():
                print(f"Backup file not found: {backup_path}", file=sys.stderr)
                return 1

            # Determine type from path
            backup_type = "manual"
            if "daily" in str(backup_path):
                backup_type = "daily"
            elif "weekly" in str(backup_path):
                backup_type = "weekly"

            # Upload
            gcs_key = f"{settings.gcs.gcs_backup_prefix}/{backup_type}/{backup_path.name}"
            result = cloud_manager.upload_file(backup_path, gcs_key)

            if result.success:
                print(f"Uploaded: {backup_path.name}")
                return 0
            else:
                print(f"Upload failed: {result.error}", file=sys.stderr)
                return 1
        else:
            # Sync all pending uploads
            print("Syncing pending uploads...")
            sync_count = 0

            for backup_type in ["daily", "weekly", "manual"]:
                backups = backup_manager.list_backups(backup_type)

                for backup_path in backups:
                    metadata_path = backup_path.with_suffix(".json")
                    if not metadata_path.exists():
                        continue

                    try:
                        metadata = load_metadata(metadata_path)
                        if not metadata:
                            continue

                        checksum = metadata["backup_info"]["checksum_sha256"]
                        filename = backup_path.name

                        if registry.is_uploaded(backup_type, filename, checksum):
                            continue

                        gcs_key = f"{settings.gcs.gcs_backup_prefix}/{backup_type}/{filename}"
                        result = cloud_manager.upload_file(backup_path, gcs_key)

                        if result.success:
                            metadata_key = f"{settings.gcs.gcs_backup_prefix}/{backup_type}/{metadata_path.name}"
                            cloud_manager.upload_file(metadata_path, metadata_key)
                            registry.mark_uploaded(backup_type, filename, checksum, gcs_key)
                            sync_count += 1
                            print(f"  Uploaded: {filename}")

                    except Exception as e:
                        print(f"  Failed: {backup_path.name}: {e}")

            print(f"Synced {sync_count} backups")
            return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Upload failed: {e}", exc_info=True)
        return 1


def cmd_download(args) -> int:
    """Download backup from cloud storage."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        if not settings.gcs.enabled:
            print("Cloud storage not configured", file=sys.stderr)
            return 1

        cloud_manager = CloudStorageManager(settings.gcs)

        # Download
        gcs_key = args.key
        output_path = Path(args.output) if args.output else Path(".")
        output_path = output_path / gcs_key.split("/")[-1]

        result = cloud_manager.download_file(gcs_key, output_path)

        if result.success:
            print(f"Downloaded to: {output_path}")
            return 0
        else:
            print(f"Download failed: {result.error}", file=sys.stderr)
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Download failed: {e}", exc_info=True)
        return 1


def cmd_test(args) -> int:
    """Test GCS connection."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        if not settings.gcs.enabled:
            print("Cloud storage not configured", file=sys.stderr)
            return 1

        print("Testing GCS connection...")
        cloud_manager = CloudStorageManager(settings.gcs)

        if cloud_manager.test_connection():
            print("GCS connection: OK")
            print(f"Bucket: {settings.gcs.gcs_bucket_name}")
            print(f"Prefix: {settings.gcs.gcs_backup_prefix}")
            return 0
        else:
            print("GCS connection: FAILED", file=sys.stderr)
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Test failed: {e}", exc_info=True)
        return 1


def cmd_status(args) -> int:
    """Check upload status of local backups."""
    try:
        settings = load_settings()
        setup_logging(settings, use_json=False)

        backup_manager = BackupManager(
            postgres_config=settings.postgres,
            backup_dir=settings.backup.backup_dir,
            backup_base_name=settings.backup.backup_base_name,
        )

        if not settings.gcs.enabled:
            print("Cloud storage not configured - upload status unavailable", file=sys.stderr)
            print("\nLocal backups:")
            for backup_type in ["daily", "weekly", "manual"]:
                backups = backup_manager.list_backups(backup_type)
                if backups:
                    print(f"\n{backup_type.upper()}:")
                    for backup in backups:
                        size_mb = backup.stat().st_size / (1024 * 1024)
                        print(f"  - {backup.name} ({size_mb:.2f} MB)")
            return 0

        from backup_postgres.cloud.registry import UploadRegistry

        registry = UploadRegistry()
        cloud_manager = CloudStorageManager(settings.gcs)

        print("=" * 60)
        print("UPLOAD STATUS")
        print("=" * 60)

        total_local = 0
        total_uploaded = 0
        total_pending = 0

        for backup_type in ["daily", "weekly", "manual"]:
            backups = backup_manager.list_backups(backup_type)

            if not backups:
                continue

            print(f"\n{backup_type.upper()} BACKUPS:")
            print("-" * 60)

            for backup_path in backups:
                total_local += 1
                metadata_path = backup_path.with_suffix(".json")
                size_mb = backup_path.stat().st_size / (1024 * 1024)

                # Check if uploaded
                try:
                    if metadata_path.exists():
                        metadata = load_metadata(metadata_path)
                        if metadata:
                            checksum = metadata["backup_info"]["checksum_sha256"]
                            filename = backup_path.name

                            is_uploaded = registry.is_uploaded(backup_type, filename, checksum)

                            if is_uploaded:
                                total_uploaded += 1
                                print(f"  ✓ {filename}")
                                print(f"    Size: {size_mb:.2f} MB | Status: UPLOADED")
                            else:
                                total_pending += 1
                                print(f"  ✗ {filename}")
                                print(f"    Size: {size_mb:.2f} MB | Status: PENDING UPLOAD")
                        else:
                            total_pending += 1
                            print(f"  ? {filename}")
                            print(f"    Size: {size_mb:.2f} MB | Status: NO METADATA")
                    else:
                        total_pending += 1
                        print(f"  ? {filename}")
                        print(f"    Size: {size_mb:.2f} MB | Status: NO METADATA FILE")
                except Exception as e:
                    print(f"  ! {backup_path.name}: Error checking - {e}")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total local backups: {total_local}")
        print(f"Uploaded to GCS:     {total_uploaded}")
        print(f"Pending upload:      {total_pending}")

        if total_pending > 0:
            print(f"\nRun 'python /app/scripts/cli.py upload' to upload pending backups")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logger.error(f"Status check failed: {e}", exc_info=True)
        return 1


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="PostgreSQL Backup System CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Backup command
    backup_parser = subparsers.add_parser("backup", help="Create a backup")
    backup_parser.add_argument(
        "--type",
        choices=["daily", "weekly", "manual"],
        default="manual",
        help="Type of backup",
    )
    backup_parser.set_defaults(func=cmd_backup)

    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Restore from backup")
    restore_parser.add_argument("backup_file", help="Path to backup .dump file")
    restore_parser.add_argument(
        "--no-drop-schema",
        action="store_true",
        help="Don't drop schema before restore",
    )
    restore_parser.set_defaults(func=cmd_restore)

    # List command
    list_parser = subparsers.add_parser("list", help="List backups")
    list_parser.add_argument(
        "--type",
        choices=["daily", "weekly", "manual"],
        help="Filter by backup type",
    )
    list_parser.add_argument("--cloud", action="store_true", help="List cloud backups")
    list_parser.add_argument("--json", action="store_true", help="JSON output")
    list_parser.add_argument("--limit", type=int, default=20, help="Limit results")
    list_parser.set_defaults(func=cmd_list)

    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Upload backups to cloud")
    upload_parser.add_argument("--file", help="Specific file to upload (default: sync all)")
    upload_parser.set_defaults(func=cmd_upload)

    # Download command
    download_parser = subparsers.add_parser("download", help="Download from cloud")
    download_parser.add_argument("key", help="GCS key to download")
    download_parser.add_argument("--output", "-o", help="Output path")
    download_parser.set_defaults(func=cmd_download)

    # Test command
    test_parser = subparsers.add_parser("test", help="Test GCS connection")
    test_parser.set_defaults(func=cmd_test)

    # Status command
    status_parser = subparsers.add_parser("status", help="Check upload status of local backups")
    status_parser.set_defaults(func=cmd_status)

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
