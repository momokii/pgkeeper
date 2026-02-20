"""
Google Cloud Storage manager for backup operations.

Handles upload, download, and listing operations with GCS.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from google.api_core import retry
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

from backup_postgres.config.settings import GCSConfig
from backup_postgres.utils.exceptions import (
    CloudDownloadError,
    CloudStorageError,
    CloudUploadError,
)

logger = logging.getLogger(__name__)


@dataclass
class BackupInfo:
    """Information about a backup in cloud storage."""

    key: str
    filename: str
    backup_type: str
    size_bytes: int
    last_modified: datetime
    etag: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "filename": self.filename,
            "backup_type": self.backup_type,
            "size_bytes": self.size_bytes,
            "last_modified": self.last_modified.isoformat(),
            "etag": self.etag,
        }


@dataclass
class UploadResult:
    """Result of an upload operation."""

    success: bool
    key: str
    size_bytes: int
    etag: str | None = None
    error: str | None = None


@dataclass
class DownloadResult:
    """Result of a download operation."""

    success: bool
    key: str
    local_path: Path
    size_bytes: int
    error: str | None = None


class CloudStorageManager:
    """
    Manages Google Cloud Storage operations.

    Responsibilities:
    - Upload backups to GCS
    - Download backups from GCS
    - List available backups
    - Verify upload integrity
    """

    # Default retry configuration
    DEFAULT_RETRY_MAX = 3
    DEFAULT_TIMEOUT = 300  # 5 minutes

    def __init__(self, config: GCSConfig) -> None:
        """
        Initialize GCS storage manager.

        Args:
            config: GCS configuration

        Raises:
            CloudStorageError: If client initialization fails
        """
        self.config = config
        self.bucket_name = config.gcs_bucket_name
        self.backup_prefix = config.gcs_backup_prefix

        try:
            # Initialize GCS client from service account JSON
            self._client = storage.Client.from_service_account_json(
                str(config.gcs_credentials_path)
            )
            self._bucket = self._client.bucket(self.bucket_name)

            # Verify bucket access
            self._bucket.exists()
            logger.info(f"Connected to GCS bucket: {self.bucket_name}")

        except Exception as e:
            error_msg = f"Failed to initialize GCS client: {e}"
            logger.error(error_msg)
            raise CloudStorageError(error_msg) from e

    def _get_retry(self) -> retry.Retry:
        """
        Get retry configuration for GCS operations.

        Returns:
            Retry configuration
        """
        return retry.Retry(
            predicate=retry.if_exception_type(GoogleCloudError),
            maximum=getattr(self.config, "gcs_upload_retry_max", self.DEFAULT_RETRY_MAX),
            deadline=self.DEFAULT_TIMEOUT,
        )

    def upload_file(
        self,
        local_path: Path,
        gcs_key: str,
        metadata: dict[str, str] | None = None,
    ) -> UploadResult:
        """
        Upload file to GCS with retry logic.

        Equivalent to TOS put_object_from_file().

        Args:
            local_path: Path to local file
            gcs_key: Destination key in GCS (e.g., "backups/postgres/daily/backup.dump")
            metadata: Optional metadata to attach to the blob

        Returns:
            UploadResult with operation details

        Raises:
            CloudUploadError: If upload fails
        """
        logger.info(f"Uploading {local_path} to gs://{self.bucket_name}/{gcs_key}")

        try:
            blob = self._bucket.blob(gcs_key)

            # Set metadata if provided
            if metadata:
                blob.metadata = metadata

            # Upload with retry
            blob.upload_from_filename(
                str(local_path),
                retry=self._get_retry(),
                timeout=self.DEFAULT_TIMEOUT,
            )

            # Reload to get final metadata
            blob.reload()

            logger.info(
                f"Upload completed: gs://{self.bucket_name}/{gcs_key} "
                f"({blob.size} bytes)"
            )

            return UploadResult(
                success=True,
                key=gcs_key,
                size_bytes=blob.size or 0,
                etag=blob.etag,
            )

        except GoogleCloudError as e:
            error_msg = f"GCS upload failed: {e}"
            logger.error(error_msg)
            return UploadResult(
                success=False,
                key=gcs_key,
                size_bytes=0,
                error=error_msg,
            )

        except Exception as e:
            error_msg = f"Upload failed: {e}"
            logger.error(error_msg)
            return UploadResult(
                success=False,
                key=gcs_key,
                size_bytes=0,
                error=error_msg,
            )

    def download_file(
        self,
        gcs_key: str,
        local_path: Path,
    ) -> DownloadResult:
        """
        Download file from GCS.

        Equivalent to TOS get_object_to_file().

        Args:
            gcs_key: Key in GCS (e.g., "backups/postgres/daily/backup.dump")
            local_path: Destination path for downloaded file

        Returns:
            DownloadResult with operation details

        Raises:
            CloudDownloadError: If download fails
        """
        logger.info(f"Downloading gs://{self.bucket_name}/{gcs_key} to {local_path}")

        try:
            # Create parent directory if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)

            blob = self._bucket.blob(gcs_key)

            # Verify blob exists
            if not blob.exists():
                error_msg = f"Blob not found: gs://{self.bucket_name}/{gcs_key}"
                logger.error(error_msg)
                return DownloadResult(
                    success=False,
                    key=gcs_key,
                    local_path=local_path,
                    size_bytes=0,
                    error=error_msg,
                )

            # Download
            blob.download_to_filename(
                str(local_path),
                retry=self._get_retry(),
                timeout=self.DEFAULT_TIMEOUT,
            )

            size = local_path.stat().st_size

            logger.info(f"Download completed: {local_path} ({size} bytes)")

            return DownloadResult(
                success=True,
                key=gcs_key,
                local_path=local_path,
                size_bytes=size,
            )

        except GoogleCloudError as e:
            error_msg = f"GCS download failed: {e}"
            logger.error(error_msg)
            return DownloadResult(
                success=False,
                key=gcs_key,
                local_path=local_path,
                size_bytes=0,
                error=error_msg,
            )

        except Exception as e:
            error_msg = f"Download failed: {e}"
            logger.error(error_msg)
            return DownloadResult(
                success=False,
                key=gcs_key,
                local_path=local_path,
                size_bytes=0,
                error=error_msg,
            )

    def list_backups(
        self,
        backup_type: str | None = None,
    ) -> list[BackupInfo]:
        """
        List backups in GCS.

        Equivalent to TOS list_objects_type2().

        Args:
            backup_type: Optional filter by type ("daily", "weekly", "manual")

        Returns:
            List of BackupInfo objects, sorted by last_modified descending

        Raises:
            CloudStorageError: If listing fails
        """
        try:
            # Build prefix
            prefix = f"{self.backup_prefix}/"
            if backup_type:
                prefix = f"{self.backup_prefix}/{backup_type}/"

            logger.debug(f"Listing backups with prefix: {prefix}")

            # List blobs
            blobs = self._bucket.list_blobs(prefix=prefix)

            backups = []

            for blob in blobs:
                # Only process .dump files
                if blob.name.endswith(".dump"):
                    # Extract backup type from path
                    parts = blob.name.replace(f"{self.backup_prefix}/", "").split("/")
                    backup_type_from_path = parts[0] if len(parts) > 1 else "unknown"
                    filename = parts[-1] if parts else blob.name.split("/")[-1]

                    backups.append(
                        BackupInfo(
                            key=blob.name,
                            filename=filename,
                            backup_type=backup_type_from_path,
                            size_bytes=blob.size or 0,
                            last_modified=blob.updated or datetime.now(),
                            etag=blob.etag,
                        )
                    )

            # Sort by last_modified descending
            backups.sort(key=lambda x: x.last_modified, reverse=True)

            logger.info(f"Found {len(backups)} backups")
            return backups

        except GoogleCloudError as e:
            error_msg = f"Failed to list backups: {e}"
            logger.error(error_msg)
            raise CloudStorageError(error_msg) from e

    def get_metadata(self, dump_key: str) -> dict | None:
        """
        Retrieve metadata JSON for a backup.

        Args:
            dump_key: Key of the .dump file

        Returns:
            Metadata dictionary, or None if not found

        Raises:
            CloudStorageError: If download fails
        """
        metadata_key = dump_key.replace(".dump", ".json")
        blob = self._bucket.blob(metadata_key)

        if not blob.exists():
            logger.warning(f"Metadata not found: {metadata_key}")
            return None

        try:
            import json

            content = blob.download_as_text(retry=self._get_retry())
            return json.loads(content)

        except Exception as e:
            logger.error(f"Failed to download metadata: {e}")
            return None

    def verify_upload(self, gcs_key: str, expected_size: int) -> bool:
        """
        Verify uploaded file size matches expected.

        Args:
            gcs_key: Key in GCS
            expected_size: Expected file size in bytes

        Returns:
            True if sizes match, False otherwise
        """
        try:
            blob = self._bucket.blob(gcs_key)
            blob.reload()  # Refresh metadata

            actual_size = blob.size or 0

            if actual_size == expected_size:
                logger.info(f"Upload verified: {gcs_key} ({actual_size} bytes)")
                return True
            else:
                logger.error(
                    f"Upload size mismatch: {gcs_key} "
                    f"expected {expected_size}, got {actual_size}"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to verify upload: {e}")
            return False

    def delete_file(self, gcs_key: str) -> bool:
        """
        Delete a file from GCS.

        Args:
            gcs_key: Key to delete

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            blob = self._bucket.blob(gcs_key)
            blob.delete()
            logger.info(f"Deleted: gs://{self.bucket_name}/{gcs_key}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete {gcs_key}: {e}")
            return False

    def test_connection(self) -> bool:
        """
        Test GCS connectivity.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get bucket metadata
            self._bucket.reload()
            logger.info("GCS connection test successful")
            return True

        except Exception as e:
            logger.error(f"GCS connection test failed: {e}")
            return False
