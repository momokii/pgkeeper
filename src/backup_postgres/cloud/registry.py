"""
Upload registry for tracking backups uploaded to cloud storage.

Prevents duplicate uploads by tracking what has been uploaded.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backup_postgres.cloud.gcs_storage import BackupInfo

logger = logging.getLogger(__name__)


class UploadRegistry:
    """
    Track uploaded backups to avoid duplicates.

    Maintains same format as current .upload_registry.json
    but with GCS-specific field names.
    """

    DEFAULT_REGISTRY_PATH = Path("/backups/.upload_registry.json")

    def __init__(self, registry_path: Path | None = None) -> None:
        """
        Initialize upload registry.

        Args:
            registry_path: Path to registry file (defaults to /backups/.upload_registry.json)
        """
        self.registry_path = registry_path or self.DEFAULT_REGISTRY_PATH
        self._data: dict[str, Any] = {"uploaded": {}, "last_updated": ""}
        self._load()

    def _load(self) -> None:
        """Load or create registry."""
        if self.registry_path.exists():
            try:
                with open(self.registry_path, "r") as f:
                    self._data = json.load(f)
                logger.info(f"Loaded registry from: {self.registry_path}")
                logger.debug(f"Registry contains {len(self._data['uploaded'])} entries")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid registry JSON, starting fresh: {e}")
                self._data = {"uploaded": {}, "last_updated": ""}
            except IOError as e:
                logger.error(f"Failed to load registry: {e}")
                self._data = {"uploaded": {}, "last_updated": ""}
        else:
            logger.info("Registry does not exist, creating new")

    def _save(self) -> None:
        """Persist registry to disk."""
        try:
            self._data["last_updated"] = datetime.now(UTC).isoformat()

            # Create parent directory if needed
            self.registry_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self.registry_path, "w") as f:
                json.dump(self._data, f, indent=2)

            logger.debug(f"Registry saved: {len(self._data['uploaded'])} entries")

        except IOError as e:
            logger.error(f"Failed to save registry: {e}")

    def is_uploaded(
        self,
        backup_type: str,
        filename: str,
        checksum: str,
    ) -> str | None:
        """
        Check if backup has been uploaded.

        Args:
            backup_type: Type of backup ("daily", "weekly", "manual")
            filename: Backup filename
            checksum: SHA-256 checksum

        Returns:
            GCS key if uploaded and checksum matches, None otherwise
        """
        key = f"{backup_type}/{filename}"
        entry = self._data["uploaded"].get(key)

        if entry:
            stored_checksum = entry.get("checksum_sha256", "")
            if stored_checksum == checksum:
                logger.debug(f"Backup already uploaded: {key}")
                return entry.get("gcs_key")
            else:
                logger.warning(f"Checksum mismatch for {key}, will re-upload")

        return None

    def mark_uploaded(
        self,
        backup_type: str,
        filename: str,
        checksum: str,
        gcs_key: str,
    ) -> None:
        """
        Mark backup as uploaded.

        Args:
            backup_type: Type of backup
            filename: Backup filename
            checksum: SHA-256 checksum
            gcs_key: GCS key where backup was uploaded
        """
        key = f"{backup_type}/{filename}"

        self._data["uploaded"][key] = {
            "filename": filename,
            "checksum_sha256": checksum,
            "uploaded_at": datetime.now(UTC).isoformat(),
            "gcs_key": gcs_key,
        }

        self._save()
        logger.info(f"Marked as uploaded: {key} -> {gcs_key}")

    def sync_from_gcs(
        self,
        cloud_backups: list[BackupInfo],
        prefix: str,
    ) -> int:
        """
        Rebuild registry from GCS objects.

        Prevents re-upload when registry is deleted or corrupted.

        Args:
            cloud_backups: List of backups from GCS
            prefix: GCS backup prefix (e.g., "backups/postgres")

        Returns:
            Number of entries synced
        """
        sync_count = 0
        prefix_slash = prefix.rstrip("/") + "/"

        for backup in cloud_backups:
            # Extract registry key from GCS key
            if not backup.key.startswith(prefix_slash):
                continue

            parts = backup.key[len(prefix_slash):].split("/", 1)
            if len(parts) != 2:
                continue

            backup_type, filename = parts
            if backup_type not in ("daily", "weekly", "manual"):
                continue

            registry_key = f"{backup_type}/{filename}"

            # Only add if not already present
            if registry_key not in self._data["uploaded"]:
                self._data["uploaded"][registry_key] = {
                    "filename": filename,
                    "checksum_sha256": backup.etag or "",  # Use etag as proxy
                    "uploaded_at": backup.last_modified.isoformat(),
                    "gcs_key": backup.key,
                    "_synced_from_cloud": True,
                }
                sync_count += 1
                logger.debug(f"Synced from cloud: {registry_key}")

        if sync_count > 0:
            self._save()
            logger.info(f"Synced {sync_count} entries from GCS")

        return sync_count

    def remove_entry(self, backup_type: str, filename: str) -> bool:
        """
        Remove an entry from the registry.

        Args:
            backup_type: Type of backup
            filename: Backup filename

        Returns:
            True if entry was removed, False if not found
        """
        key = f"{backup_type}/{filename}"

        if key in self._data["uploaded"]:
            del self._data["uploaded"][key]
            self._save()
            logger.info(f"Removed from registry: {key}")
            return True

        logger.debug(f"Entry not found in registry: {key}")
        return False

    def get_uploaded_count(self) -> int:
        """Return number of tracked uploads."""
        return len(self._data["uploaded"])

    def list_uploaded(self, backup_type: str | None = None) -> list[dict[str, Any]]:
        """
        List uploaded backups.

        Args:
            backup_type: Optional filter by backup type

        Returns:
            List of upload entries
        """
        uploaded = []

        for key, entry in self._data["uploaded"].items():
            entry_type, _ = key.split("/", 1) if "/" in key else ("unknown", key)

            if backup_type is None or entry_type == backup_type:
                uploaded.append(
                    {
                        "key": key,
                        "filename": entry["filename"],
                        "checksum": entry.get("checksum_sha256", ""),
                        "uploaded_at": entry["uploaded_at"],
                        "gcs_key": entry.get("gcs_key", ""),
                    }
                )

        return uploaded

    def clear(self) -> None:
        """Clear all registry entries."""
        self._data["uploaded"] = {}
        self._save()
        logger.warning("Registry cleared")

    def to_dict(self) -> dict[str, Any]:
        """Return registry as dictionary."""
        return self._data.copy()
