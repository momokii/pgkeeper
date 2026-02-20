"""
Custom exceptions for the backup system.

Provides specific exception types for different error scenarios.
"""


class BackupError(Exception):
    """Base exception for backup operations."""

    pass


class BackupCreationError(BackupError):
    """Raised when backup creation fails."""

    pass


class RestoreError(Exception):
    """Base exception for restore operations."""

    pass


class RestoreExecutionError(RestoreError):
    """Raised when restore execution fails."""

    pass


class ValidationError(RestoreError):
    """Raised when validation check fails."""

    pass


class CloudStorageError(Exception):
    """Base exception for cloud storage operations."""

    pass


class CloudUploadError(CloudStorageError):
    """Raised when cloud upload fails."""

    pass


class CloudDownloadError(CloudStorageError):
    """Raised when cloud download fails."""

    pass


class RetentionError(BackupError):
    """Raised when retention policy enforcement fails."""

    pass


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""

    pass
