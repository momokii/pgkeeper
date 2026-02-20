"""
Checksum utilities for backup verification.

Provides SHA-256 checksum calculation for file integrity verification.
"""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Buffer size for reading files (64KB)
BUFFER_SIZE = 65536


def calculate_sha256(file_path: Path) -> str:
    """
    Calculate SHA-256 checksum of a file.

    Args:
        file_path: Path to file

    Returns:
        Hexadecimal SHA-256 checksum

    Raises:
        IOError: If file cannot be read
    """
    sha256 = hashlib.sha256()

    try:
        with open(file_path, "rb") as f:
            while True:
                data = f.read(BUFFER_SIZE)
                if not data:
                    break
                sha256.update(data)

        checksum = sha256.hexdigest()
        logger.debug(f"SHA-256 checksum for {file_path}: {checksum}")
        return checksum
    except IOError as e:
        logger.error(f"Failed to read file for checksum: {file_path}: {e}")
        raise


def verify_checksum(file_path: Path, expected_checksum: str) -> bool:
    """
    Verify file checksum matches expected value.

    Args:
        file_path: Path to file
        expected_checksum: Expected SHA-256 checksum

    Returns:
        True if checksums match, False otherwise
    """
    try:
        actual_checksum = calculate_sha256(file_path)
        matches = actual_checksum.lower() == expected_checksum.lower()

        if matches:
            logger.info(f"Checksum verified for {file_path}")
        else:
            logger.error(
                f"Checksum mismatch for {file_path}: "
                f"expected {expected_checksum}, got {actual_checksum}"
            )

        return matches
    except IOError:
        logger.error(f"Failed to verify checksum for {file_path}")
        return False
