"""
PostgreSQL subprocess utilities.

Provides wrappers for pg_dump, pg_restore, and psql commands.
"""

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backup_postgres.config.settings import PostgresConfig

from .exceptions import BackupError, RestoreError

logger = logging.getLogger(__name__)


@dataclass
class ProcessResult:
    """Result of a subprocess execution."""

    returncode: int
    stdout: str
    stderr: str
    success: bool

    @classmethod
    def from_completed(cls, result: subprocess.CompletedProcess[str]) -> "ProcessResult":
        """Create from subprocess.CompletedProcess."""
        return cls(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            success=result.returncode == 0,
        )


def run_pg_dump(
    config: PostgresConfig,
    output_path: Path,
    compression_level: int = 9,
    verbose: bool = False,
) -> ProcessResult:
    """
    Execute pg_dump to create a custom-format backup.

    Uses exact same options as current bash script:
    -Fc: Custom format (supports parallel restore)
    -ZN: Compression level (9 = max)
    -b: Include large objects
    -v: Verbose output

    Args:
        config: PostgreSQL configuration
        output_path: Path where backup will be written
        compression_level: Compression level (0-9, default 9)
        verbose: Enable verbose output

    Returns:
        ProcessResult with execution details

    Raises:
        BackupError: If pg_dump fails
    """
    env = {
        "PGPASSWORD": config.pg_password,
        "PGHOST": config.pg_host,
        "PGPORT": str(config.pg_port),
        "PGUSER": config.pg_user,
    }

    cmd = [
        "pg_dump",
        "-Fc",  # Custom format
        f"-Z{compression_level}",  # Compression
        "-b",  # Include large objects
        "-h", config.pg_host,
        "-p", str(config.pg_port),
        "-U", config.pg_user,
        "-d", config.pg_database,
        "-f", str(output_path),
    ]

    if verbose:
        cmd.append("-v")

    logger.info(f"Starting pg_dump for database: {config.pg_database}")
    logger.debug(f"Command: pg_dump -Fc -Z{compression_level} -b -h {config.pg_host} ...")

    try:
        result = subprocess.run(
            cmd,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info(f"pg_dump completed successfully: {output_path}")
        return ProcessResult.from_completed(result)
    except subprocess.CalledProcessError as e:
        error_msg = f"pg_dump failed with return code {e.returncode}"
        if e.stderr:
            error_msg += f": {e.stderr}"
        logger.error(error_msg)
        raise BackupError(error_msg) from e
    except FileNotFoundError:
        error_msg = "pg_dump command not found. Please ensure postgresql-client is installed."
        logger.error(error_msg)
        raise BackupError(error_msg) from None


def run_pg_restore(
    config: PostgresConfig,
    backup_path: Path,
    verbose: bool = False,
) -> ProcessResult:
    """
    Execute pg_restore to restore from a custom-format backup.

    Uses exact same options as current bash script:
    --no-owner: Skip ownership changes
    --no-privileges: Skip privilege changes
    --no-tablespaces: Skip tablespace settings (avoids compatibility issues)
    -v: Verbose output

    Args:
        config: PostgreSQL configuration
        backup_path: Path to backup file (.dump)
        verbose: Enable verbose output

    Returns:
        ProcessResult with execution details

    Raises:
        RestoreError: If pg_restore fails
    """
    env = {
        "PGPASSWORD": config.pg_password,
        "PGHOST": config.pg_host,
        "PGPORT": str(config.pg_port),
        "PGUSER": config.pg_user,
    }

    cmd = [
        "pg_restore",
        "-h", config.pg_host,
        "-p", str(config.pg_port),
        "-U", config.pg_user,
        "-d", config.pg_database,
        "--no-owner",
        "--no-privileges",
        "--no-tablespaces",
        "--use-set-session-authorization",
        str(backup_path),
    ]

    if verbose:
        cmd.append("-v")

    logger.info(f"Starting pg_restore from: {backup_path}")
    logger.debug(f"Command: pg_restore -h {config.pg_host} ...")

    try:
        result = subprocess.run(
            cmd,
            env=env,
            check=False,  # Don't raise exception on non-zero exit
            capture_output=True,
            text=True,
        )
        # pg_restore returns exit code 1 if there were errors, but it may have succeeded
        # Check stderr for critical errors vs warnings (e.g., "errors ignored on restore")
        if result.returncode != 0:
            stderr_lower = result.stderr.lower()
            # If "errors ignored on restore" appears, the restore likely completed despite warnings
            if "errors ignored on restore" in stderr_lower:
                logger.warning(f"pg_restore completed with warnings: {result.returncode} errors ignored")
            else:
                error_msg = f"pg_restore failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                logger.error(error_msg)
                raise RestoreError(error_msg) from None
        logger.info("pg_restore completed successfully")
        return ProcessResult.from_completed(result)
    except FileNotFoundError:
        error_msg = "pg_restore command not found. Please ensure postgresql-client is installed."
        logger.error(error_msg)
        raise RestoreError(error_msg) from None


def run_psql(
    config: PostgresConfig,
    query: str,
    database: str | None = None,
) -> ProcessResult:
    """
    Execute a SQL query via psql.

    Args:
        config: PostgreSQL configuration
        query: SQL query to execute
        database: Database name (defaults to config.pg_database)

    Returns:
        ProcessResult with query output

    Raises:
        BackupError: If psql fails
    """
    env = {
        "PGPASSWORD": config.pg_password,
        "PGHOST": config.pg_host,
        "PGPORT": str(config.pg_port),
        "PGUSER": config.pg_user,
    }

    cmd = [
        "psql",
        "-h", config.pg_host,
        "-p", str(config.pg_port),
        "-U", config.pg_user,
        "-d", database or config.pg_database,
        "-t",  # Tuple only output
        "-A",  # Unaligned output
        "-q",  # Quiet
    ]

    logger.debug(f"Executing psql query: {query[:100]}...")

    try:
        result = subprocess.run(
            cmd,
            env=env,
            input=query,
            check=True,
            capture_output=True,
            text=True,
        )
        return ProcessResult.from_completed(result)
    except subprocess.CalledProcessError as e:
        error_msg = f"psql query failed: {e.stderr}"
        logger.error(error_msg)
        raise BackupError(error_msg) from e


def check_pg_ready(config: PostgresConfig, timeout: int = 60) -> bool:
    """
    Check if PostgreSQL is ready to accept connections.

    Uses pg_isready to test connectivity.

    Args:
        config: PostgreSQL configuration
        timeout: Maximum time to wait in seconds

    Returns:
        True if PostgreSQL is ready, False otherwise
    """
    import time

    env = {
        "PGPASSWORD": config.pg_password,
        "PGHOST": config.pg_host,
        "PGPORT": str(config.pg_port),
        "PGUSER": config.pg_user,
    }

    cmd = [
        "pg_isready",
        "-h", config.pg_host,
        "-p", str(config.pg_port),
        "-U", config.pg_user,
        "-d", config.pg_database,
    ]

    start = time.time()
    while time.time() - start < timeout:
        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
            )
            # pg_isready returns 0 if accepting connections
            if result.returncode == 0:
                logger.info("PostgreSQL is ready")
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        time.sleep(2)

    logger.warning(f"PostgreSQL not ready after {timeout}s")
    return False


def verify_backup_format(backup_path: Path) -> bool:
    """
    Verify backup file format using pg_restore --list.

    Args:
        backup_path: Path to backup file

    Returns:
        True if backup format is valid

    Raises:
        RestoreError: If verification fails
    """
    cmd = ["pg_restore", "-l", str(backup_path)]

    try:
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.debug(f"Backup format verified: {backup_path}")
        return True
    except subprocess.CalledProcessError as e:
        error_msg = f"Backup format verification failed: {e.stderr}"
        logger.error(error_msg)
        raise RestoreError(error_msg) from e
    except FileNotFoundError:
        error_msg = "pg_restore command not found. Please ensure postgresql-client is installed."
        logger.error(error_msg)
        raise RestoreError(error_msg) from None
