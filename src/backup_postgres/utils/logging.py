"""
Structured logging configuration with JSON output.

Provides consistent logging format across the application.
"""

import json
import logging
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from backup_postgres.config.settings import Settings


class JsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as JSON objects with timestamp, level, logger, and message.
    """

    def __init__(self, service_name: str = "backup-postgres") -> None:
        """Initialize JSON formatter."""
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string
        """
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": self.service_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        if hasattr(record, "backup_type"):
            log_obj["backup_type"] = record.backup_type  # type: ignore
        if hasattr(record, "database"):
            log_obj["database"] = record.database  # type: ignore
        if hasattr(record, "file_size"):
            log_obj["file_size"] = record.file_size  # type: ignore

        return json.dumps(log_obj)


class TextFormatter(logging.Formatter):
    """
    Text formatter for human-readable logs.

    Simple text format for console output when JSON is not desired.
    """

    def __init__(self) -> None:
        """Initialize text formatter."""
        super().__init__(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging(
    settings: Settings, use_json: bool = True, service_name: str = "backup-postgres"
) -> logging.Logger:
    """
    Configure application logging.

    Sets up console and file handlers with appropriate formatters.

    Args:
        settings: Application settings
        use_json: If True, use JSON formatter; otherwise use text formatter
        service_name: Service name for log identification

    Returns:
        Configured logger instance
    """
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, settings.logging.log_level))

    # Clear existing handlers
    logger.handlers.clear()

    # Choose formatter
    formatter = JsonFormatter(service_name) if use_json else TextFormatter()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, settings.logging.log_level))
    logger.addHandler(console_handler)

    # File handler with rotation (if configured)
    if settings.logging.log_file:
        log_path = Path(settings.logging.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=settings.logging.log_max_bytes,
            backupCount=settings.logging.log_backup_count,
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, settings.logging.log_level))
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Args:
        name: Logger name (typically __name__ from calling module)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_execution_time(func: callable) -> callable:
    """
    Decorator to log function execution time.

    Args:
        func: Function to decorate

    Returns:
        Wrapped function with execution time logging
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Execute function and log execution time."""
        logger = get_logger(func.__module__)
        import time

        start = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.time() - start
            logger.debug(f"{func.__name__} executed in {elapsed:.2f}s")

    return wrapper
