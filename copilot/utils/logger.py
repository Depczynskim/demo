"""Utility for consistent logging across Copilot modules."""
import logging
import os


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger with basic configuration.

    Log level can be controlled via COPILOT_LOG_LEVEL env var. Default INFO.
    """
    level_str = os.getenv("COPILOT_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    # Configure root logger only once.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            level=level,
        )
    logger = logging.getLogger(name or "copilot")
    logger.setLevel(level)
    return logger 