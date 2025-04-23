import logging
import sys
from logging.handlers import RotatingFileHandler
import os


def setup_logger(name, log_file=None, level=logging.INFO):
    """Set up logger with console and file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create formatters
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if log_file is provided)
    if log_file:
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        file_handler = RotatingFileHandler(
            f"logs/{log_file}",
            maxBytes=10485760,  # 10MB
            backupCount=5,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
