"""
das.logger.logger

Example of a simple logger implementation. Currently writes log to stdout.
The log function logs the caller function to enable notifying DaS model usage.

Primary use case: Databricks job tasks.

TODO:
- Make logger initialization occure on caller side.
- Add support for namespaced log configuration.
- Improve log formatting by separating use cases:
    - model usage log
    - other logging use cases.
"""

import inspect
import logging
import os
from enum import IntEnum
from pathlib import Path


class LOG_LEVEL(IntEnum):
    NOTSET = 0
    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40
    CRITICAL = 50


def get_log_level_from_env(default: LOG_LEVEL = LOG_LEVEL.INFO) -> LOG_LEVEL:
    """
    Read log level from environment variable LOG_LEVEL.
    Supports names (DEBUG, INFO, etc.) or integers.
    Prints a warning if an invalid value is provided.
    """
    raw = os.getenv("LOG_LEVEL")
    if raw is None:
        return default

    raw = raw.strip()

    # Try integer.
    if raw.isdigit():
        try:
            return LOG_LEVEL(int(raw))
        except ValueError:
            print(f"[WARN] Unknown numeric log level: {raw}. Falling back to default: {default.name}")
            return default

    # Try named log level.
    try:
        return LOG_LEVEL[raw.upper()]
    except KeyError:
        print(f"[WARN] Unknown log level: {raw}. Falling back to default: {default.name}")
        return default


def _get_caller_path(levels: int = 3) -> str:
    stack = inspect.stack()[3]
    function_name = stack.function
    filepath = Path(stack.filename)
    short_path = "/".join(filepath.parts[-levels:])
    return f"{short_path}:{function_name}"


def _setup_logger(level: LOG_LEVEL | int | None = None) -> logging.Logger:
    LOGGER_NAME = "das_logger"
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    # always reset level to whatever the caller passed
    logger.setLevel(level if level is not None else get_log_level_from_env())
    return logger


def _log(level: LOG_LEVEL, message: str | None = None):
    logger = _setup_logger()
    msg = f" - {message}" if message else ""
    logger.log(level, f"{_get_caller_path()}{msg}")


# Public logging API.
def log_debug(msg: str | None = None):
    _log(LOG_LEVEL.DEBUG, msg)


def log_info(msg: str | None = None):
    _log(LOG_LEVEL.INFO, msg)


def log_warn(msg: str | None = None):
    _log(LOG_LEVEL.WARN, msg)


def log_warning(msg: str | None = None):
    log_warn(msg)


def log_error(msg: str | None = None):
    _log(LOG_LEVEL.ERROR, msg)


def log_critical(msg: str | None = None):
    _log(LOG_LEVEL.CRITICAL, msg)
