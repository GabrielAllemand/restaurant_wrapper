from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.config.settings import settings


_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _build_stream_handler() -> logging.StreamHandler:
    """
    Handler console avec format homogène.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    return handler


def _build_file_handler(log_file: Path) -> logging.FileHandler:
    """
    Handler fichier pour conserver l'historique des exécutions.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    return handler


def configure_logging() -> None:
    """
    Configure le logger racine une seule fois.
    """
    root_logger = logging.getLogger()

    if getattr(configure_logging, "_configured", False):
        return

    root_logger.setLevel(settings.pipeline.log_level)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    root_logger.addHandler(_build_stream_handler())
    root_logger.addHandler(_build_file_handler(settings.paths.logs_dir / "pipeline.log"))

    configure_logging._configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger nommé, après configuration globale.
    """
    configure_logging()
    return logging.getLogger(name)