from __future__ import annotations

from pathlib import Path
from typing import Any

from src.config.settings import settings
from src.utils.files import (
    build_timestamped_filename,
    sanitize_filename,
    write_json,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)


def save_raw_payload(
    payload: Any,
    *,
    source_name: str,
    suffix: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """
    Sauvegarde un payload brut en JSON dans le dossier raw de la source.
    """
    if output_dir is None:
        source_to_dir = {
            "overpass": settings.paths.raw_overpass_dir,
            "geocoding": settings.paths.raw_geocoding_dir,
            "sirene": settings.paths.raw_sirene_dir,
        }
        if source_name not in source_to_dir:
            raise ValueError(f"Unknown raw source directory for source_name={source_name!r}")
        output_dir = source_to_dir[source_name]

    normalized_suffix = sanitize_filename(suffix) if suffix else None
    filename = build_timestamped_filename(source_name, "json", suffix=normalized_suffix)
    output_path = output_dir / filename

    logger.info("Saving raw payload for source=%s to %s", source_name, output_path)
    return write_json(payload, output_path)