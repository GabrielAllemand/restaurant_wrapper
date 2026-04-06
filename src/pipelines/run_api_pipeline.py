from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.settings import settings
from src.loaders.save_processed import save_processed_dataframe
from src.pipelines.run_geocoding import run_geocoding_enrichment
from src.pipelines.run_overpass import run_overpass_pipeline
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_api_pipeline(
    *,
    city: str,
    country: str,
    save_raw: bool = True,
    enable_geocoding: bool = True,
    geocoding_max_rows: int | None = None,
    geocoding_only_missing_coordinates: bool = True,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    """
    Exécute le pipeline API global :
    1. extraction Overpass
    2. enrichissement géocodage optionnel
    3. sauvegarde finale consolidée
    """
    logger.info(
        "Starting global API pipeline for city=%s, country=%s",
        city,
        country,
    )

    _, overpass_df, _ = run_overpass_pipeline(
        city=city,
        country=country,
        save_raw=save_raw,
    )

    final_df = overpass_df

    if enable_geocoding:
        logger.info(
            "Geocoding enrichment enabled (max_rows=%s, only_missing_coordinates=%s).",
            geocoding_max_rows,
            geocoding_only_missing_coordinates,
        )
        final_df = run_geocoding_enrichment(
            final_df,
            max_rows=geocoding_max_rows,
            only_missing_coordinates=geocoding_only_missing_coordinates,
            save_raw=save_raw,
        )
    else:
        logger.info("Geocoding enrichment disabled.")

    output_paths = save_processed_dataframe(final_df)

    logger.info(
        "Global API pipeline completed successfully: %d final row(s).",
        len(final_df),
    )

    return final_df, output_paths