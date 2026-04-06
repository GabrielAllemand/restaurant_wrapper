from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.settings import settings
from src.utils.files import write_dataframe_csv, write_dataframe_parquet
from src.utils.logger import get_logger


logger = get_logger(__name__)


def save_processed_dataframe(
    dataframe: pd.DataFrame,
    *,
    csv_path: Path | None = None,
    parquet_path: Path | None = None,
) -> dict[str, Path]:
    """
    Sauvegarde le DataFrame final dans les formats activés par la configuration.
    """
    output_paths: dict[str, Path] = {}

    target_csv_path = csv_path or settings.paths.processed_csv_path
    target_parquet_path = parquet_path or settings.paths.processed_parquet_path

    if settings.pipeline.write_csv:
        logger.info("Saving processed dataframe to CSV: %s", target_csv_path)
        output_paths["csv"] = write_dataframe_csv(dataframe, target_csv_path, index=False)

    if settings.pipeline.write_parquet:
        logger.info("Saving processed dataframe to Parquet: %s", target_parquet_path)
        output_paths["parquet"] = write_dataframe_parquet(
            dataframe,
            target_parquet_path,
            index=False,
        )

    if not output_paths:
        logger.warning("No processed output was written because all output formats are disabled.")

    return output_paths