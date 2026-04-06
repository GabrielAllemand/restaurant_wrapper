from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.extractors.overpass_client import fetch_and_parse_overpass
from src.loaders.save_processed import save_processed_dataframe
from src.loaders.save_raw import save_raw_payload
from src.transformers.standard_schema import (
    build_empty_standard_dataframe,
    standard_records_to_dataframe,
)
from src.utils.files import sanitize_filename
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_overpass_pipeline(
    *,
    city: str,
    country: str,
    save_raw: bool = True,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Path]]:
    """
    Exécute le pipeline Overpass de bout en bout :
    1. extraction du payload brut
    2. sauvegarde optionnelle du brut
    3. parsing vers records standardisés
    4. conversion en DataFrame
    5. sauvegarde des outputs finaux
    """
    logger.info(
        "Starting Overpass pipeline for city=%s, country=%s",
        city,
        country,
    )

    raw_payload, records = fetch_and_parse_overpass(city=city, country=country)

    if save_raw:
        suffix = sanitize_filename(city)
        save_raw_payload(
            raw_payload,
            source_name="overpass",
            suffix=suffix,
        )

    dataframe = _build_overpass_dataframe(records)
    output_paths = save_processed_dataframe(dataframe)

    logger.info(
        "Overpass pipeline completed successfully: %d row(s) written.",
        len(dataframe),
    )

    return raw_payload, dataframe, output_paths


def _build_overpass_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Construit un DataFrame standardisé à partir des records Overpass.
    """
    if not records:
        logger.warning("No records returned by Overpass parsing.")
        return build_empty_standard_dataframe()

    dataframe = standard_records_to_dataframe(records)
    dataframe = _postprocess_overpass_dataframe(dataframe)

    logger.info(
        "Built Overpass dataframe with %d row(s) and %d column(s).",
        len(dataframe),
        len(dataframe.columns),
    )
    return dataframe


def _postprocess_overpass_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Post-traitements légers sur le DataFrame standard :
    - suppression des doublons exacts évidents
    - tri stable pour lecture/debug
    """
    if dataframe.empty:
        return dataframe

    dedup_subset = ["source", "source_id", "name", "address", "city"]
    available_subset = [column for column in dedup_subset if column in dataframe.columns]

    if available_subset:
        before = len(dataframe)
        dataframe = dataframe.drop_duplicates(subset=available_subset, keep="first").copy()
        after = len(dataframe)

        if after < before:
            logger.info(
                "Dropped %d duplicate row(s) from Overpass dataframe.",
                before - after,
            )

    sort_columns = [column for column in ["city", "category", "name"] if column in dataframe.columns]
    if sort_columns:
        dataframe = dataframe.sort_values(by=sort_columns, kind="stable").reset_index(drop=True)

    return dataframe