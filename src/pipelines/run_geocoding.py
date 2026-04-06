from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.extractors.geocode_client import GeocodingQuery, geocode_address
from src.loaders.save_raw import save_raw_payload
from src.transformers.normalize_address import build_full_address
from src.utils.dataframe import coalesce, ensure_columns, is_missing, safe_string
from src.utils.files import sanitize_filename
from src.utils.logger import get_logger


logger = get_logger(__name__)


GEOCODING_REQUIRED_COLUMNS = [
    "address",
    "postal_code",
    "city",
    "country",
    "latitude",
    "longitude",
]


def run_geocoding_enrichment(
    dataframe: pd.DataFrame,
    *,
    max_rows: int | None = None,
    only_missing_coordinates: bool = True,
    save_raw: bool = True,
) -> pd.DataFrame:
    """
    Enrichit un DataFrame standard avec le géocodage.
    Par défaut, ne tente le géocodage que sur les lignes sans coordonnées.
    """
    if dataframe.empty:
        logger.warning("Geocoding skipped because dataframe is empty.")
        return dataframe.copy()

    enriched = ensure_columns(dataframe, GEOCODING_REQUIRED_COLUMNS)

    target_indexes = _select_rows_for_geocoding(
        enriched,
        max_rows=max_rows,
        only_missing_coordinates=only_missing_coordinates,
    )

    if not target_indexes:
        logger.info("No rows selected for geocoding.")
        return enriched

    logger.info("Starting geocoding enrichment for %d row(s).", len(target_indexes))

    success_count = 0

    for index in target_indexes:
        row = enriched.loc[index]

        query = GeocodingQuery(
            address=safe_string(row.get("address")),
            postal_code=safe_string(row.get("postal_code")),
            city=safe_string(row.get("city")),
            country=safe_string(row.get("country")),
        )

        if not build_full_address(
            address=query.address,
            postal_code=query.postal_code,
            city=query.city,
            country=query.country,
        ):
            continue

        result = geocode_address(query)
        if result is None:
            continue

        if save_raw and result.get("raw_payload") is not None:
            suffix = sanitize_filename(f"{safe_string(row.get('city')) or 'unknown_city'}_{index}")
            save_raw_payload(
                result["raw_payload"],
                source_name="geocoding",
                suffix=suffix,
            )

        enriched.at[index, "latitude"] = coalesce(row.get("latitude"), result.get("latitude"))
        enriched.at[index, "longitude"] = coalesce(row.get("longitude"), result.get("longitude"))
        enriched.at[index, "address"] = coalesce(row.get("address"), result.get("geocoded_address"))
        enriched.at[index, "postal_code"] = coalesce(row.get("postal_code"), result.get("postal_code"))
        enriched.at[index, "city"] = coalesce(row.get("city"), result.get("city"))
        enriched.at[index, "country"] = coalesce(row.get("country"), result.get("country"))

        success_count += 1

    logger.info(
        "Geocoding enrichment finished. Successful enrichments: %d / %d",
        success_count,
        len(target_indexes),
    )

    return enriched


def _select_rows_for_geocoding(
    dataframe: pd.DataFrame,
    *,
    max_rows: int | None,
    only_missing_coordinates: bool,
) -> list[Any]:
    """
    Sélectionne les lignes candidates au géocodage.
    """
    indexes: list[Any] = []

    for index, row in dataframe.iterrows():
        address = safe_string(row.get("address"))
        postal_code = safe_string(row.get("postal_code"))
        city = safe_string(row.get("city"))
        country = safe_string(row.get("country"))

        has_query_data = any([address, postal_code, city, country])
        if not has_query_data:
            continue

        latitude_missing = is_missing(row.get("latitude"))
        longitude_missing = is_missing(row.get("longitude"))

        if only_missing_coordinates and not (latitude_missing or longitude_missing):
            continue

        indexes.append(index)

        if max_rows is not None and len(indexes) >= max_rows:
            break

    return indexes