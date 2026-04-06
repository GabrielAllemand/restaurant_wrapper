from __future__ import annotations

import time

import pandas as pd

from src.extractors.geoplateforme_client import (
    GeoplateformeConfig,
    enrich_row_with_geoplateforme,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_ban_enrichment(
    dataframe: pd.DataFrame,
    *,
    max_rows: int | None = None,
    only_missing_postal_code: bool = True,
    sleep_seconds: float = 0.05,
) -> pd.DataFrame:
    df = dataframe.copy()

    if "ban_address" not in df.columns:
        df["ban_address"] = None
    if "ban_city" not in df.columns:
        df["ban_city"] = None
    if "ban_postal_code" not in df.columns:
        df["ban_postal_code"] = None
    if "ban_latitude" not in df.columns:
        df["ban_latitude"] = None
    if "ban_longitude" not in df.columns:
        df["ban_longitude"] = None
    if "ban_score" not in df.columns:
        df["ban_score"] = None
    if "ban_source" not in df.columns:
        df["ban_source"] = None

    mask = pd.Series(True, index=df.index)

    if only_missing_postal_code and "postal_code" in df.columns:
        mask &= df["postal_code"].isna()

    candidate_indexes = df.index[mask].tolist()

    if max_rows is not None:
        candidate_indexes = candidate_indexes[:max_rows]

    if not candidate_indexes:
        logger.info("No rows selected for BAN/Géoplateforme enrichment.")
        return df

    logger.info(
        "Starting BAN/Géoplateforme enrichment for %d row(s).",
        len(candidate_indexes),
    )

    config = GeoplateformeConfig()
    success_count = 0

    for idx in candidate_indexes:
        row = df.loc[idx]

        result = enrich_row_with_geoplateforme(
            row_address=row.get("address"),
            row_city=row.get("city"),
            row_postal_code=row.get("postal_code"),
            row_latitude=row.get("latitude"),
            row_longitude=row.get("longitude"),
            config=config,
        )

        if result:
            success_count += 1
            for key, value in result.items():
                df.at[idx, key] = value

            if pd.isna(row.get("postal_code")) and result.get("ban_postal_code"):
                df.at[idx, "postal_code"] = result["ban_postal_code"]

            if pd.isna(row.get("address")) and result.get("ban_address"):
                df.at[idx, "address"] = result["ban_address"]

            if pd.isna(row.get("city")) and result.get("ban_city"):
                df.at[idx, "city"] = result["ban_city"]

        time.sleep(sleep_seconds)

    logger.info(
        "BAN/Géoplateforme enrichment finished. Successful enrichments: %d",
        success_count,
    )

    return df