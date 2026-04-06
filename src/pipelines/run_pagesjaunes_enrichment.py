from __future__ import annotations

import pandas as pd

from src.extractors.pagesjaunes_client import enrich_row_with_pagesjaunes
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _ensure_pagesjaunes_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    df = dataframe.copy()

    required_columns = [
        "pj_url",
        "pj_phone",
        "pj_website",
        "pj_opening_hours",
        "pj_match_score",
    ]

    for column in required_columns:
        if column not in df.columns:
            df[column] = pd.NA

    return df


def run_pagesjaunes_enrichment(
    dataframe: pd.DataFrame,
    *,
    max_rows: int | None = None,
    only_missing_phone: bool = True,
) -> pd.DataFrame:
    df = _ensure_pagesjaunes_columns(dataframe)

    mask = df["name"].notna()

    if only_missing_phone:
        mask &= df["phone"].isna()

    indices = df.index[mask].tolist()

    if max_rows is not None:
        indices = indices[:max_rows]

    logger.info("Starting PagesJaunes enrichment for %d row(s).", len(indices))

    success_count = 0

    for index in indices:
        row = df.loc[index]

        result = enrich_row_with_pagesjaunes(
            row_name=row.get("name"),
            row_city=row.get("city"),
            row_postal_code=row.get("postal_code"),
            row_address=row.get("address"),
        )

        if not result:
            continue

        for key, value in result.items():
            df.at[index, key] = value

        success_count += 1

    logger.info(
        "PagesJaunes enrichment finished. Successful enrichments: %d",
        success_count,
    )

    return df