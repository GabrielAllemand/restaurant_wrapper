from __future__ import annotations

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none"}


def _clean_postal_code(value: object) -> str | None:
    if _is_missing(value):
        return None

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    return text or None


def run_field_consolidation(dataframe: pd.DataFrame) -> pd.DataFrame:
    df = dataframe.copy()

    if "address" not in df.columns:
        df["address"] = None
    if "city" not in df.columns:
        df["city"] = None
    if "postal_code" not in df.columns:
        df["postal_code"] = None

    updated_address = 0
    updated_city = 0
    updated_postal_code = 0

    for idx, row in df.iterrows():
        if _is_missing(row.get("address")) and not _is_missing(row.get("ban_address")):
            df.at[idx, "address"] = row.get("ban_address")
            updated_address += 1

        if _is_missing(row.get("city")) and not _is_missing(row.get("ban_city")):
            df.at[idx, "city"] = row.get("ban_city")
            updated_city += 1

        if _is_missing(row.get("postal_code")) and not _is_missing(row.get("ban_postal_code")):
            df.at[idx, "postal_code"] = _clean_postal_code(row.get("ban_postal_code"))
            updated_postal_code += 1
        elif not _is_missing(row.get("postal_code")):
            df.at[idx, "postal_code"] = _clean_postal_code(row.get("postal_code"))

    logger.info(
        "Field consolidation finished. Updated address=%d, city=%d, postal_code=%d",
        updated_address,
        updated_city,
        updated_postal_code,
    )

    return df