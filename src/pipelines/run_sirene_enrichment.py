from __future__ import annotations

import pandas as pd

from src.extractors.sirene_client import enrich_row_with_sirene, load_sirene_config
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_sirene_enrichment(
    dataframe: pd.DataFrame,
    *,
    max_rows: int | None = None,
    only_missing_siret: bool = True,
) -> pd.DataFrame:
    if dataframe.empty:
        logger.info("Sirene enrichment skipped: empty dataframe.")
        return dataframe

    config = load_sirene_config()

    df = dataframe.copy()

    enrichment_columns = [
        "sirene_siret",
        "sirene_business_status",
        "sirene_address",
        "sirene_city",
        "sirene_postal_code",
        "sirene_name",
    ]
    for column in enrichment_columns:
        if column not in df.columns:
            df[column] = None

    if only_missing_siret and "siret" in df.columns:
        mask = df["siret"].isna() | (df["siret"].astype(str).str.strip() == "")
    else:
        mask = pd.Series(True, index=df.index)

    candidate_indexes = df.index[mask].tolist()

    if max_rows is not None:
        candidate_indexes = candidate_indexes[:max_rows]

    logger.info("Starting Sirene enrichment for %d row(s).", len(candidate_indexes))

    enriched_count = 0

    for idx in candidate_indexes:
        row = df.loc[idx]

        result = enrich_row_with_sirene(
            row_name=row.get("name"),
            row_city=row.get("city"),
            row_postal_code=row.get("postal_code"),
            row_address=row.get("address"),
            config=config,
        )

        if result is None:
            continue

        for key, value in result.items():
            df.at[idx, key] = value

        if (pd.isna(df.at[idx, "siret"]) or str(df.at[idx, "siret"]).strip() == "") and result.get("sirene_siret"):
            df.at[idx, "siret"] = result["sirene_siret"]

        if (
            "business_status" in df.columns
            and (pd.isna(df.at[idx, "business_status"]) or str(df.at[idx, "business_status"]).strip() == "")
            and result.get("sirene_business_status")
        ):
            df.at[idx, "business_status"] = result["sirene_business_status"]

        if (
            "postal_code" in df.columns
            and (pd.isna(df.at[idx, "postal_code"]) or str(df.at[idx, "postal_code"]).strip() == "")
            and result.get("sirene_postal_code")
        ):
            df.at[idx, "postal_code"] = result["sirene_postal_code"]

        if (
            "city" in df.columns
            and (pd.isna(df.at[idx, "city"]) or str(df.at[idx, "city"]).strip() == "")
            and result.get("sirene_city")
        ):
            df.at[idx, "city"] = result["sirene_city"]

        if (
            "address" in df.columns
            and (pd.isna(df.at[idx, "address"]) or str(df.at[idx, "address"]).strip() == "")
            and result.get("sirene_address")
        ):
            df.at[idx, "address"] = result["sirene_address"]

        enriched_count += 1

    logger.info("Sirene enrichment finished. Successful enrichments: %d", enriched_count)
    return df