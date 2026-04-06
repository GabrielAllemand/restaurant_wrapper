from __future__ import annotations

import pandas as pd

from src.extractors.sirene_client import enrich_row_with_sirene, load_sirene_config
from src.utils.logger import get_logger


logger = get_logger(__name__)


V2_ENRICHMENT_COLUMNS = [
    "sirene_v2_siret",
    "sirene_v2_business_status",
    "sirene_v2_address",
    "sirene_v2_city",
    "sirene_v2_postal_code",
    "sirene_v2_name",
    "sirene_v2_match_score",
]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null"}


def run_sirene_v2_enrichment(
    dataframe: pd.DataFrame,
    *,
    max_rows: int | None = None,
    only_missing_siret: bool = True,
) -> pd.DataFrame:
    """
    Enrichissement Sirene V2 :
    - ne touche pas aux colonnes sirene_* existantes
    - écrit dans sirene_v2_*
    - utilise ban_* en priorité pour city/postal_code/address
    - remplit siret final uniquement si vide et si un sirene_v2_siret est trouvé
    """
    if dataframe.empty:
        logger.info("Sirene V2 enrichment skipped: empty dataframe.")
        return dataframe

    config = load_sirene_config()

    df = dataframe.copy()

    for column in V2_ENRICHMENT_COLUMNS:
        if column not in df.columns:
            df[column] = None

    if only_missing_siret and "siret" in df.columns:
        mask = df["siret"].apply(_is_missing)
    else:
        mask = pd.Series(True, index=df.index)

    candidate_indexes = df.index[mask].tolist()

    if max_rows is not None:
        candidate_indexes = candidate_indexes[:max_rows]

    logger.info("Starting Sirene V2 enrichment for %d row(s).", len(candidate_indexes))

    enriched_count = 0

    for idx in candidate_indexes:
        row = df.loc[idx]

        row_name = row.get("name")
        row_city = row.get("ban_city") if not _is_missing(row.get("ban_city")) else row.get("city")
        row_postal_code = (
            row.get("ban_postal_code")
            if not _is_missing(row.get("ban_postal_code"))
            else row.get("postal_code")
        )
        row_address = row.get("ban_address") if not _is_missing(row.get("ban_address")) else row.get("address")

        result = enrich_row_with_sirene(
            row_name=row_name,
            row_city=row_city,
            row_postal_code=row_postal_code,
            row_address=row_address,
            config=config,
        )

        if result is None:
            continue

        df.at[idx, "sirene_v2_siret"] = result.get("sirene_siret")
        df.at[idx, "sirene_v2_business_status"] = result.get("sirene_business_status")
        df.at[idx, "sirene_v2_address"] = result.get("sirene_address")
        df.at[idx, "sirene_v2_city"] = result.get("sirene_city")
        df.at[idx, "sirene_v2_postal_code"] = result.get("sirene_postal_code")
        df.at[idx, "sirene_v2_name"] = result.get("sirene_name")
        df.at[idx, "sirene_v2_match_score"] = result.get("sirene_match_score")

        if _is_missing(df.at[idx, "siret"]) and not _is_missing(result.get("sirene_siret")):
            df.at[idx, "siret"] = result["sirene_siret"]

        if "business_status" in df.columns:
            if _is_missing(df.at[idx, "business_status"]) and not _is_missing(result.get("sirene_business_status")):
                df.at[idx, "business_status"] = result["sirene_business_status"]

        enriched_count += 1

    logger.info("Sirene V2 enrichment finished. Successful enrichments: %d", enriched_count)
    return df