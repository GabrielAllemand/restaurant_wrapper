from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.extractors.inpi_rne_client import (
    InpiRneClient,
    extract_inpi_rne_fields,
    load_inpi_rne_config,
    normalize_siren,
)
from src.loaders.postgres_loader import create_postgres_engine
from src.utils.logger import get_logger


logger = get_logger(__name__)


INPI_RNE_COLUMNS: list[str] = [
    "inpi_rne_siren",
    "inpi_rne_type_personne",
    "inpi_rne_diffusion_commerciale",
    "inpi_rne_diffusion_insee",
    "inpi_rne_updated_at",
    "inpi_rne_date_creation",
    "inpi_rne_forme_juridique",
    "inpi_rne_company_name",
    "inpi_rne_main_siret",
    "inpi_rne_main_code_ape",
    "inpi_rne_main_status",
    "inpi_rne_main_postal_code",
    "inpi_rne_main_city",
    "inpi_rne_activity_start_date",
    "inpi_rne_nombre_representants_actifs",
    "inpi_rne_nombre_etablissements_ouverts",
    "inpi_rne_representative_name",
    "inpi_rne_representative_role",
    "inpi_rne_last_event_code",
    "inpi_rne_last_event_label",
    "inpi_rne_last_event_date_effet",
    "inpi_rne_payload",
]


def fetch_paris_candidates(
    *,
    only_missing: bool = True,
    limit: int | None = None,
) -> pd.DataFrame:
    engine = create_postgres_engine()

    where_clauses = [
        "city_business = 'Paris'",
        "siret IS NOT NULL",
        "TRIM(siret) <> ''",
    ]

    if only_missing:
        where_clauses.append("inpi_rne_siren IS NULL")

    limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""

    sql = f"""
    SELECT
        id,
        source,
        source_id,
        name,
        siret,
        city_business
    FROM establishments
    WHERE {' AND '.join(where_clauses)}
    ORDER BY id
    {limit_clause}
    """

    return pd.read_sql_query(text(sql), engine)


def _prepare_jobs(candidates: pd.DataFrame) -> pd.DataFrame:
    jobs = candidates.copy()
    jobs["siret_clean"] = jobs["siret"].astype(str).str.replace(r"\D", "", regex=True)
    jobs["siren"] = jobs["siret_clean"].str[:9]
    jobs = jobs[jobs["siren"].str.len() == 9].copy()
    return jobs


def _to_json_string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _prepare_sql_value(column: str, value: Any) -> Any:
    if column == "inpi_rne_payload":
        return _to_json_string_or_none(value)
    return value


def _update_row(connection, *, row_id: int, values: dict[str, Any]) -> None:
    params = {"row_id": row_id, "inpi_rne_enriched_at": datetime.now(timezone.utc)}

    set_clauses = []
    for column in INPI_RNE_COLUMNS:
        params[column] = _prepare_sql_value(column, values.get(column))

        if column == "inpi_rne_payload":
            set_clauses.append(f"{column} = CAST(:{column} AS JSONB)")
        else:
            set_clauses.append(f"{column} = :{column}")

    set_clauses.append("inpi_rne_enriched_at = :inpi_rne_enriched_at")

    sql = f"""
    UPDATE establishments
    SET
        {', '.join(set_clauses)}
    WHERE id = :row_id
    """

    connection.execute(text(sql), params)


def run_inpi_rne_enrichment_for_paris(
    *,
    limit: int | None = None,
    only_missing: bool = True,
) -> dict[str, int]:
    candidates = fetch_paris_candidates(
        only_missing=only_missing,
        limit=limit,
    )

    if candidates.empty:
        logger.info("INPI RNE enrichment skipped: no Paris candidate rows found.")
        return {
            "candidate_rows": 0,
            "distinct_sirens": 0,
            "successful_company_payloads": 0,
            "updated_rows": 0,
        }

    jobs = _prepare_jobs(candidates)

    if jobs.empty:
        logger.info("INPI RNE enrichment skipped: no valid Paris SIRET/SIREN rows found.")
        return {
            "candidate_rows": len(candidates),
            "distinct_sirens": 0,
            "successful_company_payloads": 0,
            "updated_rows": 0,
        }

    config = load_inpi_rne_config()
    client = InpiRneClient(config)
    client.login()

    distinct_sirens = jobs["siren"].drop_duplicates().tolist()
    logger.info(
        "Starting INPI RNE enrichment for Paris: %d candidate row(s), %d distinct siren(s).",
        len(jobs),
        len(distinct_sirens),
    )

    cache: dict[str, dict[str, Any] | None] = {}
    successful_company_payloads = 0

    for idx, siren in enumerate(distinct_sirens, start=1):
        payload = client.get_company_by_siren(siren)
        if payload is None:
            cache[siren] = None
            continue

        extracted = extract_inpi_rne_fields(payload)
        cache[siren] = extracted
        successful_company_payloads += 1

        if idx % 100 == 0:
            logger.info(
                "INPI RNE enrichment progress: %d/%d siren(s) fetched.",
                idx,
                len(distinct_sirens),
            )

    engine = create_postgres_engine()
    updated_rows = 0

    with engine.begin() as connection:
        for _, row in jobs.iterrows():
            siren = normalize_siren(row["siren"])
            if not siren:
                continue

            values = cache.get(siren)
            if not values:
                continue

            _update_row(
                connection,
                row_id=int(row["id"]),
                values=values,
            )
            updated_rows += 1

    logger.info(
        "INPI RNE Paris enrichment finished. Distinct sirens fetched=%d, rows updated=%d.",
        successful_company_payloads,
        updated_rows,
    )

    return {
        "candidate_rows": len(jobs),
        "distinct_sirens": len(distinct_sirens),
        "successful_company_payloads": successful_company_payloads,
        "updated_rows": updated_rows,
    }