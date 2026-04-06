from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config.france_departments import FRANCE_DEPARTMENTS, FrenchDepartment
from src.extractors.overpass_client import fetch_and_parse_overpass_department
from src.loaders.save_processed import save_processed_dataframe
from src.loaders.save_raw import save_raw_payload
from src.transformers.standard_schema import build_empty_standard_dataframe, standard_records_to_dataframe
from src.utils.files import sanitize_filename
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run_france_pipeline(
    *,
    country: str = "France",
    save_raw: bool = True,
    department_limit: int | None = None,
    department_codes: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    """
    Exécute l'extraction nationale par département, concatène et sauvegarde le résultat final.
    Peut être limité à une liste ciblée de codes département.
    """
    departments = _select_departments(
        department_limit=department_limit,
        department_codes=department_codes,
    )

    logger.info("Starting France pipeline for %d department(s).", len(departments))

    frames: list[pd.DataFrame] = []

    for index, department in enumerate(departments, start=1):
        logger.info(
            "Processing department %d/%d: %s (%s)",
            index,
            len(departments),
            department.name,
            department.code,
        )

        try:
            raw_payload, records = fetch_and_parse_overpass_department(
                department_name=department.name,
                country=country,
            )

            if save_raw:
                save_raw_payload(
                    raw_payload,
                    source_name="overpass",
                    suffix=f"{department.code}_{sanitize_filename(department.name)}",
                )

            dataframe = standard_records_to_dataframe(records) if records else build_empty_standard_dataframe()

            if not dataframe.empty:
                dataframe["department_code"] = department.code
                dataframe["department_name"] = department.name
                frames.append(dataframe)

            logger.info(
                "Department %s completed with %d row(s).",
                department.code,
                len(dataframe),
            )

        except Exception as exc:
            logger.exception(
                "Department %s (%s) failed: %s",
                department.code,
                department.name,
                exc,
            )

    if not frames:
        final_df = build_empty_standard_dataframe()
        final_df["department_code"] = None
        final_df["department_name"] = None
    else:
        final_df = pd.concat(frames, ignore_index=True)
        final_df = _postprocess_france_dataframe(final_df)

    output_paths = _build_output_paths(department_codes=department_codes)

    output_paths = save_processed_dataframe(
        final_df,
        csv_path=output_paths["csv"],
        parquet_path=output_paths["parquet"],
    )

    logger.info(
        "France pipeline completed successfully with %d final row(s).",
        len(final_df),
    )

    return final_df, output_paths


def _select_departments(
    *,
    department_limit: int | None,
    department_codes: list[str] | None,
) -> list[FrenchDepartment]:
    departments = FRANCE_DEPARTMENTS

    if department_codes:
        normalized_codes = {code.strip().upper() for code in department_codes if code.strip()}
        departments = [department for department in departments if department.code.upper() in normalized_codes]

    if department_limit is not None:
        departments = departments[:department_limit]

    return departments


def _build_output_paths(
    *,
    department_codes: list[str] | None,
) -> dict[str, Path]:
    if department_codes:
        normalized = "_".join(code.strip().lower() for code in department_codes if code.strip())
        suffix = sanitize_filename(normalized)[:120] or "subset"

        return {
            "csv": Path(f"data/processed/api_establishments_france_subset_{suffix}.csv"),
            "parquet": Path(f"data/processed/api_establishments_france_subset_{suffix}.parquet"),
        }

    return {
        "csv": Path("data/processed/api_establishments_france.csv"),
        "parquet": Path("data/processed/api_establishments_france.parquet"),
    }


def _postprocess_france_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Post-traitements nationaux :
    - suppression des doublons exacts
    - tri stable
    """
    if dataframe.empty:
        return dataframe

    dedup_subset = [
        "source",
        "source_id",
        "name",
        "address",
        "postal_code",
        "city",
        "latitude",
        "longitude",
    ]
    available_subset = [column for column in dedup_subset if column in dataframe.columns]

    if available_subset:
        before = len(dataframe)
        dataframe = dataframe.drop_duplicates(subset=available_subset, keep="first").copy()
        after = len(dataframe)

        if after < before:
            logger.info("Dropped %d duplicate row(s) in France dataframe.", before - after)

    sort_columns = [
        column for column in ["department_code", "city", "category", "name"] if column in dataframe.columns
    ]
    if sort_columns:
        dataframe = dataframe.sort_values(by=sort_columns, kind="stable").reset_index(drop=True)

    return dataframe