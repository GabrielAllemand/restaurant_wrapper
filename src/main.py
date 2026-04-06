from __future__ import annotations

import argparse

from src.config.settings import settings
from src.loaders.postgres_loader import load_schema_sql, upsert_establishments
from src.loaders.save_processed import save_processed_dataframe
from src.pipelines.run_api_pipeline import run_api_pipeline
from src.pipelines.run_ban_enrichment import run_ban_enrichment
from src.pipelines.run_field_consolidation import run_field_consolidation
from src.pipelines.run_france_pipeline import run_france_pipeline
from src.pipelines.run_pagesjaunes_enrichment import run_pagesjaunes_enrichment
from src.pipelines.run_sirene_enrichment import run_sirene_enrichment
from src.utils.logger import get_logger


logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """
    Construit l'interface CLI du projet.
    """
    parser = argparse.ArgumentParser(
        description="Restaurant & Magasin Wrapper - API ingestion pipeline"
    )

    parser.add_argument(
        "--city",
        type=str,
        default=settings.pipeline.target_city,
        help=f"Target city (default: {settings.pipeline.target_city})",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=settings.pipeline.target_country,
        help=f"Target country (default: {settings.pipeline.target_country})",
    )
    parser.add_argument(
        "--no-save-raw",
        action="store_true",
        help="Do not save raw API payloads to disk.",
    )
    parser.add_argument(
        "--disable-geocoding",
        action="store_true",
        help="Disable geocoding enrichment step.",
    )
    parser.add_argument(
        "--geocoding-max-rows",
        type=int,
        default=None,
        help="Maximum number of rows to geocode.",
    )
    parser.add_argument(
        "--geocode-all",
        action="store_true",
        help="Geocode rows even if coordinates already exist.",
    )
    parser.add_argument(
        "--load-postgres",
        action="store_true",
        help="Load the final dataframe into PostgreSQL.",
    )
    parser.add_argument(
        "--apply-schema",
        action="store_true",
        help="Apply schema.sql before loading data into PostgreSQL.",
    )
    parser.add_argument(
        "--france",
        action="store_true",
        help="Run extraction on French departments.",
    )
    parser.add_argument(
        "--department-limit",
        type=int,
        default=None,
        help="Limit the number of departments processed (useful for testing).",
    )
    parser.add_argument(
        "--department-codes",
        type=str,
        default=None,
        help="Comma-separated department codes to process, e.g. 05,09,17,2A,973",
    )
    parser.add_argument(
        "--enrich-sirene",
        action="store_true",
        help="Enable Sirene enrichment step.",
    )
    parser.add_argument(
        "--sirene-max-rows",
        type=int,
        default=None,
        help="Maximum number of rows to enrich with Sirene.",
    )
    parser.add_argument(
        "--sirene-all",
        action="store_true",
        help="Run Sirene enrichment on all rows, not only rows missing siret.",
    )
    parser.add_argument(
        "--enrich-ban",
        action="store_true",
        help="Enable BAN / Géoplateforme enrichment.",
    )
    parser.add_argument(
        "--ban-max-rows",
        type=int,
        default=None,
        help="Maximum number of rows to enrich with BAN / Géoplateforme.",
    )
    parser.add_argument(
        "--ban-all",
        action="store_true",
        help="Run BAN enrichment on all rows, not only rows missing postal_code.",
    )
    parser.add_argument(
        "--enrich-pagesjaunes",
        action="store_true",
        help="Enable PagesJaunes enrichment.",
    )
    parser.add_argument(
        "--pagesjaunes-max-rows",
        type=int,
        default=None,
        help="Maximum number of rows to enrich with PagesJaunes.",
    )
    parser.add_argument(
        "--pagesjaunes-all",
        action="store_true",
        help="Run PagesJaunes enrichment on all rows, not only rows missing phone.",
    )

    return parser


def _parse_department_codes(raw_value: str | None) -> list[str] | None:
    if raw_value is None:
        return None

    codes = [part.strip().upper() for part in raw_value.split(",") if part.strip()]
    return codes or None


def main() -> int:
    """
    Point d'entrée principal du pipeline API.
    """
    parser = build_parser()
    args = parser.parse_args()

    logger.info("Launching API pipeline...")
    logger.info("Project root: %s", settings.paths.project_root)

    department_codes = _parse_department_codes(args.department_codes)

    if args.france:
        logger.info("France mode enabled.")
        if department_codes:
            logger.info(
                "Targeted department selection enabled for codes=%s",
                ",".join(department_codes),
            )

        dataframe, output_paths = run_france_pipeline(
            country=args.country,
            save_raw=not args.no_save_raw,
            department_limit=args.department_limit,
            department_codes=department_codes,
        )
    else:
        dataframe, output_paths = run_api_pipeline(
            city=args.city,
            country=args.country,
            save_raw=not args.no_save_raw,
            enable_geocoding=not args.disable_geocoding,
            geocoding_max_rows=args.geocoding_max_rows,
            geocoding_only_missing_coordinates=not args.geocode_all,
        )

    enrichments_applied = False

    if args.enrich_sirene:
        logger.info(
            "Sirene enrichment enabled (max_rows=%s, only_missing_siret=%s).",
            args.sirene_max_rows,
            not args.sirene_all,
        )
        dataframe = run_sirene_enrichment(
            dataframe,
            max_rows=args.sirene_max_rows,
            only_missing_siret=not args.sirene_all,
        )
        enrichments_applied = True

    if args.enrich_ban:
        logger.info(
            "BAN / Géoplateforme enrichment enabled (max_rows=%s, only_missing_postal_code=%s).",
            args.ban_max_rows,
            not args.ban_all,
        )
        dataframe = run_ban_enrichment(
            dataframe,
            max_rows=args.ban_max_rows,
            only_missing_postal_code=not args.ban_all,
        )
        enrichments_applied = True

    if args.enrich_pagesjaunes:
        logger.info(
            "PagesJaunes enrichment enabled (max_rows=%s, only_missing_phone=%s).",
            args.pagesjaunes_max_rows,
            not args.pagesjaunes_all,
        )
        dataframe = run_pagesjaunes_enrichment(
            dataframe,
            max_rows=args.pagesjaunes_max_rows,
            only_missing_phone=not args.pagesjaunes_all,
        )
        enrichments_applied = True

    if args.enrich_sirene or args.enrich_ban or args.enrich_pagesjaunes:
        logger.info("Running final field consolidation step.")
        dataframe = run_field_consolidation(dataframe)

    if enrichments_applied:
        logger.info("Re-saving processed files after enrichment steps.")
        output_paths = save_processed_dataframe(
            dataframe=dataframe,
            csv_path=output_paths["csv"],
            parquet_path=output_paths["parquet"],
        )

    if args.load_postgres:
        if args.apply_schema:
            load_schema_sql()

        loaded_rows = upsert_establishments(dataframe)
        logger.info("Loaded %d row(s) into PostgreSQL.", loaded_rows)

    logger.info("Pipeline finished successfully.")
    logger.info("Final row count: %d", len(dataframe))

    for format_name, path in output_paths.items():
        logger.info("Output written [%s]: %s", format_name, path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())