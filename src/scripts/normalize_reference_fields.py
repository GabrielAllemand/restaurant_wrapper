from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.config.settings import settings
from src.loaders.postgres_loader import create_postgres_engine
from src.utils.logger import get_logger


logger = get_logger(__name__)


CITY_COLUMNS_SQL = """
ALTER TABLE establishments
ADD COLUMN IF NOT EXISTS city_normalized TEXT,
ADD COLUMN IF NOT EXISTS city_canonical TEXT,
ADD COLUMN IF NOT EXISTS city_business TEXT;
"""

CATEGORY_COLUMNS_SQL = """
ALTER TABLE establishments
ADD COLUMN IF NOT EXISTS category_normalized TEXT,
ADD COLUMN IF NOT EXISTS category_canonical TEXT,
ADD COLUMN IF NOT EXISTS subcategory_normalized TEXT,
ADD COLUMN IF NOT EXISTS subcategory_canonical TEXT;
"""


def _clean_text(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.lower() in {"nan", "none", "null"}:
        return None

    text = text.replace("’", "'").replace("`", "'").replace("´", "'")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _normalize_key(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    text = _strip_accents(cleaned.lower())

    text = re.sub(r"\bst[.\s-]+", "saint ", text)
    text = re.sub(r"\bste[.\s-]+", "sainte ", text)

    text = text.replace("'", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def _titlecase_token(token: str) -> str:
    if not token:
        return token
    return token[0].upper() + token[1:].lower()


def _display_from_normalized_key(normalized_key: str | None) -> str | None:
    if normalized_key is None:
        return None

    tokens = normalized_key.split()
    if not tokens:
        return None

    lower_connectors = {
        "de", "du", "des", "la", "le", "les", "au", "aux", "sur", "sous", "en", "et"
    }

    display_tokens: list[str] = []
    for idx, token in enumerate(tokens):
        if idx > 0 and token in lower_connectors:
            display_tokens.append(token)
        else:
            display_tokens.append(_titlecase_token(token))

    return "-".join(display_tokens)


def _business_city_from_canonical(city_canonical: str | None) -> str | None:
    if city_canonical is None:
        return None

    key = _normalize_key(city_canonical)
    if key is None:
        return city_canonical

    if key.startswith("paris ") and "arrondissement" in key:
        return "Paris"

    if key.startswith("marseille ") and "arrondissement" in key:
        return "Marseille"

    if key.startswith("lyon ") and "arrondissement" in key:
        return "Lyon"

    return city_canonical


def _normalize_category_display(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    text = cleaned.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def build_city_mapping(distinct_city_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = distinct_city_df.copy()
    df["raw_value"] = df["raw_value"].apply(_clean_text)
    df = df[df["raw_value"].notna()].copy()

    df["normalized_key"] = df["raw_value"].apply(_normalize_key)
    df = df[df["normalized_key"].notna()].copy()

    # Choisit la forme brute la plus fréquente pour chaque groupe
    df_sorted = df.sort_values(["normalized_key", "row_count", "raw_value"], ascending=[True, False, True])
    representative = df_sorted.groupby("normalized_key", as_index=False).first()

    representative["canonical_value"] = representative["normalized_key"].apply(_display_from_normalized_key)
    representative["business_value"] = representative["canonical_value"].apply(_business_city_from_canonical)

    mapping = df[["raw_value", "normalized_key"]].merge(
        representative[["normalized_key", "canonical_value", "business_value"]],
        on="normalized_key",
        how="left",
    )

    audit = (
        df.groupby("normalized_key", as_index=False)
        .agg(
            variants=("raw_value", "nunique"),
            total_rows=("row_count", "sum"),
        )
        .merge(
            representative[["normalized_key", "canonical_value", "business_value"]],
            on="normalized_key",
            how="left",
        )
        .sort_values(["variants", "total_rows"], ascending=[False, False])
    )

    return mapping, audit


def build_generic_mapping(
    distinct_df: pd.DataFrame,
    *,
    canonical_fn,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = distinct_df.copy()
    df["raw_value"] = df["raw_value"].apply(_clean_text)
    df = df[df["raw_value"].notna()].copy()

    df["normalized_key"] = df["raw_value"].apply(_normalize_key)
    df = df[df["normalized_key"].notna()].copy()

    df_sorted = df.sort_values(["normalized_key", "row_count", "raw_value"], ascending=[True, False, True])
    representative = df_sorted.groupby("normalized_key", as_index=False).first()
    representative["canonical_value"] = representative["raw_value"].apply(canonical_fn)

    mapping = df[["raw_value", "normalized_key"]].merge(
        representative[["normalized_key", "canonical_value"]],
        on="normalized_key",
        how="left",
    )

    audit = (
        df.groupby("normalized_key", as_index=False)
        .agg(
            variants=("raw_value", "nunique"),
            total_rows=("row_count", "sum"),
        )
        .merge(
            representative[["normalized_key", "canonical_value"]],
            on="normalized_key",
            how="left",
        )
        .sort_values(["variants", "total_rows"], ascending=[False, False])
    )

    return mapping, audit


def fetch_distinct_values(field_name: str) -> pd.DataFrame:
    engine = create_postgres_engine()
    sql = text(f"""
        SELECT
            {field_name} AS raw_value,
            COUNT(*) AS row_count
        FROM establishments
        WHERE {field_name} IS NOT NULL
          AND TRIM({field_name}) <> ''
        GROUP BY {field_name}
        ORDER BY COUNT(*) DESC
    """)

    return pd.read_sql_query(sql, engine)


def write_audit_files(
    output_dir: Path,
    *,
    city_mapping: pd.DataFrame,
    city_audit: pd.DataFrame,
    category_mapping: pd.DataFrame,
    category_audit: pd.DataFrame,
    subcategory_mapping: pd.DataFrame,
    subcategory_audit: pd.DataFrame,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    city_mapping.to_csv(output_dir / "city_mapping.csv", index=False)
    city_audit.to_csv(output_dir / "city_audit.csv", index=False)

    category_mapping.to_csv(output_dir / "category_mapping.csv", index=False)
    category_audit.to_csv(output_dir / "category_audit.csv", index=False)

    subcategory_mapping.to_csv(output_dir / "subcategory_mapping.csv", index=False)
    subcategory_audit.to_csv(output_dir / "subcategory_audit.csv", index=False)

    logger.info("Audit files written to %s", output_dir)


def apply_mappings_to_postgres(
    *,
    city_mapping: pd.DataFrame,
    category_mapping: pd.DataFrame,
    subcategory_mapping: pd.DataFrame,
) -> None:
    engine = create_postgres_engine()

    with engine.begin() as connection:
        connection.execute(text(CITY_COLUMNS_SQL))
        connection.execute(text(CATEGORY_COLUMNS_SQL))

        connection.execute(text("""
            UPDATE establishments
            SET
                city_normalized = NULL,
                city_canonical = NULL,
                city_business = NULL,
                category_normalized = NULL,
                category_canonical = NULL,
                subcategory_normalized = NULL,
                subcategory_canonical = NULL
        """))

    city_temp = "tmp_city_mapping"
    category_temp = "tmp_category_mapping"
    subcategory_temp = "tmp_subcategory_mapping"

    city_mapping.to_sql(city_temp, engine, if_exists="replace", index=False, method="multi", chunksize=1000)
    category_mapping.to_sql(category_temp, engine, if_exists="replace", index=False, method="multi", chunksize=1000)
    subcategory_mapping.to_sql(subcategory_temp, engine, if_exists="replace", index=False, method="multi", chunksize=1000)

    with engine.begin() as connection:
        connection.execute(text(f"""
            UPDATE establishments e
            SET
                city_normalized = m.normalized_key,
                city_canonical = m.canonical_value,
                city_business = m.business_value
            FROM {city_temp} m
            WHERE e.city = m.raw_value
        """))

        connection.execute(text(f"""
            UPDATE establishments e
            SET
                category_normalized = m.normalized_key,
                category_canonical = m.canonical_value
            FROM {category_temp} m
            WHERE e.category = m.raw_value
        """))

        connection.execute(text(f"""
            UPDATE establishments e
            SET
                subcategory_normalized = m.normalized_key,
                subcategory_canonical = m.canonical_value
            FROM {subcategory_temp} m
            WHERE e.subcategory = m.raw_value
        """))

        connection.execute(text(f"DROP TABLE IF EXISTS {city_temp}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {category_temp}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {subcategory_temp}"))

    logger.info("Normalized reference columns successfully written to PostgreSQL.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize city/category/subcategory reference fields in PostgreSQL."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply normalized mappings to PostgreSQL. Without this flag, only audit files are generated.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(settings.paths.project_root / "data" / "processed" / "reference_normalization"),
        help="Directory where audit CSV files will be written.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    logger.info("Fetching distinct city values...")
    distinct_city_df = fetch_distinct_values("city")

    logger.info("Fetching distinct category values...")
    distinct_category_df = fetch_distinct_values("category")

    logger.info("Fetching distinct subcategory values...")
    distinct_subcategory_df = fetch_distinct_values("subcategory")

    city_mapping, city_audit = build_city_mapping(distinct_city_df)
    category_mapping, category_audit = build_generic_mapping(
        distinct_category_df,
        canonical_fn=_normalize_category_display,
    )
    subcategory_mapping, subcategory_audit = build_generic_mapping(
        distinct_subcategory_df,
        canonical_fn=_normalize_category_display,
    )

    write_audit_files(
        output_dir,
        city_mapping=city_mapping,
        city_audit=city_audit,
        category_mapping=category_mapping,
        category_audit=category_audit,
        subcategory_mapping=subcategory_mapping,
        subcategory_audit=subcategory_audit,
    )

    logger.info("Top city normalization groups with the most variants:")
    logger.info("\n%s", city_audit.head(20).to_string(index=False))

    if args.apply:
        logger.info("Applying normalized mappings to PostgreSQL...")
        apply_mappings_to_postgres(
            city_mapping=city_mapping,
            category_mapping=category_mapping,
            subcategory_mapping=subcategory_mapping,
        )
    else:
        logger.info("Dry run only. No database changes were applied. Use --apply to write to PostgreSQL.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())