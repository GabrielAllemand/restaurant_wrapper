from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.loaders.postgres_loader import create_postgres_engine
from src.utils.logger import get_logger


logger = get_logger(__name__)


ADD_COLUMN_SQL = """
ALTER TABLE establishments
ADD COLUMN IF NOT EXISTS paris_arrondissement TEXT;
"""


ARRONDISSEMENT_BY_POSTAL_CODE: dict[str, str] = {
    "75001": "1er arrondissement",
    "75002": "2e arrondissement",
    "75003": "3e arrondissement",
    "75004": "4e arrondissement",
    "75005": "5e arrondissement",
    "75006": "6e arrondissement",
    "75007": "7e arrondissement",
    "75008": "8e arrondissement",
    "75009": "9e arrondissement",
    "75010": "10e arrondissement",
    "75011": "11e arrondissement",
    "75012": "12e arrondissement",
    "75013": "13e arrondissement",
    "75014": "14e arrondissement",
    "75015": "15e arrondissement",
    "75016": "16e arrondissement",
    "75017": "17e arrondissement",
    "75018": "18e arrondissement",
    "75019": "19e arrondissement",
    "75020": "20e arrondissement",
}


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    text = text.replace("’", "'").replace("`", "'").replace("´", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_for_matching(value: str | None) -> str:
    if not value:
        return ""
    text = _strip_accents(value.lower())
    text = text.replace("ème", "eme")
    text = text.replace("è", "e")
    text = text.replace("é", "e")
    text = text.replace("er", "er")
    text = text.replace("-", " ")
    text = text.replace("'", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def arrondissement_from_postal_code(postal_code: str | None) -> str | None:
    clean_postal = _clean_text(postal_code)
    if clean_postal is None:
        return None
    clean_postal = clean_postal[:5]
    return ARRONDISSEMENT_BY_POSTAL_CODE.get(clean_postal)


def arrondissement_from_address(address: str | None) -> str | None:
    clean_address = _clean_text(address)
    if clean_address is None:
        return None

    text = normalize_for_matching(clean_address)

    patterns = [
        (r"\bparis\s+1er\b", "1er arrondissement"),
        (r"\bparis\s+1\b", "1er arrondissement"),
        (r"\bparis\s+2e\b", "2e arrondissement"),
        (r"\bparis\s+2eme\b", "2e arrondissement"),
        (r"\bparis\s+2\b", "2e arrondissement"),
        (r"\bparis\s+3e\b", "3e arrondissement"),
        (r"\bparis\s+3eme\b", "3e arrondissement"),
        (r"\bparis\s+3\b", "3e arrondissement"),
        (r"\bparis\s+4e\b", "4e arrondissement"),
        (r"\bparis\s+4eme\b", "4e arrondissement"),
        (r"\bparis\s+4\b", "4e arrondissement"),
        (r"\bparis\s+5e\b", "5e arrondissement"),
        (r"\bparis\s+5eme\b", "5e arrondissement"),
        (r"\bparis\s+5\b", "5e arrondissement"),
        (r"\bparis\s+6e\b", "6e arrondissement"),
        (r"\bparis\s+6eme\b", "6e arrondissement"),
        (r"\bparis\s+6\b", "6e arrondissement"),
        (r"\bparis\s+7e\b", "7e arrondissement"),
        (r"\bparis\s+7eme\b", "7e arrondissement"),
        (r"\bparis\s+7\b", "7e arrondissement"),
        (r"\bparis\s+8e\b", "8e arrondissement"),
        (r"\bparis\s+8eme\b", "8e arrondissement"),
        (r"\bparis\s+8\b", "8e arrondissement"),
        (r"\bparis\s+9e\b", "9e arrondissement"),
        (r"\bparis\s+9eme\b", "9e arrondissement"),
        (r"\bparis\s+9\b", "9e arrondissement"),
        (r"\bparis\s+10e\b", "10e arrondissement"),
        (r"\bparis\s+10eme\b", "10e arrondissement"),
        (r"\bparis\s+10\b", "10e arrondissement"),
        (r"\bparis\s+11e\b", "11e arrondissement"),
        (r"\bparis\s+11eme\b", "11e arrondissement"),
        (r"\bparis\s+11\b", "11e arrondissement"),
        (r"\bparis\s+12e\b", "12e arrondissement"),
        (r"\bparis\s+12eme\b", "12e arrondissement"),
        (r"\bparis\s+12\b", "12e arrondissement"),
        (r"\bparis\s+13e\b", "13e arrondissement"),
        (r"\bparis\s+13eme\b", "13e arrondissement"),
        (r"\bparis\s+13\b", "13e arrondissement"),
        (r"\bparis\s+14e\b", "14e arrondissement"),
        (r"\bparis\s+14eme\b", "14e arrondissement"),
        (r"\bparis\s+14\b", "14e arrondissement"),
        (r"\bparis\s+15e\b", "15e arrondissement"),
        (r"\bparis\s+15eme\b", "15e arrondissement"),
        (r"\bparis\s+15\b", "15e arrondissement"),
        (r"\bparis\s+16e\b", "16e arrondissement"),
        (r"\bparis\s+16eme\b", "16e arrondissement"),
        (r"\bparis\s+16\b", "16e arrondissement"),
        (r"\bparis\s+17e\b", "17e arrondissement"),
        (r"\bparis\s+17eme\b", "17e arrondissement"),
        (r"\bparis\s+17\b", "17e arrondissement"),
        (r"\bparis\s+18e\b", "18e arrondissement"),
        (r"\bparis\s+18eme\b", "18e arrondissement"),
        (r"\bparis\s+18\b", "18e arrondissement"),
        (r"\bparis\s+19e\b", "19e arrondissement"),
        (r"\bparis\s+19eme\b", "19e arrondissement"),
        (r"\bparis\s+19\b", "19e arrondissement"),
        (r"\bparis\s+20e\b", "20e arrondissement"),
        (r"\bparis\s+20eme\b", "20e arrondissement"),
        (r"\bparis\s+20\b", "20e arrondissement"),
    ]

    for pattern, arrondissement in patterns:
        if re.search(pattern, text):
            return arrondissement

    return None


def infer_paris_arrondissement(
    *,
    city_business: str | None,
    city_canonical: str | None,
    postal_code: str | None,
    address: str | None,
) -> str | None:
    if _clean_text(city_business) != "Paris":
        return None

    canonical = _clean_text(city_canonical)
    if canonical and canonical != "Paris":
        normalized = normalize_for_matching(canonical)
        match = re.match(r"^paris\s+(\d{1,2})(er|e|eme)?\s+arrondissement$", normalized)
        if match:
            num = int(match.group(1))
            if num == 1:
                return "1er arrondissement"
            if 2 <= num <= 20:
                return f"{num}e arrondissement"

    by_postal = arrondissement_from_postal_code(postal_code)
    if by_postal is not None:
        return by_postal

    by_address = arrondissement_from_address(address)
    if by_address is not None:
        return by_address

    return None


def fetch_paris_rows() -> pd.DataFrame:
    engine = create_postgres_engine()
    sql = """
    SELECT
        id,
        city,
        city_canonical,
        city_business,
        postal_code,
        address
    FROM establishments
    WHERE city_business = 'Paris'
    """
    return pd.read_sql_query(sql, engine)


def build_arrondissement_mapping(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["paris_arrondissement"] = out.apply(
        lambda row: infer_paris_arrondissement(
            city_business=row.get("city_business"),
            city_canonical=row.get("city_canonical"),
            postal_code=row.get("postal_code"),
            address=row.get("address"),
        ),
        axis=1,
    )
    return out[["id", "city", "city_canonical", "postal_code", "address", "paris_arrondissement"]]


def write_audit(output_dir: Path, mapping_df: pd.DataFrame) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    mapping_df.to_csv(output_dir / "paris_arrondissement_mapping.csv", index=False)

    summary = (
        mapping_df.assign(is_filled=mapping_df["paris_arrondissement"].notna())
        .groupby("paris_arrondissement", dropna=False, as_index=False)
        .size()
        .rename(columns={"size": "total_rows"})
        .sort_values("total_rows", ascending=False)
    )
    summary.to_csv(output_dir / "paris_arrondissement_summary.csv", index=False)

    logger.info("Audit files written to %s", output_dir)


def apply_to_postgres(mapping_df: pd.DataFrame) -> None:
    engine = create_postgres_engine()

    with engine.begin() as connection:
        connection.execute(text(ADD_COLUMN_SQL))
        connection.execute(text("""
            UPDATE establishments
            SET paris_arrondissement = NULL
            WHERE city_business = 'Paris'
        """))

    tmp_table = "tmp_paris_arrondissement_mapping"
    mapping_df[["id", "paris_arrondissement"]].to_sql(
        tmp_table,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    with engine.begin() as connection:
        connection.execute(text(f"""
            UPDATE establishments e
            SET paris_arrondissement = m.paris_arrondissement
            FROM {tmp_table} m
            WHERE e.id = m.id
        """))
        connection.execute(text(f"DROP TABLE IF EXISTS {tmp_table}"))

    logger.info("Paris arrondissement enrichment successfully written to PostgreSQL.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Infer Paris arrondissement from city/postal code/address."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the inferred arrondissement into PostgreSQL.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/processed/paris_arrondissement_enrichment",
        help="Directory for audit CSV files.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    logger.info("Fetching Paris rows from PostgreSQL...")
    paris_df = fetch_paris_rows()

    logger.info("Building arrondissement mapping...")
    mapping_df = build_arrondissement_mapping(paris_df)

    total_rows = len(mapping_df)
    filled_rows = int(mapping_df["paris_arrondissement"].notna().sum())

    logger.info("Total Paris rows: %d", total_rows)
    logger.info("Rows with inferred arrondissement: %d", filled_rows)
    logger.info("Coverage: %.2f%%", 100.0 * filled_rows / total_rows if total_rows else 0.0)

    write_audit(output_dir, mapping_df)

    summary = (
        mapping_df["paris_arrondissement"]
        .value_counts(dropna=False)
        .rename_axis("paris_arrondissement")
        .reset_index(name="count")
    )
    logger.info("Arrondissement summary:\n%s", summary.head(25).to_string(index=False))

    if args.apply:
        apply_to_postgres(mapping_df)
    else:
        logger.info("Dry run only. No database changes applied. Use --apply to write to PostgreSQL.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())