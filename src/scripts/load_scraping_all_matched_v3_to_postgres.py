from __future__ import annotations

import pandas as pd

from src.loaders.postgres_loader import create_postgres_engine
from src.utils.logger import get_logger


logger = get_logger(__name__)

INPUTS = [
    ("data/processed/matching_scraping_paris_v3/matched_high_confidence_v3.csv", "high"),
    ("data/processed/matching_scraping_paris_v3/matched_medium_confidence_v3.csv", "medium"),
    ("data/processed/matching_scraping_paris_v3/matched_review_needed_v3.csv", "review"),
]

TABLE_NAME = "establishments_scraping_enrichment_paris"


def confidence_rank(value: str) -> int:
    mapping = {
        "high": 3,
        "medium": 2,
        "review": 1,
    }
    return mapping.get(str(value).strip().lower(), 0)


def parse_scrape_rating(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def load_and_concat() -> pd.DataFrame:
    frames = []

    for path, expected_confidence in INPUTS:
        logger.info("Loading file: %s", path)
        df = pd.read_csv(path).copy()

        if "match_confidence" not in df.columns:
            df["match_confidence"] = expected_confidence
        else:
            df["match_confidence"] = df["match_confidence"].fillna(expected_confidence)

        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    logger.info("Concatenated rows before dedup: %d", len(out))
    return out


def main() -> None:
    df = load_and_concat()

    df["confidence_rank"] = df["match_confidence"].apply(confidence_rank)

    df = df.sort_values(
        ["postgres_id", "confidence_rank", "final_score", "best_name_score", "address_score"],
        ascending=[True, False, False, False, False],
    ).drop_duplicates(subset=["postgres_id"], keep="first")

    logger.info("Rows after dedup on postgres_id: %d", len(df))

    keep_cols = [
        "postgres_id",
        "scrape_name",
        "scrape_address",
        "scrape_postal_code",
        "scrape_city",
        "scrape_phone",
        "scrape_rating",
        "scrape_review_count_num",
        "scrape_review_platform",
        "scrape_description",
        "scrape_tags",
        "scrape_type",
        "scrape_status",
        "scrape_source",
        "scrape_source_url",
        "match_mode",
        "match_confidence",
        "name_score_source",
        "name_score_inpi",
        "best_name_score",
        "address_score",
        "phone_match",
        "same_postal_code",
        "final_score",
        "candidate_count",
        "score_gap_vs_second",
    ]

    df = df[keep_cols].copy()

    df = df.rename(
        columns={
            "match_mode": "scrape_match_mode",
            "match_confidence": "scrape_match_confidence",
            "name_score_source": "scrape_name_score_source",
            "name_score_inpi": "scrape_name_score_inpi",
            "best_name_score": "scrape_best_name_score",
            "address_score": "scrape_address_score",
            "phone_match": "scrape_phone_match",
            "same_postal_code": "scrape_same_postal_code",
            "final_score": "scrape_final_score",
            "candidate_count": "scrape_candidate_count",
            "score_gap_vs_second": "scrape_score_gap_vs_second",
            "scrape_review_count_num": "scrape_review_count",
        }
    )

    df["scrape_rating"] = df["scrape_rating"].apply(parse_scrape_rating)
    df["scrape_review_count"] = pd.to_numeric(df["scrape_review_count"], errors="coerce")
    df["scrape_name_score_source"] = pd.to_numeric(df["scrape_name_score_source"], errors="coerce")
    df["scrape_name_score_inpi"] = pd.to_numeric(df["scrape_name_score_inpi"], errors="coerce")
    df["scrape_best_name_score"] = pd.to_numeric(df["scrape_best_name_score"], errors="coerce")
    df["scrape_address_score"] = pd.to_numeric(df["scrape_address_score"], errors="coerce")
    df["scrape_final_score"] = pd.to_numeric(df["scrape_final_score"], errors="coerce")
    df["scrape_candidate_count"] = pd.to_numeric(df["scrape_candidate_count"], errors="coerce")
    df["scrape_score_gap_vs_second"] = pd.to_numeric(df["scrape_score_gap_vs_second"], errors="coerce")

    engine = create_postgres_engine()

    logger.info("Truncating staging table before reload...")
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE {TABLE_NAME}")

    logger.info("Loading rows into PostgreSQL...")
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    logger.info("Loaded %d rows into %s", len(df), TABLE_NAME)

    print()
    print("Loaded rows:", len(df))
    print("Confidence distribution:")
    print(df["scrape_match_confidence"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()