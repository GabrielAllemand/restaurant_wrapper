from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.loaders.postgres_loader import create_postgres_engine
from src.utils.logger import get_logger


logger = get_logger(__name__)

SCRAPING_CSV_PATH = Path("data/raw/fusion_finale_complete.csv")
OUTPUT_DIR = Path("data/processed/matching_scraping_paris_v2")


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = strip_accents(text.lower())
    text = text.replace("&", " and ")
    text = re.sub(r"[’'`]", " ", text)
    text = re.sub(r"[^a-z0-9\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_city(value: object) -> str:
    text = clean_text(value)
    if text.startswith("paris"):
        return "paris"
    return text


def normalize_postal_code(value: object) -> str:
    if value is None:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5]
    return ""


def normalize_phone(value: object) -> str:
    if value is None:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())

    if digits.startswith("33") and len(digits) >= 11:
        digits = "0" + digits[2:]

    if len(digits) == 9:
        digits = "0" + digits

    return digits


def normalize_business_name(value: object) -> str:
    text = clean_text(value)

    replacements = {
        r"\bst\b": "saint",
        r"\bste\b": "sainte",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    weak_words = {
        "restaurant",
        "resto",
        "bar",
        "cafe",
        "brasserie",
        "boulangerie",
        "patisserie",
        "epicerie",
        "snack",
        "le",
        "la",
        "les",
        "de",
        "du",
        "des",
        "au",
        "aux",
        "chez",
        "sarl",
        "sas",
        "sasu",
        "eurl",
    }

    tokens = [token for token in text.split() if token not in weak_words]
    return " ".join(tokens).strip()


def normalize_address(value: object) -> str:
    text = clean_text(value)

    replacements = {
        r"\bbd\b": "boulevard",
        r"\bav\b": "avenue",
        r"\bave\b": "avenue",
        r"\bpl\b": "place",
        r"\br\b": "rue",
        r"\bst\b": "saint",
        r"\bste\b": "sainte",
        r"\bfbg\b": "faubourg",
        r"\bfg\b": "faubourg",
        r"\bfbourg\b": "faubourg",
        r"\bqu\b": "quai",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    text = text.replace("-", " ")
    text = text.replace(" bis ", " bis ")
    text = text.replace(" ter ", " ter ")

    # enlève mentions parasites fréquentes
    text = re.sub(r"\bparis\b", " ", text)
    text = re.sub(r"\b750\d{2}\b", " ", text)

    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_street_number(address_norm: str) -> str:
    if not address_norm:
        return ""
    match = re.match(r"^(\d+)", address_norm)
    return match.group(1) if match else ""


def remove_street_number(address_norm: str) -> str:
    if not address_norm:
        return ""
    return re.sub(r"^\d+\s*", "", address_norm).strip()


def first_tokens(text: str, n: int = 3) -> str:
    if not text:
        return ""
    tokens = text.split()
    return " ".join(tokens[:n]).strip()


def build_exact_address_key(address_norm: str, postal_code_norm: str, city_norm: str) -> str:
    if not address_norm or not postal_code_norm or not city_norm:
        return ""
    return f"{postal_code_norm}|{city_norm}|{address_norm}"


def build_soft_block_key(address_norm: str, postal_code_norm: str, city_norm: str) -> str:
    if not address_norm or not postal_code_norm or not city_norm:
        return ""

    street_number = extract_street_number(address_norm)
    street_rest = remove_street_number(address_norm)
    street_prefix = first_tokens(street_rest, n=3)

    if street_number and street_prefix:
        return f"{postal_code_norm}|{city_norm}|{street_number}|{street_prefix}"

    if street_prefix:
        return f"{postal_code_norm}|{city_norm}|{street_prefix}"

    return ""


def similarity(left: object, right: object) -> float:
    if left is None or right is None:
        return 0.0
    if pd.isna(left) or pd.isna(right):
        return 0.0

    left_str = str(left).strip()
    right_str = str(right).strip()

    if not left_str or not right_str:
        return 0.0

    return SequenceMatcher(None, left_str, right_str).ratio()


def parse_review_count(value: object) -> int | None:
    if value is None:
        return None
    digits = re.findall(r"\d+", str(value))
    if not digits:
        return None
    try:
        return int(digits[0])
    except ValueError:
        return None


def load_scraping_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Scraping CSV not found: {path}")

    df = pd.read_csv(path, sep=";").copy()
    df["scraping_row_id"] = range(1, len(df) + 1)

    df["postal_code_norm"] = df["postal_code"].apply(normalize_postal_code)
    df["city_norm"] = df["city"].apply(normalize_city)
    df["name_norm"] = df["name"].apply(normalize_business_name)
    df["address_norm"] = df["address"].apply(normalize_address)
    df["phone_norm"] = df["phone"].apply(normalize_phone)

    df["exact_address_key"] = df.apply(
        lambda row: build_exact_address_key(
            row["address_norm"],
            row["postal_code_norm"],
            row["city_norm"],
        ),
        axis=1,
    )
    df["soft_block_key"] = df.apply(
        lambda row: build_soft_block_key(
            row["address_norm"],
            row["postal_code_norm"],
            row["city_norm"],
        ),
        axis=1,
    )

    df["review_count_num"] = df["review_count_text"].apply(parse_review_count)

    paris_mask = (
        (df["city_norm"] == "paris")
        & df["postal_code_norm"].str.match(r"^750\d{2}$", na=False)
    )
    out = df[paris_mask].copy()

    out["review_platform"] = out["review_platform"].replace({"Tripadvisor": "TripAdvisor"})

    logger.info("Scraping rows loaded: %d | Paris rows kept: %d", len(df), len(out))
    return out


def load_postgres_paris() -> pd.DataFrame:
    engine = create_postgres_engine()

    sql = """
    SELECT
        id,
        source,
        source_id,
        name,
        address,
        city,
        city_canonical,
        city_business,
        postal_code,
        phone,
        siret,
        category,
        subcategory,
        inpi_rne_company_name,
        inpi_rne_representative_name,
        inpi_rne_date_creation,
        inpi_rne_main_siret
    FROM establishments
    WHERE city_business = 'Paris'
    """

    df = pd.read_sql_query(text(sql), engine).copy()

    df["postal_code_norm"] = df["postal_code"].apply(normalize_postal_code)
    df["city_norm"] = df["city_business"].apply(normalize_city)
    df["name_norm"] = df["name"].apply(normalize_business_name)
    df["inpi_name_norm"] = df["inpi_rne_company_name"].apply(normalize_business_name)
    df["address_norm"] = df["address"].apply(normalize_address)
    df["phone_norm"] = df["phone"].apply(normalize_phone)

    df["exact_address_key"] = df.apply(
        lambda row: build_exact_address_key(
            row["address_norm"],
            row["postal_code_norm"],
            row["city_norm"],
        ),
        axis=1,
    )
    df["soft_block_key"] = df.apply(
        lambda row: build_soft_block_key(
            row["address_norm"],
            row["postal_code_norm"],
            row["city_norm"],
        ),
        axis=1,
    )

    logger.info("PostgreSQL Paris rows loaded: %d", len(df))
    return df


def build_candidate_pairs(scraping_df: pd.DataFrame, postgres_df: pd.DataFrame) -> pd.DataFrame:
    scrape_cols = [
        "scraping_row_id",
        "name",
        "address",
        "city",
        "postal_code",
        "phone",
        "external_review",
        "review_platform",
        "review_count_text",
        "review_count_num",
        "status",
        "description",
        "tags",
        "type",
        "_source",
        "source_url",
        "name_norm",
        "address_norm",
        "postal_code_norm",
        "city_norm",
        "phone_norm",
        "exact_address_key",
        "soft_block_key",
    ]
    pg_cols = [
        "id",
        "name",
        "address",
        "city",
        "city_canonical",
        "city_business",
        "postal_code",
        "phone",
        "siret",
        "category",
        "subcategory",
        "inpi_rne_company_name",
        "inpi_rne_representative_name",
        "inpi_rne_date_creation",
        "inpi_rne_main_siret",
        "name_norm",
        "inpi_name_norm",
        "address_norm",
        "postal_code_norm",
        "city_norm",
        "phone_norm",
        "exact_address_key",
        "soft_block_key",
    ]

    scrape = scraping_df[scrape_cols].copy()
    pg = postgres_df[pg_cols].copy()

    # 1) candidats exact_address_key
    exact = scrape.merge(
        pg,
        how="left",
        on="exact_address_key",
        suffixes=("_scrape", "_pg"),
    )
    exact["match_mode"] = "exact_address"

    # 2) candidats soft_block_key pour les lignes non déjà matchées exactement
    scrape_without_exact = scrape[~scrape["scraping_row_id"].isin(exact.loc[exact["id"].notna(), "scraping_row_id"].unique())].copy()

    soft = scrape_without_exact.merge(
        pg,
        how="left",
        on="soft_block_key",
        suffixes=("_scrape", "_pg"),
    )
    soft["match_mode"] = "soft_block"

    merged = pd.concat([exact, soft], ignore_index=True, sort=False)

    for col in ["name_norm_scrape", "name_norm_pg", "inpi_name_norm", "phone_norm_scrape", "phone_norm_pg", "address_norm_scrape", "address_norm_pg"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna("")

    merged["name_score_source"] = merged.apply(
        lambda row: similarity(row.get("name_norm_scrape", ""), row.get("name_norm_pg", "")),
        axis=1,
    )
    merged["name_score_inpi"] = merged.apply(
        lambda row: similarity(row.get("name_norm_scrape", ""), row.get("inpi_name_norm", "")),
        axis=1,
    )
    merged["best_name_score"] = merged[["name_score_source", "name_score_inpi"]].max(axis=1)

    merged["address_score"] = merged.apply(
        lambda row: similarity(row.get("address_norm_scrape", ""), row.get("address_norm_pg", "")),
        axis=1,
    )

    merged["phone_match"] = (
        (merged["phone_norm_scrape"] != "")
        & (merged["phone_norm_pg"] != "")
        & (merged["phone_norm_scrape"] == merged["phone_norm_pg"])
    )

    # scoring
    merged["final_score"] = (
        0.55 * merged["best_name_score"]
        + 0.30 * merged["address_score"]
        + 0.15 * merged["phone_match"].astype(float)
    ).round(4)

    return merged


def classify_matches(candidate_pairs: pd.DataFrame, scraping_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    matched_high_confidence_rows: list[dict] = []
    matched_review_needed_rows: list[dict] = []
    unmatched_rows: list[dict] = []

    grouped = candidate_pairs.groupby("scraping_row_id", dropna=False)

    for scraping_row_id, group in grouped:
        group = group.copy()
        base_scrape = group.iloc[0]

        has_pg_candidate = group["id"].notna().any()

        if not has_pg_candidate:
            unmatched_rows.append(
                {
                    "scraping_row_id": scraping_row_id,
                    "scrape_name": base_scrape.get("name_scrape"),
                    "scrape_address": base_scrape.get("address_scrape"),
                    "scrape_postal_code": base_scrape.get("postal_code_scrape"),
                    "scrape_city": base_scrape.get("city_scrape"),
                    "scrape_phone": base_scrape.get("phone_scrape"),
                    "scrape_rating": base_scrape.get("external_review"),
                    "scrape_review_platform": base_scrape.get("review_platform"),
                    "scrape_review_count_text": base_scrape.get("review_count_text"),
                    "scrape_review_count_num": base_scrape.get("review_count_num"),
                    "scrape_source": base_scrape.get("_source"),
                    "scrape_source_url": base_scrape.get("source_url"),
                    "reason": "no_candidate_after_blocking",
                }
            )
            continue

        group = group[group["id"].notna()].copy()
        group = group.sort_values(
            ["final_score", "best_name_score", "address_score", "phone_match"],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)

        top = group.iloc[0]
        second_score = group.iloc[1]["final_score"] if len(group) > 1 else None
        score_gap = round(float(top["final_score"] - second_score), 4) if second_score is not None else None

        row = {
            "scraping_row_id": scraping_row_id,
            "postgres_id": int(top["id"]),
            "match_mode": top.get("match_mode"),
            "scrape_name": top.get("name_scrape"),
            "postgres_name": top.get("name_pg"),
            "postgres_inpi_name": top.get("inpi_rne_company_name"),
            "scrape_address": top.get("address_scrape"),
            "postgres_address": top.get("address_pg"),
            "scrape_postal_code": top.get("postal_code_scrape"),
            "postgres_postal_code": top.get("postal_code_pg"),
            "scrape_city": top.get("city_scrape"),
            "postgres_city_business": top.get("city_business"),
            "scrape_phone": top.get("phone_scrape"),
            "postgres_phone": top.get("phone_pg"),
            "siret": top.get("siret"),
            "category": top.get("category"),
            "subcategory": top.get("subcategory"),
            "name_score_source": round(float(top["name_score_source"]), 4),
            "name_score_inpi": round(float(top["name_score_inpi"]), 4),
            "best_name_score": round(float(top["best_name_score"]), 4),
            "address_score": round(float(top["address_score"]), 4),
            "phone_match": bool(top["phone_match"]),
            "final_score": round(float(top["final_score"]), 4),
            "candidate_count": int(len(group)),
            "score_gap_vs_second": score_gap,
            "scrape_rating": top.get("external_review"),
            "scrape_review_platform": top.get("review_platform"),
            "scrape_review_count_text": top.get("review_count_text"),
            "scrape_review_count_num": top.get("review_count_num"),
            "scrape_status": top.get("status"),
            "scrape_type": top.get("type"),
            "scrape_tags": top.get("tags"),
            "scrape_description": top.get("description"),
            "scrape_source": top.get("_source"),
            "scrape_source_url": top.get("source_url"),
            "inpi_rne_representative_name": top.get("inpi_rne_representative_name"),
            "inpi_rne_date_creation": top.get("inpi_rne_date_creation"),
            "inpi_rne_main_siret": top.get("inpi_rne_main_siret"),
        }

        # règles high confidence strictes
        is_high_confidence = False

        if row["best_name_score"] >= 0.88 and row["address_score"] >= 0.88:
            if row["candidate_count"] == 1 or (score_gap is not None and score_gap >= 0.12):
                is_high_confidence = True

        if row["best_name_score"] >= 0.84 and row["address_score"] >= 0.92 and row["phone_match"]:
            if row["candidate_count"] == 1 or (score_gap is not None and score_gap >= 0.10):
                is_high_confidence = True

        # garde-fou : téléphone seul ne suffit jamais
        if row["best_name_score"] < 0.78:
            is_high_confidence = False

        if is_high_confidence:
            row["match_confidence"] = "high"
            matched_high_confidence_rows.append(row)
        else:
            row["match_confidence"] = "review_needed"
            matched_review_needed_rows.append(row)

    matched_high_confidence = pd.DataFrame(matched_high_confidence_rows)
    matched_review_needed = pd.DataFrame(matched_review_needed_rows)
    unmatched = pd.DataFrame(unmatched_rows)

    seen_scraping_ids = set(candidate_pairs["scraping_row_id"].unique())
    all_scraping_ids = set(scraping_df["scraping_row_id"].unique())
    missing_ids = sorted(all_scraping_ids - seen_scraping_ids)

    if missing_ids:
        extra = scraping_df[scraping_df["scraping_row_id"].isin(missing_ids)].copy()
        extra_rows = []
        for _, row in extra.iterrows():
            extra_rows.append(
                {
                    "scraping_row_id": int(row["scraping_row_id"]),
                    "scrape_name": row["name"],
                    "scrape_address": row["address"],
                    "scrape_postal_code": row["postal_code"],
                    "scrape_city": row["city"],
                    "scrape_phone": row["phone"],
                    "scrape_rating": row["external_review"],
                    "scrape_review_platform": row["review_platform"],
                    "scrape_review_count_text": row["review_count_text"],
                    "scrape_review_count_num": row["review_count_num"],
                    "scrape_source": row["_source"],
                    "scrape_source_url": row["source_url"],
                    "reason": "not_seen_in_candidate_generation",
                }
            )
        unmatched = pd.concat([unmatched, pd.DataFrame(extra_rows)], ignore_index=True)

    return matched_high_confidence, matched_review_needed, unmatched


def save_outputs(
    scraping_df: pd.DataFrame,
    postgres_df: pd.DataFrame,
    matched_high_confidence: pd.DataFrame,
    matched_review_needed: pd.DataFrame,
    unmatched: pd.DataFrame,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    scraping_df.to_parquet(OUTPUT_DIR / "scraping_paris_normalized_v2.parquet", index=False)
    postgres_df.to_parquet(OUTPUT_DIR / "postgres_paris_reference_v2.parquet", index=False)

    matched_high_confidence.to_csv(OUTPUT_DIR / "matched_high_confidence_v2.csv", index=False)
    matched_high_confidence.to_parquet(OUTPUT_DIR / "matched_high_confidence_v2.parquet", index=False)

    matched_review_needed.to_csv(OUTPUT_DIR / "matched_review_needed_v2.csv", index=False)
    matched_review_needed.to_parquet(OUTPUT_DIR / "matched_review_needed_v2.parquet", index=False)

    unmatched.to_csv(OUTPUT_DIR / "unmatched_scraping_v2.csv", index=False)
    unmatched.to_parquet(OUTPUT_DIR / "unmatched_scraping_v2.parquet", index=False)


def main() -> None:
    logger.info("Loading scraping CSV...")
    scraping_df = load_scraping_csv(SCRAPING_CSV_PATH)

    logger.info("Loading PostgreSQL Paris reference dataset...")
    postgres_df = load_postgres_paris()

    logger.info("Building candidate pairs...")
    candidate_pairs = build_candidate_pairs(scraping_df, postgres_df)

    logger.info("Classifying matches...")
    matched_high_confidence, matched_review_needed, unmatched = classify_matches(
        candidate_pairs=candidate_pairs,
        scraping_df=scraping_df,
    )

    logger.info("Saving outputs to %s", OUTPUT_DIR)
    save_outputs(
        scraping_df=scraping_df,
        postgres_df=postgres_df,
        matched_high_confidence=matched_high_confidence,
        matched_review_needed=matched_review_needed,
        unmatched=unmatched,
    )

    logger.info("Summary:")
    logger.info("  Scraping Paris rows: %d", len(scraping_df))
    logger.info("  PostgreSQL Paris rows: %d", len(postgres_df))
    logger.info("  High confidence matches V2: %d", len(matched_high_confidence))
    logger.info("  Review needed matches V2: %d", len(matched_review_needed))
    logger.info("  Unmatched scraping rows V2: %d", len(unmatched))

    print()
    print("Files written:")
    print(f"- {OUTPUT_DIR / 'scraping_paris_normalized_v2.parquet'}")
    print(f"- {OUTPUT_DIR / 'postgres_paris_reference_v2.parquet'}")
    print(f"- {OUTPUT_DIR / 'matched_high_confidence_v2.csv'}")
    print(f"- {OUTPUT_DIR / 'matched_high_confidence_v2.parquet'}")
    print(f"- {OUTPUT_DIR / 'matched_review_needed_v2.csv'}")
    print(f"- {OUTPUT_DIR / 'matched_review_needed_v2.parquet'}")
    print(f"- {OUTPUT_DIR / 'unmatched_scraping_v2.csv'}")
    print(f"- {OUTPUT_DIR / 'unmatched_scraping_v2.parquet'}")


if __name__ == "__main__":
    main()