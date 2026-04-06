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
OUTPUT_DIR = Path("data/processed/matching_scraping_paris_v3")


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
    text = re.sub(r"[^a-z0-9\s\-/,]", " ", text)
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
    return digits[:5] if len(digits) >= 5 else ""


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
        r"\bsarl\b": " ",
        r"\bsas\b": " ",
        r"\bsasu\b": " ",
        r"\beurl\b": " ",
        r"\bsnc\b": " ",
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
        "hotel",
        "le",
        "la",
        "les",
        "de",
        "du",
        "des",
        "au",
        "aux",
        "chez",
        "paris",
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
        r"\bfb\b": "faubourg",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    text = text.replace("-", " ")
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
    return " ".join(text.split()[:n]).strip()


def build_exact_address_key(address_norm: str, postal_code_norm: str, city_norm: str) -> str:
    if not address_norm or not postal_code_norm or not city_norm:
        return ""
    return f"{postal_code_norm}|{city_norm}|{address_norm}"


def build_soft_block_key(address_norm: str, postal_code_norm: str, city_norm: str) -> str:
    if not address_norm or not postal_code_norm or not city_norm:
        return ""

    number = extract_street_number(address_norm)
    rest = remove_street_number(address_norm)
    prefix = first_tokens(rest, n=3)

    if number and prefix:
        return f"{postal_code_norm}|{city_norm}|{number}|{prefix}"
    if prefix:
        return f"{postal_code_norm}|{city_norm}|{prefix}"
    return ""


def build_name_cp_key(name_norm: str, postal_code_norm: str) -> str:
    if not name_norm or not postal_code_norm:
        return ""
    return f"{postal_code_norm}|{first_tokens(name_norm, n=3)}"


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


def split_address_variants(raw_address: object) -> list[str]:
    text = str(raw_address or "").strip()
    if not text:
        return []

    parts = re.split(r"\s*\|\s*|\s*/\s*|\s*,\s*|\s+angle\s+", text, flags=re.IGNORECASE)
    parts = [p.strip() for p in parts if p.strip()]

    expanded: list[str] = []
    for part in parts:
        lowered = strip_accents(part.lower())
        matches = re.findall(
            r"(\d+\s+(?:rue|avenue|boulevard|place|quai|faubourg)[^0-9|/]*)",
            lowered,
        )
        if matches and len(matches) > 1:
            expanded.extend(m.strip() for m in matches if m.strip())
        else:
            expanded.append(part)

    unique_variants: list[str] = []
    seen: set[str] = set()
    for part in [text] + expanded:
        norm = normalize_address(part)
        if norm and norm not in seen:
            seen.add(norm)
            unique_variants.append(norm)

    return unique_variants[:5]


def load_scraping_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Scraping CSV not found: {path}")

    df = pd.read_csv(path, sep=";").copy()
    df["scraping_row_id"] = range(1, len(df) + 1)

    df["postal_code_norm"] = df["postal_code"].apply(normalize_postal_code)
    df["city_norm"] = df["city"].apply(normalize_city)
    df["name_norm"] = df["name"].apply(normalize_business_name)
    df["phone_norm"] = df["phone"].apply(normalize_phone)
    df["review_count_num"] = df["review_count_text"].apply(parse_review_count)

    paris_mask = (
        (df["city_norm"] == "paris")
        & df["postal_code_norm"].str.match(r"^750\d{2}$", na=False)
    )
    df = df[paris_mask].copy()

    df["review_platform"] = df["review_platform"].replace({"Tripadvisor": "TripAdvisor"})
    df["address_variants"] = df["address"].apply(split_address_variants)

    exploded = df.explode("address_variants").copy()
    exploded["address_norm"] = exploded["address_variants"].fillna("").astype(str)
    exploded["exact_address_key"] = exploded.apply(
        lambda row: build_exact_address_key(row["address_norm"], row["postal_code_norm"], row["city_norm"]),
        axis=1,
    )
    exploded["soft_block_key"] = exploded.apply(
        lambda row: build_soft_block_key(row["address_norm"], row["postal_code_norm"], row["city_norm"]),
        axis=1,
    )
    exploded["name_cp_key"] = exploded.apply(
        lambda row: build_name_cp_key(row["name_norm"], row["postal_code_norm"]),
        axis=1,
    )

    logger.info("Scraping rows loaded: %d | Paris rows kept: %d", len(pd.read_csv(path, sep=';')), len(df))
    logger.info("Exploded scraping candidate rows: %d", len(exploded))
    return exploded


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
        lambda row: build_exact_address_key(row["address_norm"], row["postal_code_norm"], row["city_norm"]),
        axis=1,
    )
    df["soft_block_key"] = df.apply(
        lambda row: build_soft_block_key(row["address_norm"], row["postal_code_norm"], row["city_norm"]),
        axis=1,
    )
    df["name_cp_key"] = df.apply(
        lambda row: build_name_cp_key(row["name_norm"], row["postal_code_norm"]),
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
        "name_cp_key",
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
        "name_cp_key",
    ]

    scrape = scraping_df[scrape_cols].copy()
    pg = postgres_df[pg_cols].copy()

    # 1) exact address
    exact = scrape.merge(
        pg,
        how="left",
        on="exact_address_key",
        suffixes=("_scrape", "_pg"),
    )
    exact["match_mode"] = "exact_address"

    exact_matched_ids = set(exact.loc[exact["id"].notna(), "scraping_row_id"].unique())

    # 2) soft block
    scrape_soft = scrape[~scrape["scraping_row_id"].isin(exact_matched_ids)].copy()
    soft = scrape_soft.merge(
        pg,
        how="left",
        on="soft_block_key",
        suffixes=("_scrape", "_pg"),
    )
    soft["match_mode"] = "soft_block"

    soft_matched_ids = set(soft.loc[soft["id"].notna(), "scraping_row_id"].unique())

    # 3) fallback maîtrisé par code postal, sans merge massif
    scrape_fallback = scrape[~scrape["scraping_row_id"].isin(exact_matched_ids | soft_matched_ids)].copy()

    pg_by_cp: dict[str, pd.DataFrame] = {
        cp: grp.copy()
        for cp, grp in pg.groupby("postal_code_norm", dropna=False)
    }

    fallback_rows: list[dict] = []

    for _, srow in scrape_fallback.iterrows():
        cp = srow["postal_code_norm"]
        if not cp:
            continue

        candidates = pg_by_cp.get(cp)
        if candidates is None or candidates.empty:
            continue

        s_name = srow["name_norm"]
        s_addr = srow["address_norm"]
        s_phone = srow["phone_norm"]

        cand = candidates.copy()

        cand["name_score_source"] = cand["name_norm"].apply(lambda x: similarity(s_name, x))
        cand["name_score_inpi"] = cand["inpi_name_norm"].apply(lambda x: similarity(s_name, x))
        cand["best_name_score_tmp"] = cand[["name_score_source", "name_score_inpi"]].max(axis=1)

        # préfiltre pour éviter trop de candidats faibles
        cand = cand[cand["best_name_score_tmp"] >= 0.72].copy()
        if cand.empty:
            continue

        cand["address_score_tmp"] = cand["address_norm"].apply(lambda x: similarity(s_addr, x))
        cand["phone_match_tmp"] = cand["phone_norm"].apply(
            lambda x: bool(s_phone and x and s_phone == x)
        )

        cand["same_postal_code_tmp"] = True
        cand["strategy_boost_tmp"] = 0.0

        cand["final_score_tmp"] = (
            0.50 * cand["best_name_score_tmp"]
            + 0.25 * cand["address_score_tmp"]
            + 0.15 * cand["phone_match_tmp"].astype(float)
            + 0.02 * cand["same_postal_code_tmp"].astype(float)
            + cand["strategy_boost_tmp"]
        ).clip(upper=1.0)

        # ne garder que les meilleurs candidats du fallback pour chaque ligne scraping
        cand = cand.sort_values(
            ["final_score_tmp", "best_name_score_tmp", "address_score_tmp", "phone_match_tmp"],
            ascending=[False, False, False, False],
        ).head(8)

        for _, prow in cand.iterrows():
            fallback_rows.append(
                {
                    "scraping_row_id": srow["scraping_row_id"],
                    "name_scrape": srow["name"],
                    "address_scrape": srow["address"],
                    "city_scrape": srow["city"],
                    "postal_code_scrape": srow["postal_code"],
                    "phone_scrape": srow["phone"],
                    "external_review": srow["external_review"],
                    "review_platform": srow["review_platform"],
                    "review_count_text": srow["review_count_text"],
                    "review_count_num": srow["review_count_num"],
                    "status": srow["status"],
                    "description": srow["description"],
                    "tags": srow["tags"],
                    "type": srow["type"],
                    "_source": srow["_source"],
                    "source_url": srow["source_url"],
                    "name_norm_scrape": srow["name_norm"],
                    "address_norm_scrape": srow["address_norm"],
                    "postal_code_norm_scrape": srow["postal_code_norm"],
                    "city_norm_scrape": srow["city_norm"],
                    "phone_norm_scrape": srow["phone_norm"],
                    "id": prow["id"],
                    "name_pg": prow["name"],
                    "address_pg": prow["address"],
                    "city_pg": prow["city"],
                    "city_canonical": prow["city_canonical"],
                    "city_business": prow["city_business"],
                    "postal_code_pg": prow["postal_code"],
                    "phone_pg": prow["phone"],
                    "siret": prow["siret"],
                    "category": prow["category"],
                    "subcategory": prow["subcategory"],
                    "inpi_rne_company_name": prow["inpi_rne_company_name"],
                    "inpi_rne_representative_name": prow["inpi_rne_representative_name"],
                    "inpi_rne_date_creation": prow["inpi_rne_date_creation"],
                    "inpi_rne_main_siret": prow["inpi_rne_main_siret"],
                    "name_norm_pg": prow["name_norm"],
                    "inpi_name_norm": prow["inpi_name_norm"],
                    "address_norm_pg": prow["address_norm"],
                    "postal_code_norm_pg": prow["postal_code_norm"],
                    "city_norm_pg": prow["city_norm"],
                    "phone_norm_pg": prow["phone_norm"],
                    "match_mode": "name_cp_fallback",
                }
            )

    fallback = pd.DataFrame(fallback_rows)

    merged = pd.concat([exact, soft, fallback], ignore_index=True, sort=False)

    for col in [
        "name_norm_scrape",
        "name_norm_pg",
        "inpi_name_norm",
        "phone_norm_scrape",
        "phone_norm_pg",
        "address_norm_scrape",
        "address_norm_pg",
        "postal_code_norm_scrape",
        "postal_code_norm_pg",
    ]:
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

    merged["same_postal_code"] = (
        merged.get("postal_code_norm_scrape", "") == merged.get("postal_code_norm_pg", "")
    )

    def strategy_boost(row: pd.Series) -> float:
        mode = row.get("match_mode", "")
        if mode == "exact_address":
            return 0.08
        if mode == "soft_block":
            return 0.04
        return 0.0

    merged["strategy_boost"] = merged.apply(strategy_boost, axis=1)

    merged["final_score"] = (
        0.50 * merged["best_name_score"]
        + 0.25 * merged["address_score"]
        + 0.15 * merged["phone_match"].astype(float)
        + 0.02 * merged["same_postal_code"].astype(float)
        + merged["strategy_boost"]
    ).clip(upper=1.0).round(4)

    return merged


def classify_matches(candidate_pairs: pd.DataFrame, scraping_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    high_rows: list[dict] = []
    medium_rows: list[dict] = []
    review_rows: list[dict] = []
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
            "same_postal_code": bool(top["same_postal_code"]),
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

        best_name = row["best_name_score"]
        addr_score = row["address_score"]
        phone_match = row["phone_match"]
        final_score = row["final_score"]
        candidate_count = row["candidate_count"]
        gap_ok = (score_gap is None) or (score_gap >= 0.08)

        level = "review"

        if best_name >= 0.88 and addr_score >= 0.80 and gap_ok:
            level = "high"
        elif best_name >= 0.82 and addr_score >= 0.72 and phone_match and gap_ok:
            level = "high"
        elif final_score >= 0.84 and best_name >= 0.80 and gap_ok:
            level = "medium"
        elif final_score >= 0.78 and best_name >= 0.75 and (phone_match or addr_score >= 0.70):
            level = "medium"
        elif final_score >= 0.70 and best_name >= 0.68:
            level = "review"

        # garde-fou
        if best_name < 0.68:
            level = "review"

        row["match_confidence"] = level

        if level == "high":
            high_rows.append(row)
        elif level == "medium":
            medium_rows.append(row)
        else:
            review_rows.append(row)

    matched_high = pd.DataFrame(high_rows)
    matched_medium = pd.DataFrame(medium_rows)
    matched_review = pd.DataFrame(review_rows)
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

    return matched_high, matched_medium, matched_review, unmatched


def save_outputs(
    scraping_df: pd.DataFrame,
    postgres_df: pd.DataFrame,
    matched_high: pd.DataFrame,
    matched_medium: pd.DataFrame,
    matched_review: pd.DataFrame,
    unmatched: pd.DataFrame,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    scraping_df.to_parquet(OUTPUT_DIR / "scraping_paris_normalized_v3.parquet", index=False)
    postgres_df.to_parquet(OUTPUT_DIR / "postgres_paris_reference_v3.parquet", index=False)

    matched_high.to_csv(OUTPUT_DIR / "matched_high_confidence_v3.csv", index=False)
    matched_high.to_parquet(OUTPUT_DIR / "matched_high_confidence_v3.parquet", index=False)

    matched_medium.to_csv(OUTPUT_DIR / "matched_medium_confidence_v3.csv", index=False)
    matched_medium.to_parquet(OUTPUT_DIR / "matched_medium_confidence_v3.parquet", index=False)

    matched_review.to_csv(OUTPUT_DIR / "matched_review_needed_v3.csv", index=False)
    matched_review.to_parquet(OUTPUT_DIR / "matched_review_needed_v3.parquet", index=False)

    unmatched.to_csv(OUTPUT_DIR / "unmatched_scraping_v3.csv", index=False)
    unmatched.to_parquet(OUTPUT_DIR / "unmatched_scraping_v3.parquet", index=False)


def main() -> None:
    logger.info("Loading scraping CSV...")
    scraping_df = load_scraping_csv(SCRAPING_CSV_PATH)

    logger.info("Loading PostgreSQL Paris reference dataset...")
    postgres_df = load_postgres_paris()

    logger.info("Building candidate pairs...")
    logger.info("This step can take time on fallback matching, but should no longer explode memory.")
    candidate_pairs = build_candidate_pairs(scraping_df, postgres_df)

    logger.info("Classifying matches...")
    matched_high, matched_medium, matched_review, unmatched = classify_matches(
        candidate_pairs=candidate_pairs,
        scraping_df=scraping_df,
    )

    logger.info("Saving outputs to %s", OUTPUT_DIR)
    save_outputs(
        scraping_df=scraping_df,
        postgres_df=postgres_df,
        matched_high=matched_high,
        matched_medium=matched_medium,
        matched_review=matched_review,
        unmatched=unmatched,
    )

    logger.info("Summary:")
    logger.info("  Scraping Paris rows: %d", scraping_df["scraping_row_id"].nunique())
    logger.info("  PostgreSQL Paris rows: %d", len(postgres_df))
    logger.info("  High confidence matches V3: %d", len(matched_high))
    logger.info("  Medium confidence matches V3: %d", len(matched_medium))
    logger.info("  Review needed matches V3: %d", len(matched_review))
    logger.info("  Unmatched scraping rows V3: %d", len(unmatched))

    print()
    print("Files written:")
    print(f"- {OUTPUT_DIR / 'matched_high_confidence_v3.csv'}")
    print(f"- {OUTPUT_DIR / 'matched_medium_confidence_v3.csv'}")
    print(f"- {OUTPUT_DIR / 'matched_review_needed_v3.csv'}")
    print(f"- {OUTPUT_DIR / 'unmatched_scraping_v3.csv'}")


if __name__ == "__main__":
    main()