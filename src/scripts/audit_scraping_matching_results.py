from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)

BASE_DIR = Path("data/processed/matching_scraping_paris_v2")
HIGH_PATH = BASE_DIR / "matched_high_confidence_v2.csv"
REVIEW_PATH = BASE_DIR / "matched_review_needed_v2.csv"
UNMATCHED_PATH = BASE_DIR / "unmatched_scraping_v2.csv"
OUTPUT_PATH = BASE_DIR / "matching_audit_report_v2.txt"


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)


def safe_series(df: pd.DataFrame, col: str, default=None) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def pct(n: int | float, d: int | float) -> float:
    if not d:
        return 0.0
    return round(100.0 * float(n) / float(d), 2)


def summarize_high_confidence(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    total = len(df)

    if total == 0:
        lines.append("Aucun high_confidence à analyser.")
        return lines

    final_score = pd.to_numeric(safe_series(df, "final_score"), errors="coerce")
    name_score = pd.to_numeric(safe_series(df, "best_name_score"), errors="coerce")
    phone_match = safe_series(df, "phone_match", False).fillna(False).astype(bool)
    candidate_count = pd.to_numeric(safe_series(df, "candidate_count"), errors="coerce")
    score_gap = pd.to_numeric(safe_series(df, "score_gap_vs_second"), errors="coerce")

    very_strong = ((final_score >= 0.90) & (name_score >= 0.85)).sum()
    strong = ((final_score >= 0.85) & (name_score >= 0.78)).sum()
    weak_inside_high = (
        (final_score < 0.82)
        | (name_score < 0.75)
        | ((candidate_count > 1) & (score_gap.fillna(999) < 0.10))
    ).sum()

    address_equal = (
        safe_series(df, "scrape_address", "").fillna("").astype(str).str.strip().str.lower()
        == safe_series(df, "postgres_address", "").fillna("").astype(str).str.strip().str.lower()
    ).sum()

    lines.append(f"High confidence total: {total}")
    lines.append(f"Phone match = True: {int(phone_match.sum())} ({pct(int(phone_match.sum()), total)}%)")
    lines.append(f"Adresse strictement identique: {int(address_equal)} ({pct(int(address_equal), total)}%)")
    lines.append(f"Très forts matches (final>=0.90 et nom>=0.85): {int(very_strong)} ({pct(int(very_strong), total)}%)")
    lines.append(f"Forts matches (final>=0.85 et nom>=0.78): {int(strong)} ({pct(int(strong), total)}%)")
    lines.append(f"Suspicious inside high_confidence: {int(weak_inside_high)} ({pct(int(weak_inside_high), total)}%)")
    lines.append("")
    lines.append("Distribution final_score:")
    lines.append(str(final_score.describe()))
    lines.append("")
    lines.append("Distribution best_name_score:")
    lines.append(str(name_score.describe()))
    lines.append("")

    top_suspects = df.loc[
        (final_score < 0.82)
        | (name_score < 0.75)
        | ((candidate_count > 1) & (score_gap.fillna(999) < 0.10)),
        [
            c for c in [
                "scrape_name",
                "postgres_name",
                "postgres_inpi_name",
                "scrape_address",
                "postgres_address",
                "scrape_phone",
                "postgres_phone",
                "best_name_score",
                "phone_match",
                "final_score",
                "candidate_count",
                "score_gap_vs_second",
            ] if c in df.columns
        ],
    ].head(20)

    lines.append("Top 20 high_confidence suspects:")
    lines.append(top_suspects.to_string(index=False) if not top_suspects.empty else "Aucun suspect majeur.")
    lines.append("")

    return lines


def summarize_review_needed(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    total = len(df)

    if total == 0:
        lines.append("Aucun review_needed à analyser.")
        return lines

    final_score = pd.to_numeric(safe_series(df, "final_score"), errors="coerce")
    name_score = pd.to_numeric(safe_series(df, "best_name_score"), errors="coerce")
    phone_match = safe_series(df, "phone_match", False).fillna(False).astype(bool)
    candidate_count = pd.to_numeric(safe_series(df, "candidate_count"), errors="coerce")
    score_gap = pd.to_numeric(safe_series(df, "score_gap_vs_second"), errors="coerce")

    promotable = (
        (final_score >= 0.80)
        & (name_score >= 0.76)
        & ((phone_match) | (candidate_count == 1) | (score_gap.fillna(999) >= 0.12))
    ).sum()

    lines.append(f"Review needed total: {total}")
    lines.append(f"Phone match = True: {int(phone_match.sum())} ({pct(int(phone_match.sum()), total)}%)")
    lines.append(f"Promotable after relaxed review heuristic: {int(promotable)} ({pct(int(promotable), total)}%)")
    lines.append("")
    lines.append("Distribution final_score:")
    lines.append(str(final_score.describe()))
    lines.append("")
    lines.append("Distribution best_name_score:")
    lines.append(str(name_score.describe()))
    lines.append("")

    top_promotable = df.loc[
        (final_score >= 0.80)
        & (name_score >= 0.76)
        & ((phone_match) | (candidate_count == 1) | (score_gap.fillna(999) >= 0.12)),
        [
            c for c in [
                "scrape_name",
                "postgres_name",
                "postgres_inpi_name",
                "scrape_address",
                "postgres_address",
                "scrape_phone",
                "postgres_phone",
                "best_name_score",
                "phone_match",
                "final_score",
                "candidate_count",
                "score_gap_vs_second",
            ] if c in df.columns
        ],
    ].head(20)

    lines.append("Top 20 promotable review_needed rows:")
    lines.append(top_promotable.to_string(index=False) if not top_promotable.empty else "Aucun cas promotable identifié.")
    lines.append("")

    return lines


def summarize_unmatched(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    total = len(df)

    if total == 0:
        lines.append("Aucun unmatched à analyser.")
        return lines

    postal = safe_series(df, "scrape_postal_code", "").fillna("").astype(str)
    source = safe_series(df, "scrape_source", "").fillna("").astype(str)
    platform = safe_series(df, "scrape_review_platform", "").fillna("").astype(str)
    rating = pd.to_numeric(safe_series(df, "scrape_rating"), errors="coerce")
    review_count = pd.to_numeric(safe_series(df, "scrape_review_count_num"), errors="coerce")
    phone = safe_series(df, "scrape_phone", "").fillna("").astype(str)
    reason = safe_series(df, "reason", "").fillna("").astype(str)

    paris_cp_valid = postal.str.match(r"^750\d{2}$", na=False).sum()
    phone_present = (phone.str.strip() != "").sum()
    strong_leads = ((rating >= 4.0) & (review_count >= 20)).sum()

    lines.append(f"Unmatched total: {total}")
    lines.append(f"Code postal Paris valide: {int(paris_cp_valid)} ({pct(int(paris_cp_valid), total)}%)")
    lines.append(f"Téléphone présent: {int(phone_present)} ({pct(int(phone_present), total)}%)")
    lines.append(f"Strong leads (rating>=4.0 et >=20 avis): {int(strong_leads)} ({pct(int(strong_leads), total)}%)")
    lines.append("")
    lines.append("Top sources unmatched:")
    lines.append(source.value_counts(dropna=False).head(20).to_string())
    lines.append("")
    lines.append("Top review platforms unmatched:")
    lines.append(platform.value_counts(dropna=False).head(20).to_string())
    lines.append("")
    lines.append("Top reasons unmatched:")
    lines.append(reason.value_counts(dropna=False).head(20).to_string())
    lines.append("")

    top_unmatched = df.sort_values(
        by=[c for c in ["scrape_review_count_num", "scrape_rating"] if c in df.columns],
        ascending=False,
    ).head(20)

    cols = [
        c for c in [
            "scrape_name",
            "scrape_address",
            "scrape_postal_code",
            "scrape_city",
            "scrape_phone",
            "scrape_rating",
            "scrape_review_platform",
            "scrape_review_count_text",
            "scrape_source",
            "reason",
        ] if c in top_unmatched.columns
    ]
    lines.append("Top 20 unmatched high-value leads:")
    lines.append(top_unmatched[cols].to_string(index=False) if not top_unmatched.empty else "Aucun.")
    lines.append("")

    return lines


def global_recommendation(high_df: pd.DataFrame, review_df: pd.DataFrame, unmatched_df: pd.DataFrame) -> list[str]:
    lines: list[str] = []

    total_high = len(high_df)
    total_review = len(review_df)
    total_unmatched = len(unmatched_df)

    weak_inside_high = 0
    if total_high:
        final_score = pd.to_numeric(safe_series(high_df, "final_score"), errors="coerce")
        name_score = pd.to_numeric(safe_series(high_df, "best_name_score"), errors="coerce")
        candidate_count = pd.to_numeric(safe_series(high_df, "candidate_count"), errors="coerce")
        score_gap = pd.to_numeric(safe_series(high_df, "score_gap_vs_second"), errors="coerce")
        weak_inside_high = int(
            (
                (final_score < 0.82)
                | (name_score < 0.75)
                | ((candidate_count > 1) & (score_gap.fillna(999) < 0.10))
            ).sum()
        )

    promotable_review = 0
    if total_review:
        final_score = pd.to_numeric(safe_series(review_df, "final_score"), errors="coerce")
        name_score = pd.to_numeric(safe_series(review_df, "best_name_score"), errors="coerce")
        phone_match = safe_series(review_df, "phone_match", False).fillna(False).astype(bool)
        candidate_count = pd.to_numeric(safe_series(review_df, "candidate_count"), errors="coerce")
        score_gap = pd.to_numeric(safe_series(review_df, "score_gap_vs_second"), errors="coerce")
        promotable_review = int(
            (
                (final_score >= 0.80)
                & (name_score >= 0.76)
                & ((phone_match) | (candidate_count == 1) | (score_gap.fillna(999) >= 0.12))
            ).sum()
        )

    lines.append("=== RECOMMANDATION AUTOMATIQUE ===")
    if total_high and pct(weak_inside_high, total_high) <= 5:
        lines.append(
            f"Le bloc high_confidence paraît exploitable pour un test de fusion contrôlée : "
            f"{total_high} lignes, avec {weak_inside_high} lignes potentiellement suspectes "
            f"({pct(weak_inside_high, total_high)}%)."
        )
    else:
        lines.append(
            "Le bloc high_confidence demande encore prudence : trop de lignes potentiellement ambiguës "
            "selon les seuils automatiques."
        )

    if promotable_review > 0:
        lines.append(
            f"Le bloc review_needed contient environ {promotable_review} lignes potentiellement promotables "
            "après une deuxième passe de matching plus souple."
        )
    else:
        lines.append("Le bloc review_needed ne semble pas très récupérable sans revoir la logique de matching.")

    if total_unmatched > (total_high + total_review):
        lines.append(
            "Le volume de unmatched est très élevé : il faudra probablement améliorer la normalisation d’adresse "
            "et ajouter une seconde stratégie de matching plus souple."
        )
    else:
        lines.append("Le volume de unmatched reste contenu.")

    lines.append("")
    lines.append("Prochaine étape recommandée :")
    lines.append("1. fusionner seulement matched_high_confidence dans une table de test ou un export de simulation ;")
    lines.append("2. améliorer ensuite le matching pour récupérer une partie de review_needed et unmatched ;")
    lines.append("3. ne pas écrire directement dans les champs finaux de PostgreSQL sans table de staging.")
    lines.append("")

    return lines


def main() -> None:
    high_df = load_csv(HIGH_PATH)
    review_df = load_csv(REVIEW_PATH)
    unmatched_df = load_csv(UNMATCHED_PATH)

    report_lines: list[str] = []
    report_lines.append("=== AUDIT AUTOMATIQUE DU MATCHING SCRAPING PARIS ===")
    report_lines.append("")

    report_lines.extend(summarize_high_confidence(high_df))
    report_lines.extend(summarize_review_needed(review_df))
    report_lines.extend(summarize_unmatched(unmatched_df))
    report_lines.extend(global_recommendation(high_df, review_df, unmatched_df))

    report_text = "\n".join(report_lines)

    OUTPUT_PATH.write_text(report_text, encoding="utf-8")

    print(report_text)
    print()
    print(f"Written: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()