from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path("data/processed/matching_scraping_paris_v3")
HIGH_PATH = BASE_DIR / "matched_high_confidence_v3.csv"
MEDIUM_PATH = BASE_DIR / "matched_medium_confidence_v3.csv"
REVIEW_PATH = BASE_DIR / "matched_review_needed_v3.csv"
UNMATCHED_PATH = BASE_DIR / "unmatched_scraping_v3.csv"
OUTPUT_PATH = BASE_DIR / "matching_audit_report_v3.txt"


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)


def pct(n: int | float, d: int | float) -> float:
    if not d:
        return 0.0
    return round(100.0 * float(n) / float(d), 2)


def describe_block(name: str, df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    total = len(df)
    lines.append(f"{name} total: {total}")
    if total == 0:
        lines.append("")
        return lines

    final_score = pd.to_numeric(df.get("final_score"), errors="coerce")
    name_score = pd.to_numeric(df.get("best_name_score"), errors="coerce")
    address_score = pd.to_numeric(df.get("address_score"), errors="coerce")
    phone_match = df.get("phone_match", pd.Series([False] * len(df))).fillna(False).astype(bool)

    lines.append(f"Phone match = True: {int(phone_match.sum())} ({pct(int(phone_match.sum()), total)}%)")
    lines.append("Distribution final_score:")
    lines.append(str(final_score.describe()))
    lines.append("")
    lines.append("Distribution best_name_score:")
    lines.append(str(name_score.describe()))
    lines.append("")
    lines.append("Distribution address_score:")
    lines.append(str(address_score.describe()))
    lines.append("")
    return lines


def main() -> None:
    high_df = load_csv(HIGH_PATH)
    medium_df = load_csv(MEDIUM_PATH)
    review_df = load_csv(REVIEW_PATH)
    unmatched_df = load_csv(UNMATCHED_PATH)

    lines: list[str] = []
    lines.append("=== AUDIT V3 MATCHING SCRAPING PARIS ===")
    lines.append("")

    lines.extend(describe_block("HIGH", high_df))
    lines.extend(describe_block("MEDIUM", medium_df))
    lines.extend(describe_block("REVIEW", review_df))

    total = len(high_df) + len(medium_df) + len(review_df) + len(unmatched_df)
    matched_total = len(high_df) + len(medium_df) + len(review_df)

    lines.append(f"Matched total V3: {matched_total} / {total} ({pct(matched_total, total)}%)")
    lines.append(f"High only V3: {len(high_df)} / {total} ({pct(len(high_df), total)}%)")
    lines.append(f"High + medium V3: {len(high_df) + len(medium_df)} / {total} ({pct(len(high_df)+len(medium_df), total)}%)")
    lines.append(f"Unmatched V3: {len(unmatched_df)} / {total} ({pct(len(unmatched_df), total)}%)")
    lines.append("")
    lines.append("Top unmatched by review count:")
    top_unmatched = unmatched_df.sort_values(
        by=[c for c in ["scrape_review_count_num"] if c in unmatched_df.columns],
        ascending=False,
    ).head(20)
    cols = [c for c in ["scrape_name", "scrape_address", "scrape_postal_code", "scrape_phone", "scrape_rating", "scrape_review_platform", "scrape_review_count_text"] if c in top_unmatched.columns]
    lines.append(top_unmatched[cols].to_string(index=False) if not top_unmatched.empty else "Aucun")
    lines.append("")

    report = "\n".join(lines)
    OUTPUT_PATH.write_text(report, encoding="utf-8")
    print(report)
    print()
    print(f"Written: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()