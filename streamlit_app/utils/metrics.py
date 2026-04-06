from __future__ import annotations

import pandas as pd


RICHNESS_FIELDS = [
    "address",
    "postal_code",
    "city",
    "phone",
    "website",
    "email",
    "siret",
    "opening_hours",
]


def compute_richness_score(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    score = 0
    for col in RICHNESS_FIELDS:
        if col in output.columns:
            score = score + output[col].notna().astype(int)
    output["richness_score"] = score
    return output


def percentage(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return round(100.0 * numerator / denominator, 2)


def kpi_percentages(kpi_df: pd.DataFrame) -> dict[str, float]:
    row = kpi_df.iloc[0]
    total = row["total_rows"]

    return {
        "pct_address": percentage(row["address_filled"], total),
        "pct_postal_code": percentage(row["postal_code_filled"], total),
        "pct_city": percentage(row["city_filled"], total),
        "pct_phone": percentage(row["phone_filled"], total),
        "pct_website": percentage(row["website_filled"], total),
        "pct_email": percentage(row["email_filled"], total),
        "pct_siret": percentage(row["siret_filled"], total),
        "pct_ban": percentage(row["ban_address_filled"], total),
        "pct_sirene": percentage(row["sirene_siret_filled"], total),
    }