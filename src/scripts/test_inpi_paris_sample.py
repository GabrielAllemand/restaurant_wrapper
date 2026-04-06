from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.extractors.inpi_rne_client import (
    InpiRneClient,
    extract_inpi_rne_fields,
    load_inpi_rne_config,
    normalize_siren,
)
from src.loaders.postgres_loader import create_postgres_engine


def main() -> None:
    engine = create_postgres_engine()

    sql = """
    SELECT
        id,
        name,
        siret,
        city_business,
        city,
        postal_code,
        address
    FROM establishments
    WHERE city_business = 'Paris'
      AND siret IS NOT NULL
      AND TRIM(siret) <> ''
    ORDER BY id
    LIMIT 20
    """

    df = pd.read_sql_query(text(sql), engine)

    df["siret_clean"] = df["siret"].astype(str).str.replace(r"\D", "", regex=True)
    df["siren"] = df["siret_clean"].str[:9]
    df = df[df["siren"].str.len() == 9].copy()

    config = load_inpi_rne_config()
    client = InpiRneClient(config)
    client.login()

    rows = []

    for _, row in df.iterrows():
        siren = normalize_siren(row["siren"])
        payload = client.get_company_by_siren(siren) if siren else None

        extracted = extract_inpi_rne_fields(payload) if payload else {}

        rows.append(
            {
                "id": row["id"],
                "name": row["name"],
                "siret": row["siret"],
                "siren": siren,
                "city_business": row["city_business"],
                "city": row["city"],
                "postal_code": row["postal_code"],
                "address": row["address"],
                "inpi_rne_company_name": extracted.get("inpi_rne_company_name"),
                "inpi_rne_date_creation": extracted.get("inpi_rne_date_creation"),
                "inpi_rne_main_siret": extracted.get("inpi_rne_main_siret"),
                "inpi_rne_main_city": extracted.get("inpi_rne_main_city"),
                "inpi_rne_main_postal_code": extracted.get("inpi_rne_main_postal_code"),
                "inpi_rne_forme_juridique": extracted.get("inpi_rne_forme_juridique"),
                "inpi_rne_diffusion_commerciale": extracted.get("inpi_rne_diffusion_commerciale"),
                "inpi_rne_diffusion_insee": extracted.get("inpi_rne_diffusion_insee"),
                "inpi_rne_representative_name": extracted.get("inpi_rne_representative_name"),
                "inpi_rne_last_event_code": extracted.get("inpi_rne_last_event_code"),
                "inpi_rne_last_event_date_effet": extracted.get("inpi_rne_last_event_date_effet"),
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv("data/processed/inpi_rne_paris_test.csv", index=False)
    out.to_parquet("data/processed/inpi_rne_paris_test.parquet", index=False)

    print(out.head(20).to_string())
    print()
    print("Written:")
    print("- data/processed/inpi_rne_paris_test.csv")
    print("- data/processed/inpi_rne_paris_test.parquet")


if __name__ == "__main__":
    main()