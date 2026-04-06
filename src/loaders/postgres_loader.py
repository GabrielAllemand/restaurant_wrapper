from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.utils.logger import get_logger


logger = get_logger(__name__)


DB_COLUMNS: list[str] = [
    "source",
    "source_id",
    "name",
    "category",
    "subcategory",
    "address",
    "postal_code",
    "city",
    "country",
    "latitude",
    "longitude",
    "phone",
    "email",
    "website",
    "opening_hours",
    "cuisine",
    "rating",
    "review_count",
    "siret",
    "business_status",
    "ban_address",
    "ban_city",
    "ban_postal_code",
    "ban_latitude",
    "ban_longitude",
    "ban_score",
    "ban_source",
    "sirene_name",
    "sirene_address",
    "sirene_city",
    "sirene_postal_code",
    "sirene_siret",
    "sirene_business_status",
    "sirene_match_score",
    "raw_payload",
    "collected_at",
]


UPSERT_UPDATE_COLUMNS: list[str] = [
    "name",
    "category",
    "subcategory",
    "address",
    "postal_code",
    "city",
    "country",
    "latitude",
    "longitude",
    "phone",
    "email",
    "website",
    "opening_hours",
    "cuisine",
    "rating",
    "review_count",
    "siret",
    "business_status",
    "ban_address",
    "ban_city",
    "ban_postal_code",
    "ban_latitude",
    "ban_longitude",
    "ban_score",
    "ban_source",
    "sirene_name",
    "sirene_address",
    "sirene_city",
    "sirene_postal_code",
    "sirene_siret",
    "sirene_business_status",
    "sirene_match_score",
    "raw_payload",
    "collected_at",
]


def create_postgres_engine() -> Engine:
    """
    Crée l'engine SQLAlchemy PostgreSQL à partir de la configuration.
    """
    logger.info(
        "Creating PostgreSQL engine for %s:%s/%s",
        settings.postgres.host,
        settings.postgres.port,
        settings.postgres.database,
    )
    return create_engine(
        settings.postgres.sqlalchemy_url,
        echo=settings.postgres.echo_sql,
        future=True,
    )


def load_schema_sql(schema_path: Path | None = None) -> None:
    """
    Exécute le fichier schema.sql pour créer les objets de base.
    """
    resolved_schema_path = schema_path or (settings.paths.project_root / "schema.sql")

    if not resolved_schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {resolved_schema_path}")

    logger.info("Applying database schema from %s", resolved_schema_path)
    sql_text = resolved_schema_path.read_text(encoding="utf-8")

    engine = create_postgres_engine()
    with engine.begin() as connection:
        connection.execute(text(sql_text))

    logger.info("Database schema applied successfully.")


def table_exists(table_name: str) -> bool:
    """
    Vérifie si une table existe dans le schéma public.
    """
    engine = create_postgres_engine()

    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
        );
        """
    )

    with engine.begin() as connection:
        return bool(connection.execute(query, {"table_name": table_name}).scalar())


def prepare_dataframe_for_postgres(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Prépare un DataFrame standard pour insertion PostgreSQL.
    """
    prepared = dataframe.copy()

    for column in DB_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = None

    prepared = prepared.loc[:, DB_COLUMNS]

    prepared["raw_payload"] = prepared["raw_payload"].apply(_to_json_string_or_none)

    numeric_columns = [
        "latitude",
        "longitude",
        "rating",
        "review_count",
        "ban_latitude",
        "ban_longitude",
        "ban_score",
        "sirene_match_score",
    ]
    for column in numeric_columns:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    prepared["collected_at"] = pd.to_datetime(
        prepared["collected_at"],
        utc=True,
        errors="coerce",
    )

    text_columns = [
        "source",
        "source_id",
        "name",
        "category",
        "subcategory",
        "address",
        "postal_code",
        "city",
        "country",
        "phone",
        "email",
        "website",
        "opening_hours",
        "cuisine",
        "siret",
        "business_status",
        "ban_address",
        "ban_city",
        "ban_postal_code",
        "ban_source",
        "sirene_name",
        "sirene_address",
        "sirene_city",
        "sirene_postal_code",
        "sirene_siret",
        "sirene_business_status",
    ]
    for column in text_columns:
        prepared[column] = prepared[column].where(prepared[column].notna(), None)

    return prepared


def insert_establishments(dataframe: pd.DataFrame, *, if_exists: str = "append") -> int:
    """
    Insère les lignes du DataFrame dans la table establishments.
    """
    if dataframe.empty:
        logger.warning("No rows to insert into PostgreSQL.")
        return 0

    prepared = prepare_dataframe_for_postgres(dataframe)
    engine = create_postgres_engine()

    logger.info("Inserting %d row(s) into PostgreSQL (mode=%s).", len(prepared), if_exists)

    prepared.to_sql(
        name="establishments",
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=1000,
    )

    logger.info("Inserted %d row(s) into PostgreSQL.", len(prepared))
    return len(prepared)


def truncate_establishments() -> None:
    """
    Vide entièrement la table establishments.
    """
    logger.warning("Truncating table establishments.")
    engine = create_postgres_engine()

    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE establishments RESTART IDENTITY;"))

    logger.info("Table establishments truncated successfully.")


def upsert_establishments(dataframe: pd.DataFrame) -> int:
    """
    UPSERT via table temporaire :
    - insertion dans une table de staging
    - merge sur (source, source_id)
    - fallback insert pur pour les lignes sans source_id
    """
    if dataframe.empty:
        logger.warning("No rows to upsert into PostgreSQL.")
        return 0

    prepared = prepare_dataframe_for_postgres(dataframe)
    engine = create_postgres_engine()
    temp_table_name = "establishments_staging"

    logger.info("Upserting %d row(s) into PostgreSQL.", len(prepared))

    with engine.begin() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))

    prepared.to_sql(
        name=temp_table_name,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    insert_columns_sql = ",\n        ".join(DB_COLUMNS)

    select_columns_sql = ",\n        ".join(
        [
            "source",
            "source_id",
            "name",
            "category",
            "subcategory",
            "address",
            "postal_code",
            "city",
            "country",
            "CAST(latitude AS DOUBLE PRECISION) AS latitude",
            "CAST(longitude AS DOUBLE PRECISION) AS longitude",
            "phone",
            "email",
            "website",
            "opening_hours",
            "cuisine",
            "CAST(rating AS DOUBLE PRECISION) AS rating",
            "CAST(review_count AS INTEGER) AS review_count",
            "siret",
            "business_status",
            "ban_address",
            "ban_city",
            "ban_postal_code",
            "CAST(ban_latitude AS DOUBLE PRECISION) AS ban_latitude",
            "CAST(ban_longitude AS DOUBLE PRECISION) AS ban_longitude",
            "CAST(ban_score AS DOUBLE PRECISION) AS ban_score",
            "ban_source",
            "sirene_name",
            "sirene_address",
            "sirene_city",
            "sirene_postal_code",
            "sirene_siret",
            "sirene_business_status",
            "CAST(sirene_match_score AS DOUBLE PRECISION) AS sirene_match_score",
            "CAST(raw_payload AS JSONB) AS raw_payload",
            "CAST(collected_at AS TIMESTAMPTZ) AS collected_at",
        ]
    )

    update_assignments_sql = ",\n        ".join(
        [f"{column} = EXCLUDED.{column}" for column in UPSERT_UPDATE_COLUMNS]
        + ["updated_at = NOW()"]
    )

    upsert_sql = f"""
    INSERT INTO establishments (
        {insert_columns_sql}
    )
    SELECT
        {select_columns_sql}
    FROM {temp_table_name}
    WHERE source_id IS NOT NULL
    ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
    DO UPDATE SET
        {update_assignments_sql};
    """

    insert_without_source_id_sql = f"""
    INSERT INTO establishments (
        {insert_columns_sql}
    )
    SELECT
        {select_columns_sql}
    FROM {temp_table_name}
    WHERE source_id IS NULL;
    """

    with engine.begin() as connection:
        connection.execute(text(upsert_sql))
        connection.execute(text(insert_without_source_id_sql))
        connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))

    logger.info("Upsert completed successfully for %d row(s).", len(prepared))
    return len(prepared)


def _to_json_string_or_none(value: Any) -> str | None:
    """
    Convertit une valeur en JSON valide pour PostgreSQL.
    Gère :
    - dict / list Python
    - chaîne déjà en JSON
    - chaîne de représentation Python issue d'un CSV (quotes simples)
    """
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        try:
            parsed = json.loads(text)
            return json.dumps(parsed, ensure_ascii=False, default=str)
        except json.JSONDecodeError:
            pass

        try:
            parsed = ast.literal_eval(text)
            return json.dumps(parsed, ensure_ascii=False, default=str)
        except (ValueError, SyntaxError):
            return json.dumps(text, ensure_ascii=False)

    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)