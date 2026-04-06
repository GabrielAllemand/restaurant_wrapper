from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from streamlit_app.utils.db import get_engine


CITY_BUSINESS_EXPR = "COALESCE(city_business, city_canonical, city)"
CITY_DETAIL_EXPR = "COALESCE(city_canonical, city_business, city)"
CATEGORY_EXPR = "COALESCE(category_canonical, category, 'Non renseigné')"
SUBCATEGORY_EXPR = "COALESCE(subcategory_canonical, subcategory, 'Non renseigné')"


def _geo_expr(geo_mode: str, only_paris: bool = False) -> str:
    if geo_mode == "detailed" and only_paris:
        return "paris_arrondissement"
    return CITY_DETAIL_EXPR if geo_mode == "detailed" else CITY_BUSINESS_EXPR


def _geo_where_sql(
    geo_mode: str,
    geo_value: str | None,
    only_paris: bool,
) -> tuple[list[str], dict[str, object]]:
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions = ["1=1"]
    params: dict[str, object] = {}

    if only_paris:
        conditions.append("COALESCE(city_business, city) = 'Paris'")
        if geo_mode == "detailed":
            conditions.append("paris_arrondissement IS NOT NULL")

    if geo_value:
        conditions.append(f"{geo_expr} = :geo_value")
        params["geo_value"] = geo_value

    return conditions, params


@st.cache_data(ttl=600, show_spinner=False)
def load_full_dataset(limit: int | None = None) -> pd.DataFrame:
    engine = get_engine()
    limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""

    sql = f"""
    SELECT
        source,
        source_id,
        name,
        {CATEGORY_EXPR} AS category,
        {SUBCATEGORY_EXPR} AS subcategory,
        category AS category_raw,
        subcategory AS subcategory_raw,
        address,
        postal_code,
        city,
        city_canonical,
        city_business,
        paris_arrondissement,
        country,
        latitude,
        longitude,
        phone,
        email,
        website,
        opening_hours,
        cuisine,
        rating,
        review_count,
        siret,
        business_status,
        ban_address,
        ban_city,
        ban_postal_code,
        ban_latitude,
        ban_longitude,
        ban_score,
        ban_source,
        sirene_name,
        sirene_address,
        sirene_city,
        sirene_postal_code,
        sirene_siret,
        sirene_business_status,
        sirene_match_score,
        inpi_rne_siren,
        inpi_rne_type_personne,
        inpi_rne_diffusion_commerciale,
        inpi_rne_diffusion_insee,
        inpi_rne_updated_at,
        inpi_rne_date_creation,
        inpi_rne_forme_juridique,
        inpi_rne_company_name,
        inpi_rne_main_siret,
        inpi_rne_main_code_ape,
        inpi_rne_main_status,
        inpi_rne_main_postal_code,
        inpi_rne_main_city,
        inpi_rne_activity_start_date,
        inpi_rne_nombre_representants_actifs,
        inpi_rne_nombre_etablissements_ouverts,
        inpi_rne_representative_name,
        inpi_rne_representative_role,
        inpi_rne_last_event_code,
        inpi_rne_last_event_label,
        inpi_rne_last_event_date_effet,
        inpi_rne_enriched_at,
        collected_at
    FROM establishments
    {limit_clause}
    """
    return pd.read_sql_query(sql, engine)


@st.cache_data(ttl=600, show_spinner=False)
def get_geo_values(geo_mode: str = "business", only_paris: bool = False) -> list[str]:
    engine = get_engine()
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions, params = _geo_where_sql(geo_mode, None, only_paris)

    sql = f"""
    SELECT DISTINCT {geo_expr} AS geo_name
    FROM establishments
    WHERE {' AND '.join(conditions)}
      AND {geo_expr} IS NOT NULL
      AND TRIM({geo_expr}) <> ''
    ORDER BY geo_name
    """
    df = pd.read_sql_query(text(sql), engine, params=params)
    return df["geo_name"].tolist()


@st.cache_data(ttl=600, show_spinner=False)
def get_categories() -> list[str]:
    engine = get_engine()
    sql = f"""
    SELECT DISTINCT {CATEGORY_EXPR} AS category
    FROM establishments
    WHERE {CATEGORY_EXPR} IS NOT NULL
      AND TRIM({CATEGORY_EXPR}) <> ''
    ORDER BY category
    """
    df = pd.read_sql_query(sql, engine)
    return df["category"].tolist()


@st.cache_data(ttl=600, show_spinner=False)
def get_names_for_search(limit: int = 5000) -> list[str]:
    engine = get_engine()
    sql = f"""
    SELECT name
    FROM establishments
    WHERE name IS NOT NULL
      AND TRIM(name) <> ''
    GROUP BY name
    ORDER BY COUNT(*) DESC, name
    LIMIT {int(limit)}
    """
    df = pd.read_sql_query(sql, engine)
    return df["name"].tolist()


@st.cache_data(ttl=600, show_spinner=False)
def get_dashboard_scope_dataset(
    geo_mode: str = "business",
    geo_value: str | None = None,
    category: str | None = None,
    only_paris: bool = False,
    limit: int | None = None,
) -> pd.DataFrame:
    engine = get_engine()
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions, params = _geo_where_sql(geo_mode, geo_value, only_paris)

    if category:
        conditions.append(f"{CATEGORY_EXPR} = :category")
        params["category"] = category

    limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""

    sql = f"""
    SELECT
        name,
        {CATEGORY_EXPR} AS category,
        {SUBCATEGORY_EXPR} AS subcategory,
        address,
        postal_code,
        city,
        city_canonical,
        city_business,
        paris_arrondissement,
        latitude,
        longitude,
        phone,
        website,
        email,
        siret,
        collected_at,
        inpi_rne_siren,
        inpi_rne_diffusion_commerciale,
        inpi_rne_diffusion_insee,
        inpi_rne_date_creation,
        inpi_rne_company_name,
        inpi_rne_forme_juridique,
        inpi_rne_representative_name,
        inpi_rne_main_siret,
        inpi_rne_main_city,
        inpi_rne_main_postal_code,
        inpi_rne_last_event_code,
        inpi_rne_last_event_label,
        inpi_rne_last_event_date_effet,
        {geo_expr} AS geo_label
    FROM establishments
    WHERE {' AND '.join(conditions)}
    {limit_clause}
    """
    return pd.read_sql_query(text(sql), engine, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def search_entities(
    geo_mode: str = "business",
    geo_value: str | None = None,
    category: str | None = None,
    has_phone: bool | None = None,
    has_website: bool | None = None,
    has_siret: bool | None = None,
    query_text: str | None = None,
    only_paris: bool = False,
    limit: int = 2000,
) -> pd.DataFrame:
    engine = get_engine()
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions, params = _geo_where_sql(geo_mode, geo_value, only_paris)

    if category:
        conditions.append(f"{CATEGORY_EXPR} = :category")
        params["category"] = category

    if has_phone is True:
        conditions.append("phone IS NOT NULL")
    elif has_phone is False:
        conditions.append("phone IS NULL")

    if has_website is True:
        conditions.append("website IS NOT NULL")
    elif has_website is False:
        conditions.append("website IS NULL")

    if has_siret is True:
        conditions.append("siret IS NOT NULL")
    elif has_siret is False:
        conditions.append("siret IS NULL")

    if query_text:
        conditions.append("LOWER(name) LIKE :query_text")
        params["query_text"] = f"%{query_text.lower()}%"

    sql = f"""
    SELECT
        source,
        source_id,
        name,
        {CATEGORY_EXPR} AS category,
        {SUBCATEGORY_EXPR} AS subcategory,
        address,
        postal_code,
        city,
        city_canonical,
        city_business,
        paris_arrondissement,
        country,
        latitude,
        longitude,
        phone,
        email,
        website,
        siret,
        collected_at,
        ban_address,
        ban_city,
        ban_postal_code,
        ban_score,
        sirene_name,
        sirene_address,
        sirene_city,
        sirene_postal_code,
        sirene_siret,
        sirene_match_score,
        inpi_rne_siren,
        inpi_rne_diffusion_commerciale,
        inpi_rne_diffusion_insee,
        inpi_rne_date_creation,
        inpi_rne_company_name,
        inpi_rne_forme_juridique,
        inpi_rne_representative_name,
        inpi_rne_representative_role,
        inpi_rne_main_siret,
        inpi_rne_main_city,
        inpi_rne_main_postal_code,
        inpi_rne_last_event_code,
        inpi_rne_last_event_label,
        inpi_rne_last_event_date_effet,
        {geo_expr} AS geo_label
    FROM establishments
    WHERE {' AND '.join(conditions)}
    ORDER BY geo_label NULLS LAST, name NULLS LAST
    LIMIT {int(limit)}
    """
    return pd.read_sql_query(text(sql), engine, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def get_global_kpis() -> pd.DataFrame:
    engine = get_engine()
    sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT {CITY_BUSINESS_EXPR}) AS total_cities,
        COUNT(DISTINCT {CATEGORY_EXPR}) AS total_categories,
        COUNT(phone) AS phone_filled,
        COUNT(website) AS website_filled,
        COUNT(email) AS email_filled,
        COUNT(inpi_rne_siren) AS inpi_filled,
        COUNT(inpi_rne_date_creation) AS inpi_creation_filled
    FROM establishments
    """
    return pd.read_sql_query(sql, engine)


@st.cache_data(ttl=600, show_spinner=False)
def get_top_geographies(
    geo_mode: str = "business",
    category: str | None = None,
    only_paris: bool = False,
    limit: int = 15,
) -> pd.DataFrame:
    engine = get_engine()
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions, params = _geo_where_sql(geo_mode, None, only_paris)

    if category:
        conditions.append(f"{CATEGORY_EXPR} = :category")
        params["category"] = category

    sql = f"""
    SELECT
        {geo_expr} AS geography,
        COUNT(*) AS total_rows,
        COUNT(phone) AS phone_filled,
        COUNT(website) AS website_filled,
        COUNT(inpi_rne_siren) AS inpi_filled,
        COUNT(inpi_rne_date_creation) AS age_filled
    FROM establishments
    WHERE {' AND '.join(conditions)}
    GROUP BY 1
    ORDER BY total_rows DESC
    LIMIT {int(limit)}
    """
    return pd.read_sql_query(text(sql), engine, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def get_top_categories(
    geo_mode: str = "business",
    geo_value: str | None = None,
    only_paris: bool = False,
    limit: int = 12,
) -> pd.DataFrame:
    engine = get_engine()
    conditions, params = _geo_where_sql(geo_mode, geo_value, only_paris)

    sql = f"""
    SELECT
        {CATEGORY_EXPR} AS category,
        COUNT(*) AS total_rows,
        COUNT(phone) AS phone_filled,
        COUNT(website) AS website_filled,
        COUNT(inpi_rne_date_creation) AS age_filled
    FROM establishments
    WHERE {' AND '.join(conditions)}
    GROUP BY 1
    ORDER BY total_rows DESC
    LIMIT {int(limit)}
    """
    return pd.read_sql_query(text(sql), engine, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def get_market_table(
    geo_mode: str = "business",
    min_rows: int = 100,
    only_paris: bool = False,
) -> pd.DataFrame:
    engine = get_engine()
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions, params = _geo_where_sql(geo_mode, None, only_paris)

    sql = f"""
    SELECT
        {geo_expr} AS geography,
        COUNT(*) AS total_rows,
        ROUND(100.0 * COUNT(phone) / COUNT(*), 2) AS pct_phone,
        ROUND(100.0 * COUNT(website) / COUNT(*), 2) AS pct_website,
        ROUND(100.0 * COUNT(email) / COUNT(*), 2) AS pct_email,
        ROUND(100.0 * COUNT(inpi_rne_date_creation) / COUNT(*), 2) AS pct_age_available,
        ROUND(100.0 * COUNT(*) FILTER (WHERE inpi_rne_diffusion_commerciale IS TRUE) / COUNT(*), 2) AS pct_diffusion_commerciale
    FROM establishments
    WHERE {' AND '.join(conditions)}
    GROUP BY 1
    HAVING COUNT(*) >= {int(min_rows)}
    ORDER BY total_rows DESC
    """
    return pd.read_sql_query(text(sql), engine, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def get_top_category_heatmap_base(
    geo_mode: str = "business",
    only_paris: bool = False,
    top_n_geographies: int = 10,
    top_n_categories: int = 8,
) -> pd.DataFrame:
    engine = get_engine()
    geo_expr = _geo_expr(geo_mode, only_paris=only_paris)
    conditions, params = _geo_where_sql(geo_mode, None, only_paris)

    sql = f"""
    WITH top_geographies AS (
        SELECT
            {geo_expr} AS geography,
            COUNT(*) AS total_rows
        FROM establishments
        WHERE {' AND '.join(conditions)}
        GROUP BY 1
        ORDER BY total_rows DESC
        LIMIT {int(top_n_geographies)}
    ),
    top_categories AS (
        SELECT
            {CATEGORY_EXPR} AS category,
            COUNT(*) AS total_rows
        FROM establishments
        WHERE {' AND '.join(conditions)}
        GROUP BY 1
        ORDER BY total_rows DESC
        LIMIT {int(top_n_categories)}
    )
    SELECT
        {geo_expr} AS geography,
        {CATEGORY_EXPR} AS category,
        COUNT(*) AS count_rows
    FROM establishments
    WHERE {' AND '.join(conditions)}
      AND {geo_expr} IN (SELECT geography FROM top_geographies)
      AND {CATEGORY_EXPR} IN (SELECT category FROM top_categories)
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return pd.read_sql_query(text(sql), engine, params=params)