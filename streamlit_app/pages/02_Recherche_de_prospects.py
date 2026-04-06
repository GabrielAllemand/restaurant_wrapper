from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from streamlit_app.utils.branding import (
    load_css,
    page_config,
    render_header,
    section_title,
)
from streamlit_app.utils.queries import get_categories, get_geo_values, search_entities


def safe_text(value: object, default: str = "—") -> str:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    return text if text else default


def format_review_count(value: object) -> str:
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
    except Exception:
        pass
    try:
        return f"{int(float(value)):,}".replace(",", " ")
    except Exception:
        return safe_text(value)


page_config("Recherche de prospects", "📞")
load_css()

render_header(
    "Recherche de prospects",
    "Filtrez une cible, consultez les établissements les plus exploitables et exportez une liste CRM directement activable.",
)

st.sidebar.markdown("## Paramètres de recherche")

geo_mode_label = st.sidebar.radio(
    "Niveau géographique",
    options=["Ville business", "Ville détaillée"],
    index=0,
)
geo_mode = "business" if geo_mode_label == "Ville business" else "detailed"

only_paris = st.sidebar.toggle("Limiter à Paris / arrondissements", value=False)

geo_values = ["All"] + get_geo_values(geo_mode=geo_mode, only_paris=only_paris)
categories = ["All"] + get_categories(only_paris=only_paris)

geo_value = st.sidebar.selectbox("Zone", options=geo_values, index=0)
category = st.sidebar.selectbox("Catégorie", options=categories, index=0)
phone_filter = st.sidebar.selectbox("Téléphone", options=["All", "Yes", "No"], index=0)
website_filter = st.sidebar.selectbox("Site web", options=["All", "Yes", "No"], index=0)
query_text = st.sidebar.text_input("Recherche nom")

results = search_entities(
    geo_mode=geo_mode,
    geo_value=None if geo_value == "All" else geo_value,
    category=None if category == "All" else category,
    has_phone=None if phone_filter == "All" else phone_filter == "Yes",
    has_website=None if website_filter == "All" else website_filter == "Yes",
    has_siret=None,
    query_text=query_text or None,
    only_paris=only_paris,
    limit=None,
)

section_title(
    "Vue synthétique des résultats",
    "Lecture rapide du volume de prospects et du niveau d’information disponible.",
)

top = st.columns([1, 1, 1])
top[0].metric("Prospects trouvés", f"{len(results):,}".replace(",", " "))
top[1].metric("Contactables", f"{int(results['phone'].notna().sum()):,}".replace(",", " "))
top[2].metric("Avec site web", f"{int(results['website'].notna().sum()):,}".replace(",", " "))

if only_paris and geo_mode == "detailed":
    st.info("La vue Paris détaillée s’appuie sur `paris_arrondissement`.")

section_title(
    "Fiches prospects",
    "Affichage prioritaire des établissements les plus complets et les plus directement exploitables.",
)

display_df = results.copy()
display_df["zone"] = display_df["geo_label"]

completeness_columns = [
    "phone",
    "website",
    "email",
    "siret",
    "inpi_rne_company_name",
    "inpi_rne_date_creation",
    "inpi_rne_representative_name",
    "rating",
    "review_count",
    "scrape_review_platform",
    "scrape_description",
]
existing_completeness_columns = [c for c in completeness_columns if c in display_df.columns]

display_df["completeness_score"] = 0
for col in existing_completeness_columns:
    display_df["completeness_score"] += display_df[col].notna().astype(int)

required_columns = [
    "phone",
    "website",
    "siret",
    "inpi_rne_company_name",
    "inpi_rne_date_creation",
    "rating",
    "review_count",
]
existing_required_columns = [c for c in required_columns if c in display_df.columns]

if existing_required_columns:
    complete_df = display_df.dropna(subset=existing_required_columns).copy()
else:
    complete_df = display_df.copy()

sort_cols = [c for c in ["completeness_score", "review_count", "rating", "name"] if c in complete_df.columns]
ascending = [False, False, False, True][: len(sort_cols)]

if sort_cols:
    complete_df = complete_df.sort_values(
        by=sort_cols,
        ascending=ascending,
        na_position="last",
    )

if complete_df.empty:
    st.warning("Aucun établissement ne possède actuellement toutes les informations demandées pour cette vue.")
else:
    for _, row in complete_df.iterrows():
        locality = (
            row.get("paris_arrondissement")
            or row.get("city_canonical")
            or row.get("city_business")
            or row.get("city")
            or ""
        )

        name_display = safe_text(row.get("name"), "Unnamed")
        category_display = safe_text(row.get("category"))
        subcategory_display = safe_text(row.get("subcategory"))
        address_display = safe_text(row.get("address"), "Adresse non renseignée")
        postal_code_display = safe_text(row.get("postal_code"), "")
        locality_display = safe_text(locality, "")
        phone_display = safe_text(row.get("phone"))
        website_display = safe_text(row.get("website"))
        email_display = safe_text(row.get("email"))
        siret_display = safe_text(row.get("siret"))
        zone_display = safe_text(row.get("zone"))
        legal_name_display = safe_text(row.get("inpi_rne_company_name"))
        creation_display = safe_text(row.get("inpi_rne_date_creation"))
        representative_display = safe_text(row.get("inpi_rne_representative_name"))
        platform_display = safe_text(row.get("scrape_review_platform"))
        description_display = safe_text(row.get("scrape_description"))

        rating_value = row.get("rating")
        if pd.notna(rating_value):
            try:
                rating_display = f"{float(rating_value):.1f}"
            except Exception:
                rating_display = safe_text(rating_value)
        else:
            rating_display = "—"

        review_display = format_review_count(row.get("review_count"))

        badges = []
        if pd.notna(row.get("phone")):
            badges.append("Téléphone")
        if pd.notna(row.get("website")):
            badges.append("Site web")
        if pd.notna(row.get("email")):
            badges.append("Email")
        if pd.notna(row.get("inpi_rne_company_name")):
            badges.append("INPI")

        with st.container(border=True):
            left, right = st.columns([1.3, 1], gap="large")

            with left:
                st.markdown(f"### {name_display}")
                st.caption(f"{category_display} · {subcategory_display}")
                st.write(f"{address_display}")
                st.write(f"{postal_code_display} {locality_display}".strip())

                if badges:
                    st.caption(" • ".join(badges))

                st.markdown("**Description**")
                st.write(description_display)

            with right:
                st.write(f"**Téléphone :** {phone_display}")
                st.write(f"**Site web :** {website_display}")
                st.write(f"**Email :** {email_display}")
                st.write(f"**SIRET :** {siret_display}")
                st.write(f"**Zone :** {zone_display}")
                st.write(f"**Nom juridique :** {legal_name_display}")
                st.write(f"**Date de création :** {creation_display}")
                st.write(f"**Représentant :** {representative_display}")
                st.write(f"**Note :** {rating_display}")
                st.write(f"**Nombre d'avis :** {review_display}")
                st.write(f"**Plateforme :** {platform_display}")

section_title(
    "Export CRM",
    "Téléchargez une liste exploitable immédiatement pour prospection commerciale.",
)

export_cols = [
    "name",
    "category",
    "subcategory",
    "address",
    "postal_code",
    "city",
    "city_canonical",
    "city_business",
    "paris_arrondissement",
    "phone",
    "website",
    "email",
    "siret",
    "zone",
    "rating",
    "review_count",
    "scrape_review_platform",
    "scrape_description",
    "scrape_tags",
    "inpi_rne_company_name",
    "inpi_rne_date_creation",
    "inpi_rne_representative_name",
]
export_df = display_df[[c for c in export_cols if c in display_df.columns]].copy()

csv_buffer = io.StringIO()
export_df.to_csv(csv_buffer, index=False)

st.download_button(
    "Télécharger CSV CRM",
    data=csv_buffer.getvalue(),
    file_name="prospects_export.csv",
    mime="text/csv",
)

st.dataframe(export_df, use_container_width=True)