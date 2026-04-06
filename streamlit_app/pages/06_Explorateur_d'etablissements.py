from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app.utils.branding import (
    load_css,
    page_config,
    render_header,
    section_title,
    status_badge,
)
from streamlit_app.utils.charts import map_scatter
from streamlit_app.utils.queries import get_categories, get_geo_values, search_entities


def enrich_business_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["inpi_rne_date_creation"] = pd.to_datetime(out["inpi_rne_date_creation"], errors="coerce")
    today = pd.Timestamp.today().normalize()
    out["business_age_years"] = ((today - out["inpi_rne_date_creation"]).dt.days / 365.25).round(1)
    return out


page_config("Explorateur d'établissements", "🔎")
load_css()

render_header(
    "Explorateur d'établissements",
    "Recherche fine d’un établissement avec lecture terrain, enrichissements BAN/SIRENE, INPI et données complémentaires issues du scraping.",
)

st.sidebar.markdown("## Recherche avancée")

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
has_siret = st.sidebar.selectbox("SIRET", options=["All", "Yes", "No"], index=0)
has_phone = st.sidebar.selectbox("Téléphone", options=["All", "Yes", "No"], index=0)
has_website = st.sidebar.selectbox("Site web", options=["All", "Yes", "No"], index=0)
query_text = st.sidebar.text_input("Recherche nom / enseigne")

results = search_entities(
    geo_mode=geo_mode,
    geo_value=None if geo_value == "All" else geo_value,
    category=None if category == "All" else category,
    has_phone=None if has_phone == "All" else has_phone == "Yes",
    has_website=None if has_website == "All" else has_website == "Yes",
    has_siret=None if has_siret == "All" else has_siret == "Yes",
    query_text=query_text or None,
    only_paris=only_paris,
    limit=None,
)
results = enrich_business_metrics(results)

top = st.columns(5)
top[0].metric("Résultats", f"{len(results):,}".replace(",", " "))
top[1].metric("Avec SIRET", f"{int(results['siret'].notna().sum()):,}".replace(",", " "))
top[2].metric("Avec téléphone", f"{int(results['phone'].notna().sum()):,}".replace(",", " "))
top[3].metric("Avec site web", f"{int(results['website'].notna().sum()):,}".replace(",", " "))
top[4].metric("Avec INPI", f"{int(results['inpi_rne_siren'].notna().sum()):,}".replace(",", " "))

section_title(
    "Carte des entités",
    "Vue spatiale des établissements correspondant à la recherche.",
)
st.plotly_chart(
    map_scatter(results, color_col="category", title="Entity Explorer map", height=640),
    use_container_width=True,
)

section_title(
    "Résultats de recherche",
    "Table de lecture rapide pour repérer l’établissement recherché.",
)

table_cols = [
    "name",
    "category",
    "subcategory",
    "geo_label",
    "address",
    "postal_code",
    "phone",
    "website",
    "rating",
    "review_count",
    "scrape_review_platform",
    "siret",
    "inpi_rne_company_name",
    "business_age_years",
]
table_cols = [c for c in table_cols if c in results.columns]
st.dataframe(results[table_cols], use_container_width=True)

if not results.empty:
    entity_labels = (
        results["name"].fillna("Unnamed")
        + " | "
        + results["geo_label"].fillna("Unknown zone")
    ).tolist()

    selected_label = st.selectbox("Sélectionnez un établissement", options=entity_labels)
    selected = results.iloc[entity_labels.index(selected_label)]

    section_title(
        "Fiche établissement",
        "Lecture consolidée des champs finaux, enrichissements et données légales INPI.",
    )

    phone_badge = (
        status_badge("Téléphone disponible", "green")
        if selected.get("phone")
        else status_badge("Pas de téléphone", "gray")
    )
    web_badge = (
        status_badge("Site web disponible", "orange")
        if selected.get("website")
        else status_badge("Pas de site", "gray")
    )
    siret_badge = (
        status_badge("SIRET disponible", "green")
        if selected.get("siret")
        else status_badge("SIRET absent", "gray")
    )
    inpi_badge = (
        status_badge("Enrichi INPI", "purple")
        if selected.get("inpi_rne_siren")
        else status_badge("Pas d'INPI", "gray")
    )
    diffusion_badge = (
        status_badge("Diffusion commerciale OK", "green")
        if selected.get("inpi_rne_diffusion_commerciale") is True
        else status_badge("Diffusion commerciale limitée", "orange")
    )

    st.markdown(
        f"""
        <div class="section-card">
            <div style="display:flex; justify-content:space-between; gap:18px; align-items:flex-start;">
                <div style="flex:1;">
                    <div style="font-size:1.35rem; font-weight:800; color:#0f172a;">
                        {selected.get('name', 'Unnamed')}
                    </div>
                    <div class="small-muted">
                        {selected.get('category', 'Non renseigné')} · {selected.get('subcategory', 'Non renseigné')}
                    </div>
                    <div style="margin-top:10px; color:#334155;">
                        {selected.get('address', 'Adresse non renseignée')}<br>
                        {selected.get('postal_code', '')} {selected.get('paris_arrondissement') or selected.get('city_canonical') or selected.get('city_business') or selected.get('city') or ''}
                    </div>
                    <div style="margin-top:12px;">
                        {phone_badge} {web_badge} {siret_badge} {inpi_badge} {diffusion_badge}
                    </div>
                </div>
                <div style="min-width:360px;">
                    <div><strong>Nom juridique INPI :</strong> {selected.get('inpi_rne_company_name', '—')}</div>
                    <div><strong>Date création :</strong> {selected.get('inpi_rne_date_creation', '—')}</div>
                    <div><strong>Âge du commerce :</strong> {selected.get('business_age_years', '—')} an(s)</div>
                    <div><strong>Représentant :</strong> {selected.get('inpi_rne_representative_name', '—')}</div>
                    <div><strong>Forme juridique :</strong> {selected.get('inpi_rne_forme_juridique', '—')}</div>
                    <div><strong>SIREN INPI :</strong> {selected.get('inpi_rne_siren', '—')}</div>
                    <div><strong>Note :</strong> {selected.get('rating', '—')}</div>
                    <div><strong>Nombre d'avis :</strong> {selected.get('review_count', '—')}</div>
                    <div><strong>Plateforme :</strong> {selected.get('scrape_review_platform', '—')}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    left, right = st.columns(2)

    with left:
        st.markdown("### Champs finaux")
        st.json(
            {
                "name": selected.get("name"),
                "category": selected.get("category"),
                "subcategory": selected.get("subcategory"),
                "address": selected.get("address"),
                "postal_code": selected.get("postal_code"),
                "city_raw": selected.get("city"),
                "city_canonical": selected.get("city_canonical"),
                "city_business": selected.get("city_business"),
                "paris_arrondissement": selected.get("paris_arrondissement"),
                "phone": selected.get("phone"),
                "email": selected.get("email"),
                "website": selected.get("website"),
                "rating": selected.get("rating"),
                "review_count": selected.get("review_count"),
                "scrape_review_platform": selected.get("scrape_review_platform"),
                "scrape_description": selected.get("scrape_description"),
                "scrape_tags": selected.get("scrape_tags"),
                "siret": selected.get("siret"),
            },
            expanded=False,
        )

    with right:
        st.markdown("### Enrichissements & INPI")
        st.json(
            {
                "ban_address": selected.get("ban_address"),
                "ban_city": selected.get("ban_city"),
                "ban_postal_code": selected.get("ban_postal_code"),
                "ban_score": selected.get("ban_score"),
                "sirene_name": selected.get("sirene_name"),
                "sirene_address": selected.get("sirene_address"),
                "sirene_city": selected.get("sirene_city"),
                "sirene_postal_code": selected.get("sirene_postal_code"),
                "sirene_siret": selected.get("sirene_siret"),
                "sirene_match_score": selected.get("sirene_match_score"),
                "inpi_rne_siren": selected.get("inpi_rne_siren"),
                "inpi_rne_company_name": selected.get("inpi_rne_company_name"),
                "inpi_rne_date_creation": selected.get("inpi_rne_date_creation"),
                "business_age_years": selected.get("business_age_years"),
                "inpi_rne_representative_name": selected.get("inpi_rne_representative_name"),
                "inpi_rne_representative_role": selected.get("inpi_rne_representative_role"),
                "inpi_rne_forme_juridique": selected.get("inpi_rne_forme_juridique"),
                "inpi_rne_main_siret": selected.get("inpi_rne_main_siret"),
                "inpi_rne_main_city": selected.get("inpi_rne_main_city"),
                "inpi_rne_main_postal_code": selected.get("inpi_rne_main_postal_code"),
                "inpi_rne_diffusion_commerciale": selected.get("inpi_rne_diffusion_commerciale"),
                "inpi_rne_diffusion_insee": selected.get("inpi_rne_diffusion_insee"),
                "inpi_rne_last_event_code": selected.get("inpi_rne_last_event_code"),
                "inpi_rne_last_event_label": selected.get("inpi_rne_last_event_label"),
                "inpi_rne_last_event_date_effet": selected.get("inpi_rne_last_event_date_effet"),
            },
            expanded=False,
        )
else:
    st.info("Aucun établissement ne correspond aux filtres sélectionnés.")