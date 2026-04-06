from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app.utils.branding import load_css, page_config, render_header, section_title
from streamlit_app.utils.charts import bar_chart, heatmap, scatter_priority
from streamlit_app.utils.queries import (
    get_dashboard_scope_dataset,
    get_top_category_heatmap_base,
)


def enrich_business_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["inpi_rne_date_creation"] = pd.to_datetime(out["inpi_rne_date_creation"], errors="coerce")
    today = pd.Timestamp.today().normalize()
    out["business_age_years"] = ((today - out["inpi_rne_date_creation"]).dt.days / 365.25).round(1)
    out["is_recent_24m"] = out["inpi_rne_date_creation"] >= (today - pd.DateOffset(months=24))
    out["is_recent_12m"] = out["inpi_rne_date_creation"] >= (today - pd.DateOffset(months=12))
    out["is_mature_5y"] = out["business_age_years"] >= 5
    return out


page_config("Analyse de marché", "📈")
load_css()

render_header(
    "Analyse de marché",
    "Comprendre la structure d’un marché, son renouvellement et son degré de maturité.",
)

st.sidebar.markdown("## Analyse")
geo_mode_label = st.sidebar.radio(
    "Granularité",
    options=["Ville business", "Ville détaillée"],
    index=0,
)
geo_mode = "business" if geo_mode_label == "Ville business" else "detailed"

scope_mode = st.sidebar.radio(
    "Périmètre",
    options=["France entière", "Paris uniquement", "France hors Paris"],
    index=0,
)

only_paris = scope_mode == "Paris uniquement"

df = get_dashboard_scope_dataset(
    geo_mode=geo_mode,
    geo_value=None,
    category=None,
    only_paris=only_paris,
    limit=None,
)

if scope_mode == "France hors Paris":
    df = df[df["city_business"].fillna("") != "Paris"].copy()

df = enrich_business_metrics(df)
df["geo_analysis"] = df["geo_label"]

combined = (
    df["category"].fillna("").str.lower()
    + " "
    + df["subcategory"].fillna("").str.lower()
)
df["is_restaurant_like"] = combined.str.contains(r"restaurant|fast_food|fast food|cafe|café|bar|pub|brasserie|bistro", regex=True)

section_title(
    "Densité de restaurants par zone",
    "Où la restauration est-elle la plus dense ?",
)
resto_density = (
    df[df["is_restaurant_like"]]
    .groupby("geo_analysis", as_index=False)
    .size()
    .rename(columns={"size": "restaurant_rows"})
    .sort_values("restaurant_rows", ascending=False)
    .head(20)
)
st.plotly_chart(
    bar_chart(resto_density, x="geo_analysis", y="restaurant_rows", title="Top zones en volume de restauration"),
    use_container_width=True,
)

section_title(
    "Catégories dominantes",
    "Lecture des catégories dominantes sur les zones les plus volumineuses.",
)
heatmap_df = get_top_category_heatmap_base(
    geo_mode=geo_mode,
    only_paris=only_paris,
    top_n_geographies=10,
    top_n_categories=8,
)

if scope_mode == "France hors Paris" and not heatmap_df.empty:
    heatmap_df = heatmap_df[~heatmap_df["geography"].fillna("").astype(str).str.contains("Paris", case=False, na=False)].copy()

st.plotly_chart(
    heatmap(heatmap_df, x="geography", y="category", z="count_rows", title="Heatmap catégories × zones"),
    use_container_width=True,
)

section_title(
    "Taux d’équipement digital",
    "Présence site web et téléphone comme proxy de maturité commerciale.",
)
digital = (
    df.groupby("geo_analysis", as_index=False)
    .agg(
        total_rows=("name", "size"),
        pct_phone=("phone", lambda s: round(100.0 * s.notna().mean(), 2)),
        pct_website=("website", lambda s: round(100.0 * s.notna().mean(), 2)),
    )
)
digital = digital[digital["total_rows"] >= 50].copy()
digital["priority_score"] = 0.6 * digital["pct_phone"] + 0.4 * digital["pct_website"]

st.plotly_chart(
    scatter_priority(
        digital,
        x="total_rows",
        y="pct_website",
        size="pct_phone",
        color="priority_score",
        hover_name="geo_analysis",
        title="Volume de marché vs présence digitale",
    ),
    use_container_width=True,
)

section_title(
    "Dynamique récente du marché",
    "Où les créations récentes sont-elles les plus concentrées ?",
)
recent_market = (
    df.groupby("geo_analysis", as_index=False)
    .agg(
        total_rows=("name", "size"),
        recent_24m=("is_recent_24m", "sum"),
        recent_12m=("is_recent_12m", "sum"),
        median_age=("business_age_years", "median"),
        pct_diffusion_commerciale=("inpi_rne_diffusion_commerciale", lambda s: round(100.0 * (s == True).mean(), 2)),  # noqa: E712
    )
)
recent_market = recent_market[recent_market["total_rows"] >= 50].copy()

if not recent_market.empty:
    recent_market["pct_recent_24m"] = (100.0 * recent_market["recent_24m"] / recent_market["total_rows"]).round(2)
    recent_market["pct_recent_12m"] = (100.0 * recent_market["recent_12m"] / recent_market["total_rows"]).round(2)
    recent_market["median_age"] = recent_market["median_age"].round(1)

    left, right = st.columns(2)
    with left:
        top_recent = recent_market.sort_values(["pct_recent_24m", "total_rows"], ascending=[False, False]).head(20)
        st.plotly_chart(
            bar_chart(
                top_recent,
                x="geo_analysis",
                y="pct_recent_24m",
                title="% de commerces récents (< 24 mois)",
                height=450,
            ),
            use_container_width=True,
        )
    with right:
        st.plotly_chart(
            scatter_priority(
                recent_market,
                x="total_rows",
                y="pct_recent_24m",
                size="pct_diffusion_commerciale",
                color="median_age",
                hover_name="geo_analysis",
                title="Volume vs récence vs âge médian",
            ),
            use_container_width=True,
        )

    section_title(
        "Tableau de lecture marché",
        "Synthèse zone par zone : volume, récence et ancienneté.",
    )
    st.dataframe(
        recent_market.sort_values(["pct_recent_24m", "total_rows"], ascending=[False, False]).rename(
            columns={
                "geo_analysis": "Zone",
                "total_rows": "Total",
                "recent_24m": "Nb récents 24m",
                "recent_12m": "Nb récents 12m",
                "pct_recent_24m": "% récents 24m",
                "pct_recent_12m": "% récents 12m",
                "median_age": "Âge médian",
                "pct_diffusion_commerciale": "% diffusion commerciale",
            }
        ),
        use_container_width=True,
    )