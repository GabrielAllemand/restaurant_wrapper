from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app.utils.branding import load_css, page_config, render_header, render_kpi_card, section_title
from streamlit_app.utils.charts import bar_chart, donut_chart, map_scatter
from streamlit_app.utils.queries import (
    get_dashboard_scope_dataset,
    get_geo_values,
    get_top_categories,
    get_top_geographies,
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


page_config("Tableau de bord commercial", "💼")
load_css()

render_header(
    "Tableau de bord commercial",
    "Vue business immédiate sur les segments, la contactabilité, l’ancienneté et le renouvellement du marché.",
)

st.sidebar.markdown("## Périmètre")
scope_mode = st.sidebar.radio(
    "Zone d’analyse",
    options=["France", "Paris global", "Paris détaillé"],
    index=0,
)

category_options_df = get_top_categories(limit=200)

category_filter = st.sidebar.selectbox(
    "Filtre catégorie",
    options=["All"] + category_options_df["category"].tolist(),
    index=0,
)

if scope_mode == "France":
    geo_mode = "business"
    only_paris = False
    geo_values = ["All"] + get_geo_values(geo_mode="business", only_paris=False)
    selected_geo = st.sidebar.selectbox("Ville business", options=geo_values, index=0)
elif scope_mode == "Paris global":
    geo_mode = "business"
    only_paris = True
    selected_geo = "Paris"
else:
    geo_mode = "detailed"
    only_paris = True
    geo_values = ["All"] + get_geo_values(geo_mode="detailed", only_paris=True)
    selected_geo = st.sidebar.selectbox("Arrondissement", options=geo_values, index=0)

geo_value = None if selected_geo == "All" else selected_geo
category_value = None if category_filter == "All" else category_filter

df = get_dashboard_scope_dataset(
    geo_mode=geo_mode,
    geo_value=geo_value,
    category=category_value,
    only_paris=only_paris,
    limit=None,
)
df = enrich_business_metrics(df)

# Retirer les pharmacies du dashboard
df = df[df["category"].fillna("").str.lower() != "pharmacie"].copy()

combined = (
    df["category"].fillna("").str.lower()
    + " "
    + df["subcategory"].fillna("").str.lower()
)

restaurant_count = int(combined.str.contains(r"restaurant|fast_food|fast food|cafe|café|bar|pub|brasserie|bistro", regex=True).sum())
hair_count = int(combined.str.contains(r"hairdresser|coiff|barber|barbier", regex=True).sum())
bakery_count = int(combined.str.contains(r"bakery|boulanger|pastry|patisser|pâtisser|confectionery", regex=True).sum())

contactable_count = int(df["phone"].notna().sum())
digital_count = int(df["website"].notna().sum())
recent_24m_count = int(df["is_recent_24m"].fillna(False).sum())
total_count = int(len(df))

age_series = df["business_age_years"].dropna()
median_age = round(float(age_series.median()), 1) if not age_series.empty else None
pct_recent = round(100.0 * recent_24m_count / total_count, 1) if total_count else 0.0

section_title(
    "Carte des prospects",
    "Vue géographique immédiate du périmètre sélectionné.",
)
st.plotly_chart(
    map_scatter(df, color_col="category", title="Carte des prospects"),
    use_container_width=True,
)

section_title(
    "Volumes clés",
    "Combien de prospects, quels segments et quel niveau de maturité marché ?",
)

row1 = st.columns(3)
with row1[0]:
    render_kpi_card("Restaurants", f"{restaurant_count:,}".replace(",", " "), "Segment restauration", "kpi-blue")
with row1[1]:
    render_kpi_card("Salons de coiffure", f"{hair_count:,}".replace(",", " "), "Segment coiffure", "kpi-purple")
with row1[2]:
    render_kpi_card("Boulangeries", f"{bakery_count:,}".replace(",", " "), "Segment boulangerie", "kpi-orange")

row2 = st.columns(4)
with row2[0]:
    render_kpi_card("Prospects dans la zone", f"{total_count:,}".replace(",", " "), "Volume total analysé", "kpi-blue")
with row2[1]:
    render_kpi_card("Téléphone disponible", f"{contactable_count:,}".replace(",", " "), "Contactables immédiatement", "kpi-green")
with row2[2]:
    render_kpi_card("Site web disponible", f"{digital_count:,}".replace(",", " "), "Présence digitale", "kpi-orange")
with row2[3]:
    render_kpi_card("Commerces récents", f"{recent_24m_count:,}".replace(",", " "), "< 24 mois", "kpi-purple")

row3 = st.columns(2)
with row3[0]:
    render_kpi_card("Âge médian", f"{median_age} ans" if median_age is not None else "N/A", "Selon la date de création INPI", "kpi-blue")
with row3[1]:
    render_kpi_card("% récents", f"{pct_recent}%", "Créés depuis moins de 24 mois", "kpi-purple")

section_title(
    "Répartition des segments",
    "Quels types de prospects dominent dans la zone sélectionnée ?",
)
top_categories = (
    df.groupby("category", dropna=False)
    .size()
    .reset_index(name="total_rows")
    .sort_values("total_rows", ascending=False)
    .head(12)
)

left, right = st.columns([1.2, 1])
with left:
    st.plotly_chart(
        bar_chart(top_categories, x="category", y="total_rows", title="Top catégories dans la zone"),
        use_container_width=True,
    )
with right:
    age_bucket_df = pd.DataFrame(
        {
            "bucket": ["< 12 mois", "12-24 mois", "2-5 ans", "> 5 ans"],
            "count": [
                int(df["is_recent_12m"].fillna(False).sum()),
                int(((df["business_age_years"] >= 1) & (df["business_age_years"] < 2)).sum()),
                int(((df["business_age_years"] >= 2) & (df["business_age_years"] < 5)).sum()),
                int((df["business_age_years"] >= 5).sum()),
            ],
        }
    )
    st.plotly_chart(
        donut_chart(age_bucket_df, names="bucket", values="count", title="Répartition par ancienneté"),
        use_container_width=True,
    )

section_title(
    "Top zones",
    "Quelles zones concentrent le plus grand volume de prospects ?",
)
top_geos = get_top_geographies(
    geo_mode=geo_mode,
    category=category_value,
    only_paris=only_paris,
    limit=15,
)

# Optionnel : retirer pharmacie du top zones si jamais agrégations indirectes
st.plotly_chart(
    bar_chart(top_geos, x="geography", y="total_rows", title="Top zones par volume", height=450),
    use_container_width=True,
)

section_title(
    "Renouvellement du marché",
    "Quelles zones affichent la plus forte densité de commerces récents ?",
)
if not df.empty:
    zone_recent = (
        df.groupby("geo_label", dropna=False)
        .agg(
            total_rows=("name", "size"),
            recent_24m=("is_recent_24m", "sum"),
            median_age=("business_age_years", "median"),
        )
        .reset_index()
    )
    zone_recent = zone_recent[zone_recent["total_rows"] >= 20].copy()
    if not zone_recent.empty:
        zone_recent["pct_recent_24m"] = (100.0 * zone_recent["recent_24m"] / zone_recent["total_rows"]).round(2)
        zone_recent["median_age"] = zone_recent["median_age"].round(1)
        zone_recent = zone_recent.sort_values(["pct_recent_24m", "total_rows"], ascending=[False, False]).head(15)

        left2, right2 = st.columns(2)
        with left2:
            st.plotly_chart(
                bar_chart(
                    zone_recent,
                    x="geo_label",
                    y="pct_recent_24m",
                    title="% de commerces récents par zone",
                    height=450,
                ),
                use_container_width=True,
            )
        with right2:
            st.dataframe(
                zone_recent.rename(
                    columns={
                        "geo_label": "Zone",
                        "total_rows": "Total",
                        "recent_24m": "Nb récents",
                        "pct_recent_24m": "% récents",
                        "median_age": "Âge médian",
                    }
                ),
                use_container_width=True,
            )