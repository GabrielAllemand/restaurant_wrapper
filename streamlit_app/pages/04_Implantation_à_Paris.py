from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app.utils.branding import (
    load_css,
    page_config,
    render_header,
    render_kpi_card,
    section_title,
    status_badge,
)
from streamlit_app.utils.charts import bar_chart
from streamlit_app.utils.queries import get_categories, get_dashboard_scope_dataset


def enrich_business_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["inpi_rne_date_creation"] = pd.to_datetime(out["inpi_rne_date_creation"], errors="coerce")
    today = pd.Timestamp.today().normalize()
    out["business_age_years"] = ((today - out["inpi_rne_date_creation"]).dt.days / 365.25).round(1)
    out["is_recent_24m"] = out["inpi_rne_date_creation"] >= (today - pd.DateOffset(months=24))
    out["is_recent_12m"] = out["inpi_rne_date_creation"] >= (today - pd.DateOffset(months=12))
    return out


page_config("Implantation à Paris", "🗼")
load_css()

render_header(
    "Recommandation d'emplacement B2B — Paris",
    "Évaluez chaque arrondissement selon la densité concurrentielle, la contactabilité et le renouvellement du marché.",
)

categories = ["All"] + get_categories(only_paris=True)
selected_category = st.sidebar.selectbox("Catégorie cible", options=categories, index=0)

paris_df = get_dashboard_scope_dataset(
    geo_mode="detailed",
    geo_value=None,
    category=None if selected_category == "All" else selected_category,
    only_paris=True,
    limit=None,
)
paris_df = enrich_business_metrics(paris_df)
paris_df["arrondissement"] = paris_df["paris_arrondissement"]

if paris_df.empty:
    st.warning("Aucune donnée arrondissement disponible sur Paris pour ce filtre.")
    st.stop()

arr_stats = (
    paris_df.groupby("arrondissement", as_index=False)
    .agg(
        total_rows=("name", "size"),
        phone_rate=("phone", lambda s: round(100.0 * s.notna().mean(), 2)),
        website_rate=("website", lambda s: round(100.0 * s.notna().mean(), 2)),
        recent_24m_rate=("is_recent_24m", lambda s: round(100.0 * s.fillna(False).mean(), 2)),
        median_age=("business_age_years", "median"),
        diffusion_rate=(
            "inpi_rne_diffusion_commerciale",
            lambda s: round(100.0 * (s == True).mean(), 2),  # noqa: E712
        ),
    )
)

arr_stats["median_age"] = arr_stats["median_age"].round(1)

max_total = max(arr_stats["total_rows"].max(), 1)
arr_stats["competition_score"] = (arr_stats["total_rows"] / max_total * 100.0).round(2)

arr_stats["opportunity_score"] = (
    0.35 * (100.0 - arr_stats["competition_score"])
    + 0.20 * arr_stats["phone_rate"]
    + 0.15 * arr_stats["website_rate"]
    + 0.20 * arr_stats["recent_24m_rate"]
    + 0.10 * arr_stats["diffusion_rate"]
).round(1)

arr_stats = arr_stats.sort_values("opportunity_score", ascending=False).reset_index(drop=True)

best = arr_stats.iloc[0]
top3 = arr_stats.head(3)

row = st.columns(5)
with row[0]:
    render_kpi_card(
        "Prospects analysés",
        f"{len(paris_df):,}".replace(",", " "),
        "Volume Paris détaillé analysé",
        "kpi-blue",
    )
with row[1]:
    render_kpi_card(
        "Meilleur arrondissement",
        best["arrondissement"],
        "Meilleur score d'opportunité",
        "kpi-green",
    )
with row[2]:
    render_kpi_card(
        "Score d'opportunité",
        f"{best['opportunity_score']}/100",
        "Score enrichi par la récence",
        "kpi-orange",
    )
with row[3]:
    render_kpi_card(
        "% récents sur la meilleure zone",
        f"{best['recent_24m_rate']}%",
        "Créés depuis moins de 24 mois",
        "kpi-purple",
    )
with row[4]:
    render_kpi_card(
        "Âge médian meilleur arr.",
        f"{best['median_age']} ans" if pd.notna(best["median_age"]) else "N/A",
        "Selon INPI",
        "kpi-blue",
    )

st.info(
    "Le score combine : concurrence relative plus faible, contactabilité, présence digitale, dynamisme récent et diffusion commerciale."
)

section_title(
    "Top recommandations",
    "Les trois arrondissements les plus intéressants selon le score d’opportunité enrichi.",
)

cards = st.columns(3)
medals = ["🥇", "🥈", "🥉"]
for idx, col in enumerate(cards):
    row_data = top3.iloc[idx]
    if row_data["opportunity_score"] >= 70:
        badge = status_badge("Fortement recommandé", "green")
    elif row_data["opportunity_score"] >= 55:
        badge = status_badge("Opportunité modérée", "orange")
    else:
        badge = status_badge("Saturé", "gray")

    with col:
        st.markdown(
            f"""
            <div class="section-card">
                <div style="font-size:1.4rem; font-weight:800;">{medals[idx]} {row_data['arrondissement']}</div>
                <div style="margin-top:12px; font-size:2rem; font-weight:800; color:#0f172a;">{row_data['opportunity_score']}/100</div>
                <div class="small-muted">Score d'opportunité</div>
                <div style="margin-top:12px;"><strong>Concurrents :</strong> {int(row_data['total_rows'])}</div>
                <div><strong>Téléphone :</strong> {row_data['phone_rate']}%</div>
                <div><strong>Site web :</strong> {row_data['website_rate']}%</div>
                <div><strong>% récents :</strong> {row_data['recent_24m_rate']}%</div>
                <div><strong>Âge médian :</strong> {row_data['median_age']} ans</div>
                <div><strong>% diffusion commerciale :</strong> {row_data['diffusion_rate']}%</div>
                <div style="margin-top:12px;">{badge}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

section_title(
    "Analyse par arrondissement",
    "Lecture croisée du score, du volume et du renouvellement récent.",
)

left, right = st.columns(2)

with left:
    st.plotly_chart(
        bar_chart(
            arr_stats,
            x="arrondissement",
            y="opportunity_score",
            title="Score d'opportunité par arrondissement",
            height=500,
        ),
        use_container_width=True,
    )

with right:
    st.plotly_chart(
        bar_chart(
            arr_stats.sort_values("recent_24m_rate", ascending=False),
            x="arrondissement",
            y="recent_24m_rate",
            title="% de commerces récents par arrondissement",
            height=500,
        ),
        use_container_width=True,
    )

section_title(
    "Analyse complète par arrondissement",
    "Base détaillée de lecture pour la décision.",
)
st.dataframe(
    arr_stats.rename(
        columns={
            "arrondissement": "Arrondissement",
            "total_rows": "Nb concurrents",
            "phone_rate": "Téléphone %",
            "website_rate": "Site web %",
            "recent_24m_rate": "% récents 24m",
            "median_age": "Âge médian",
            "diffusion_rate": "% diffusion commerciale",
            "competition_score": "Score concurrence",
            "opportunity_score": "Score opportunité",
        }
    ),
    use_container_width=True,
)