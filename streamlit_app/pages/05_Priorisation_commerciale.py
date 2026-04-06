from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app.utils.branding import load_css, page_config, render_header, render_kpi_card, section_title
from streamlit_app.utils.charts import bar_chart, scatter_priority
from streamlit_app.utils.queries import get_dashboard_scope_dataset


page_config("Priorisation Commerciale", "🎯")
load_css()

render_header(
    "Priorisation Commerciale",
    "Identifier les quick wins et les zones à forte valeur commerciale à activer en priorité.",
)

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

df["geo_analysis"] = df["geo_label"]

priority_df = (
    df.groupby("geo_analysis", as_index=False)
    .agg(
        total_rows=("name", "size"),
        pct_phone=("phone", lambda s: round(100.0 * s.notna().mean(), 2)),
        pct_website=("website", lambda s: round(100.0 * s.notna().mean(), 2)),
    )
)
priority_df = priority_df[priority_df["total_rows"] >= 30].copy()

if priority_df.empty:
    st.warning("Aucune zone exploitable ne ressort avec les filtres actuels.")
    st.stop()

max_rows = max(priority_df["total_rows"].max(), 1)
priority_df["volume_score"] = priority_df["total_rows"] / max_rows * 100.0
priority_df["quick_win_score"] = (
    0.50 * priority_df["volume_score"]
    + 0.30 * priority_df["pct_phone"]
    + 0.20 * priority_df["pct_website"]
).round(2)

top_quick = priority_df.sort_values("quick_win_score", ascending=False).head(10)
best_zone = top_quick.iloc[0]

row = st.columns(3)
with row[0]:
    render_kpi_card("Zone la plus prioritaire", str(best_zone["geo_analysis"]), "Meilleur quick win commercial", "kpi-green")
with row[1]:
    render_kpi_card("Quick win score", f"{best_zone['quick_win_score']:.2f}", "Volume + contactabilité + présence digitale", "kpi-orange")
with row[2]:
    render_kpi_card("Zones analysées", f"{len(priority_df):,}".replace(",", " "), "Périmètres comparables actifs", "kpi-blue")

section_title(
    "Top quick wins",
    "Zones avec le meilleur compromis entre volume et exploitabilité commerciale.",
)
st.plotly_chart(
    bar_chart(top_quick, x="geo_analysis", y="quick_win_score", title="Top zones prioritaires", height=500),
    use_container_width=True,
)

section_title(
    "Matrice volume × contactabilité",
    "Identifier les poches de prospection les plus prometteuses.",
)
st.plotly_chart(
    scatter_priority(
        priority_df,
        x="total_rows",
        y="pct_phone",
        size="pct_website",
        color="quick_win_score",
        hover_name="geo_analysis",
        title="Volume de prospects vs taux de téléphone",
    ),
    use_container_width=True,
)

section_title(
    "Table de priorisation",
    "Liste exploitable pour séquencer les actions commerciales.",
)
st.dataframe(
    priority_df.sort_values(["quick_win_score", "total_rows"], ascending=[False, False]),
    use_container_width=True,
)