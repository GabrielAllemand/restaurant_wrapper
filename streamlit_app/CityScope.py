from __future__ import annotations

import base64
from pathlib import Path

import streamlit as st

from streamlit_app.utils.branding import load_css, page_config


page_config("CityScope", "🏙️")
load_css()


def image_to_base64(path: str) -> str:
    return base64.b64encode(Path(path).read_bytes()).decode("utf-8")


logo_b64 = image_to_base64("streamlit_app/assets/cityscope_logo.png")

st.markdown(
    """
    <style>
    .cs-hero {
        background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 48%, #38bdf8 100%);
        border-radius: 32px;
        padding: 48px;
        margin-bottom: 32px;
        box-shadow: 0 20px 50px rgba(15, 23, 42, 0.18);
        position: relative;
        overflow: hidden;
    }

    .cs-hero::before {
        content: "";
        position: absolute;
        top: -60px;
        right: -60px;
        width: 220px;
        height: 220px;
        background: rgba(255,255,255,0.08);
        border-radius: 999px;
    }

    .cs-kicker {
        display: inline-block;
        background: rgba(255,255,255,0.14);
        color: white;
        border: 1px solid rgba(255,255,255,0.18);
        padding: 8px 14px;
        border-radius: 999px;
        font-size: 0.9rem;
        font-weight: 700;
        margin-bottom: 18px;
    }

    .cs-title {
        color: white;
        font-size: 3.4rem;
        line-height: 1.0;
        font-weight: 900;
        margin: 0 0 18px 0;
        letter-spacing: -0.03em;
    }

    .cs-subtitle {
        color: rgba(255,255,255,0.93);
        font-size: 1.15rem;
        line-height: 1.7;
        margin: 0;
        max-width: 760px;
    }

    .cs-logo-card {
        background: rgba(255,255,255,0.14);
        border: 1px solid rgba(255,255,255,0.18);
        border-radius: 28px;
        padding: 16px;
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.15);
        max-width: 320px;
        margin-left: auto;
        margin-right: auto;
    }

    .cs-logo-card img {
        width: 100%;
        display: block;
        border-radius: 20px;
        background: white;
    }

    .cs-stat {
        background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
        border: 1px solid #e2e8f0;
        border-radius: 22px;
        padding: 16px 18px;
        color: #0f172a;
        text-align: center;
        height: 100%;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
    }

    .cs-stat-value {
        font-size: 1.5rem;
        font-weight: 800;
        margin-bottom: 4px;
        color: #0f172a;
    }

    .cs-stat-label {
        font-size: 0.95rem;
        color: #64748b;
        line-height: 1.4;
    }

    .cs-section-title {
        font-size: 2rem;
        font-weight: 800;
        color: #0f172a;
        margin: 10px 0 6px 0;
        letter-spacing: -0.02em;
    }

    .cs-section-subtitle {
        color: #64748b;
        font-size: 1.05rem;
        margin-bottom: 20px;
    }

    .cs-card {
        background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
        border: 1px solid #e2e8f0;
        border-radius: 24px;
        padding: 24px;
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.06);
        height: 100%;
    }

    .cs-card-icon {
        font-size: 1.7rem;
        margin-bottom: 10px;
    }

    .cs-card-title {
        font-size: 1.16rem;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 10px;
    }

    .cs-card-text {
        color: #475569;
        line-height: 1.7;
        font-size: 1rem;
    }

    .cs-big-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fbff 100%);
        border: 1px solid #dbe7f3;
        border-radius: 28px;
        padding: 28px;
        box-shadow: 0 14px 34px rgba(15, 23, 42, 0.06);
        height: 100%;
    }

    .cs-big-title {
        font-size: 1.45rem;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 12px;
    }

    .cs-list {
        color: #475569;
        line-height: 1.9;
        padding-left: 18px;
        margin: 0;
    }

    .cs-module-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 22px;
        padding: 22px;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
        height: 100%;
    }

    .cs-module-title {
        font-size: 1.06rem;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 8px;
    }

    .cs-module-text {
        color: #475569;
        line-height: 1.65;
        font-size: 0.98rem;
    }

    .cs-footer-note {
        text-align: center;
        color: #64748b;
        font-size: 0.98rem;
        margin-top: 12px;
        margin-bottom: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="cs-hero">
        <div class="cs-kicker">Plateforme d’analyse territoriale &amp; prospection B2B</div>
        <div class="cs-title">CityScope</div>
        <div class="cs-subtitle">
            Une application conçue pour transformer des données locales, géographiques, légales et
            commerciales en décisions concrètes : comprendre un marché, cibler des prospects,
            prioriser les zones les plus prometteuses et enrichir la lecture terrain.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

hero_left, hero_right = st.columns([1.35, 0.65], gap="large")

with hero_left:
    stats_cols = st.columns(3)
    with stats_cols[0]:
        st.markdown(
            """
            <div class="cs-stat">
                <div class="cs-stat-value">6 modules</div>
                <div class="cs-stat-label">Analyse, ciblage et exploration</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with stats_cols[1]:
        st.markdown(
            """
            <div class="cs-stat">
                <div class="cs-stat-value">B2B</div>
                <div class="cs-stat-label">Prospection et aide à la décision</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with stats_cols[2]:
        st.markdown(
            """
            <div class="cs-stat">
                <div class="cs-stat-value">Paris + France</div>
                <div class="cs-stat-label">Vision locale et nationale</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

with hero_right:
    st.markdown(
        f"""
        <div class="cs-logo-card">
            <img src="data:image/png;base64,{logo_b64}" alt="CityScope logo">
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown('<div class="cs-section-title">Pourquoi CityScope ?</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="cs-section-subtitle">CityScope ne se contente pas d’afficher des établissements : l’application structure la donnée pour la rendre lisible, exploitable et directement actionnable dans un contexte business.</div>',
    unsafe_allow_html=True,
)

cols = st.columns(3, gap="large")
cards = [
    (
        "🧭",
        "Comprendre un marché local",
        "Lire rapidement la densité commerciale, les segments dominants, la couverture digitale, l’ancienneté des commerces et la dynamique récente d’une zone.",
    ),
    (
        "🎯",
        "Cibler des prospects activables",
        "Identifier des établissements plus facilement contactables, mieux documentés et plus pertinents pour des actions commerciales ou de développement réseau.",
    ),
    (
        "📈",
        "Décider avec plus de contexte",
        "Appuyer les décisions d’implantation, de priorisation ou de segmentation avec une lecture croisée entre volume, récence, contactabilité et structure marché.",
    ),
]

for col, (icon, title, text) in zip(cols, cards):
    with col:
        st.markdown(
            f"""
            <div class="cs-card">
                <div class="cs-card-icon">{icon}</div>
                <div class="cs-card-title">{title}</div>
                <div class="cs-card-text">{text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

big_left, big_right = st.columns(2, gap="large")

with big_left:
    st.markdown(
        """
        <div class="cs-big-card">
            <div class="cs-big-title">Pourquoi une approche B2B ?</div>
            <div class="cs-card-text" style="margin-bottom:10px;">
                CityScope est pensé pour des usages business concrets. Les équipes commerciales,
                expansion, réseau, business development ou analyse marché ont besoin d’un outil capable de :
            </div>
            <ul class="cs-list">
                <li>repérer rapidement les zones à fort potentiel ;</li>
                <li>prioriser les établissements les plus actionnables ;</li>
                <li>comparer plusieurs territoires sur des bases homogènes ;</li>
                <li>mieux préparer la prospection et enrichir les bases CRM ;</li>
                <li>rendre plus lisibles les dynamiques locales de marché.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with big_right:
    st.markdown(
        """
        <div class="cs-big-card">
            <div class="cs-big-title">Valeur ajoutée de la donnée enrichie</div>
            <div class="cs-card-text" style="margin-bottom:10px;">
                L’application consolide plusieurs niveaux d’information pour offrir une lecture plus riche :
            </div>
            <ul class="cs-list">
                <li>géolocalisation et structuration territoriale ;</li>
                <li>données légales et informations INPI ;</li>
                <li>ancienneté et signaux de récence ;</li>
                <li>coordonnées utiles à la prospection ;</li>
                <li>données terrain complémentaires issues du scraping.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown('<div class="cs-section-title">Les modules de la plateforme</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="cs-section-subtitle">Chaque page répond à un besoin métier précis, de la lecture d’ensemble jusqu’à la fiche établissement.</div>',
    unsafe_allow_html=True,
)

module_cols_top = st.columns(2, gap="large")
module_cols_mid = st.columns(2, gap="large")
module_cols_bot = st.columns(2, gap="large")

modules = [
    (
        "1. Tableau de bord commercial",
        "Vue synthétique du périmètre étudié : volumes, segments dominants, carte des prospects et premiers indicateurs de maturité marché.",
    ),
    (
        "2. Recherche de prospects",
        "Filtrage opérationnel par zone, catégorie et niveau d’information, avec visualisation cartographique et export exploitable.",
    ),
    (
        "3. Analyse de marché",
        "Lecture approfondie des dynamiques locales : densité, présence digitale, ancienneté des commerces et renouvellement récent.",
    ),
    (
        "4. Implantation à Paris",
        "Comparaison des arrondissements par volume, concurrence, récence et potentiel d’opportunité.",
    ),
    (
        "5. Priorisation commerciale",
        "Hiérarchisation des zones à traiter en priorité selon leur capacité à générer des quick wins business.",
    ),
    (
        "6. Explorateur d’établissements",
        "Fiche détaillée d’un établissement avec vue consolidée des informations finales, légales et enrichies.",
    ),
]

for col, (title, text) in zip(module_cols_top + module_cols_mid + module_cols_bot, modules):
    with col:
        st.markdown(
            f"""
            <div class="cs-module-card">
                <div class="cs-module-title">{title}</div>
                <div class="cs-module-text">{text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown(
    '<div class="cs-footer-note">Utilisez le menu de navigation à gauche pour explorer les différents modules de CityScope.</div>',
    unsafe_allow_html=True,
)