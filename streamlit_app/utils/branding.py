from __future__ import annotations

from pathlib import Path

import streamlit as st


def load_css() -> None:
    css_path = Path(__file__).resolve().parents[1] / "assets" / "styles.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def page_config(page_title: str, page_icon: str = "📊") -> None:
    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )


def render_header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="hero-box">
            <div class="hero-title">{title}</div>
            <div class="hero-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(title: str, caption: str | None = None) -> None:
    st.markdown(f"## {title}")
    if caption:
        st.markdown(f"<div class='small-muted'>{caption}</div>", unsafe_allow_html=True)


def render_kpi_card(label: str, value: str, help_text: str = "", variant: str = "kpi-blue") -> None:
    st.markdown(
        f"""
        <div class="kpi-card {variant}">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-help">{help_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def status_badge(label: str, status: str) -> str:
    mapping = {
        "green": "status-green",
        "orange": "status-orange",
        "gray": "status-gray",
    }
    css_class = mapping.get(status, "status-gray")
    return f"<span class='status-badge {css_class}'>{label}</span>"