from __future__ import annotations

import streamlit as st

from streamlit_app.utils.queries import get_distinct_categories, get_distinct_cities


def sidebar_filters() -> dict[str, object]:
    st.sidebar.markdown("## Filters")

    cities = get_distinct_cities()
    categories = get_distinct_categories()

    city = st.sidebar.selectbox("City", options=["All"] + cities, index=0)
    category = st.sidebar.selectbox("Category", options=["All"] + categories, index=0)

    has_siret = st.sidebar.selectbox("SIRET", options=["All", "Yes", "No"], index=0)
    has_phone = st.sidebar.selectbox("Phone", options=["All", "Yes", "No"], index=0)
    has_website = st.sidebar.selectbox("Website", options=["All", "Yes", "No"], index=0)

    return {
        "city": None if city == "All" else city,
        "category": None if category == "All" else category,
        "has_siret": None if has_siret == "All" else has_siret == "Yes",
        "has_phone": None if has_phone == "All" else has_phone == "Yes",
        "has_website": None if has_website == "All" else has_website == "Yes",
    }