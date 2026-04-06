from __future__ import annotations

import streamlit as st
from sqlalchemy.engine import Engine

from src.loaders.postgres_loader import create_postgres_engine


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    return create_postgres_engine()