from __future__ import annotations

from typing import Any

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def is_missing(value: Any) -> bool:
    """
    Détermine si une valeur doit être considérée comme manquante.
    """
    if value is None:
        return True
    try:
        return pd.isna(value)
    except TypeError:
        return False


def coalesce(*values: Any) -> Any:
    """
    Retourne la première valeur non manquante.
    """
    for value in values:
        if not is_missing(value):
            return value
    return None


def ensure_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Ajoute les colonnes manquantes à un DataFrame sans modifier l'original.
    """
    result = dataframe.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result


def safe_string(value: Any) -> str | None:
    """
    Convertit une valeur en chaîne nettoyée, ou None si vide/manquante.
    """
    if is_missing(value):
        return None

    text = str(value).strip()
    return text or None