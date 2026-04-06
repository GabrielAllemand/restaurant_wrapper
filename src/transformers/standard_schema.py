from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from src.utils.dates import utc_now_iso


STANDARD_COLUMNS: list[str] = [
    "source",
    "source_id",
    "name",
    "category",
    "subcategory",
    "address",
    "postal_code",
    "city",
    "country",
    "latitude",
    "longitude",
    "phone",
    "email",
    "website",
    "opening_hours",
    "cuisine",
    "rating",
    "review_count",
    "siret",
    "business_status",
    "raw_payload",
    "collected_at",
]


def build_empty_standard_dataframe() -> pd.DataFrame:
    """
    Retourne un DataFrame vide respectant le schéma standard.
    """
    return pd.DataFrame(columns=STANDARD_COLUMNS)


def _normalize_scalar(value: Any) -> Any:
    """
    Normalise les scalaires simples pour éviter les chaînes vides parasites.
    """
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None

    return value


def build_standard_record(
    *,
    source: str,
    source_id: str | None = None,
    name: str | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    address: str | None = None,
    postal_code: str | None = None,
    city: str | None = None,
    country: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    phone: str | None = None,
    email: str | None = None,
    website: str | None = None,
    opening_hours: str | None = None,
    cuisine: str | None = None,
    rating: float | None = None,
    review_count: int | None = None,
    siret: str | None = None,
    business_status: str | None = None,
    raw_payload: Mapping[str, Any] | Sequence[Any] | None = None,
    collected_at: str | None = None,
) -> dict[str, Any]:
    """
    Construit un enregistrement conforme au schéma standard.
    """
    record: dict[str, Any] = {
        "source": _normalize_scalar(source),
        "source_id": _normalize_scalar(source_id),
        "name": _normalize_scalar(name),
        "category": _normalize_scalar(category),
        "subcategory": _normalize_scalar(subcategory),
        "address": _normalize_scalar(address),
        "postal_code": _normalize_scalar(postal_code),
        "city": _normalize_scalar(city),
        "country": _normalize_scalar(country),
        "latitude": latitude,
        "longitude": longitude,
        "phone": _normalize_scalar(phone),
        "email": _normalize_scalar(email),
        "website": _normalize_scalar(website),
        "opening_hours": _normalize_scalar(opening_hours),
        "cuisine": _normalize_scalar(cuisine),
        "rating": rating,
        "review_count": review_count,
        "siret": _normalize_scalar(siret),
        "business_status": _normalize_scalar(business_status),
        "raw_payload": raw_payload,
        "collected_at": _normalize_scalar(collected_at) or utc_now_iso(),
    }
    return enforce_standard_record(record)


def enforce_standard_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """
    Garantit qu'un dictionnaire respecte exactement le schéma standard.
    Les colonnes manquantes sont ajoutées à None, les colonnes inconnues sont ignorées.
    """
    normalized: dict[str, Any] = {}
    for column in STANDARD_COLUMNS:
        value = record.get(column)
        normalized[column] = _normalize_scalar(value) if column not in {"raw_payload", "latitude", "longitude", "rating", "review_count"} else value
    return normalized


def standard_records_to_dataframe(records: list[Mapping[str, Any]]) -> pd.DataFrame:
    """
    Convertit une liste de records standardisés en DataFrame stable.
    """
    if not records:
        return build_empty_standard_dataframe()

    normalized_records = [enforce_standard_record(record) for record in records]
    dataframe = pd.DataFrame(normalized_records)

    for column in STANDARD_COLUMNS:
        if column not in dataframe.columns:
            dataframe[column] = None

    return dataframe.loc[:, STANDARD_COLUMNS]


def validate_required_standard_fields(record: Mapping[str, Any]) -> None:
    """
    Vérifie les champs minimaux attendus pour un record exploitable.
    """
    source = record.get("source")
    name = record.get("name")

    if not source:
        raise ValueError("Standard record is missing required field: 'source'")
    if not name:
        raise ValueError("Standard record is missing required field: 'name'")