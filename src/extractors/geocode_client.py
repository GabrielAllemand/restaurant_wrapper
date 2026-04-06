from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.config.settings import settings
from src.config.sources import SourceName
from src.transformers.normalize_address import build_full_address
from src.transformers.normalize_text import normalize_city, normalize_country, normalize_postal_code
from src.utils.http import HttpClientConfig, create_session, polite_sleep, request_json_any
from src.utils.logger import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class GeocodingQuery:
    address: str | None = None
    postal_code: str | None = None
    city: str | None = None
    country: str | None = None
    limit: int | None = None


def _build_session():
    return create_session(
        HttpClientConfig(
            timeout_seconds=settings.geocoding.timeout_seconds,
            max_retries=settings.geocoding.max_retries,
            backoff_factor=1.0,
        )
    )


def geocode_address(query: GeocodingQuery) -> dict[str, Any] | None:
    """
    Géocode une adresse via l'API IGN/Géoplateforme.
    Retourne le meilleur résultat normalisé ou None si aucun résultat.
    """
    full_address = build_full_address(
        address=query.address,
        postal_code=query.postal_code,
        city=query.city,
        country=query.country,
    )

    if not full_address:
        logger.debug("Skipping geocoding because no usable address was provided.")
        return None

    session = _build_session()

    params = {
        "q": full_address,
        "limit": query.limit or settings.geocoding.limit,
    }

    logger.debug("Geocoding address: %s", full_address)

    payload = request_json_any(
        session,
        method="GET",
        url=settings.geocoding.search_url,
        timeout_seconds=settings.geocoding.timeout_seconds,
        params=params,
        context=f"Geocoding address: {full_address}",
    )

    polite_sleep(0.1)

    if not isinstance(payload, dict):
        logger.warning("Unexpected geocoding payload type: %s", type(payload).__name__)
        return None

    features = payload.get("features", [])
    if not isinstance(features, list) or not features:
        return None

    best_feature = features[0]
    return normalize_geocoding_feature(best_feature, original_query=query)


def normalize_geocoding_feature(
    feature: dict[str, Any],
    *,
    original_query: GeocodingQuery | None = None,
) -> dict[str, Any] | None:
    """
    Normalise une feature GeoJSON de l'API de géocodage.
    """
    if not isinstance(feature, dict):
        return None

    properties = feature.get("properties", {})
    geometry = feature.get("geometry", {})

    if not isinstance(properties, dict) or not isinstance(geometry, dict):
        return None

    coordinates = geometry.get("coordinates", [])
    longitude = None
    latitude = None

    if isinstance(coordinates, list) and len(coordinates) >= 2:
        try:
            longitude = float(coordinates[0])
            latitude = float(coordinates[1])
        except (TypeError, ValueError):
            longitude = None
            latitude = None

    normalized = {
        "source": SourceName.GEOCODING.value,
        "geocoded_label": _clean_string(properties.get("label")),
        "geocoded_address": _clean_string(properties.get("name")) or _clean_string(properties.get("label")),
        "postal_code": normalize_postal_code(properties.get("postcode"))
        or normalize_postal_code(original_query.postal_code if original_query else None),
        "city": normalize_city(properties.get("city"))
        or normalize_city(original_query.city if original_query else None),
        "country": normalize_country(original_query.country if original_query else "France"),
        "latitude": latitude,
        "longitude": longitude,
        "score": _safe_float(properties.get("score")),
        "raw_payload": feature,
    }

    return normalized


def reverse_geocode(latitude: float, longitude: float) -> dict[str, Any] | None:
    """
    Reverse geocoding simple à partir de coordonnées.
    """
    session = _build_session()

    params = {
        "lat": latitude,
        "lon": longitude,
        "limit": settings.geocoding.limit,
    }

    payload = request_json_any(
        session,
        method="GET",
        url=settings.geocoding.reverse_url,
        timeout_seconds=settings.geocoding.timeout_seconds,
        params=params,
        context=f"Reverse geocoding lat={latitude}, lon={longitude}",
    )

    polite_sleep(0.1)

    if not isinstance(payload, dict):
        return None

    features = payload.get("features", [])
    if not isinstance(features, list) or not features:
        return None

    return normalize_geocoding_feature(features[0])


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None