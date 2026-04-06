from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

from src.utils.logger import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class GeoplateformeConfig:
    base_url: str = "https://data.geopf.fr/geocodage"
    timeout_seconds: int = 20


def _clean_text(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    if text.endswith(".0"):
        text = text[:-2]

    return text


def build_address_query(
    *,
    address: str | None,
    city: str | None,
    postal_code: str | None,
) -> str | None:
    parts: list[str] = []

    clean_address = _clean_text(address)
    clean_city = _clean_text(city)
    clean_postal_code = _clean_text(postal_code)

    if clean_address:
        parts.append(clean_address)
    if clean_postal_code:
        parts.append(clean_postal_code)
    if clean_city:
        parts.append(clean_city)

    if not parts:
        return None

    return " ".join(parts)


def search_address(
    *,
    query: str,
    config: GeoplateformeConfig,
    limit: int = 5,
    max_retries: int = 3,
    retry_delay_seconds: float = 3.0,
) -> list[dict[str, Any]]:
    url = f"{config.base_url}/search"

    params = {
        "q": query,
        "limit": limit,
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=config.timeout_seconds,
                headers={"Accept": "application/json"},
            )

            if response.status_code in {502, 503, 504}:
                raise requests.exceptions.HTTPError(
                    f"Temporary server error: {response.status_code}",
                    response=response,
                )

            response.raise_for_status()

            payload = response.json()
            features = payload.get("features", [])

            if not isinstance(features, list):
                return []

            return features

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt == max_retries:
                logger.warning(
                    "Geoplateforme direct query failed after %d attempt(s) for query=%s | error=%s",
                    attempt,
                    query,
                    exc,
                )
                return []

            logger.warning(
                "Geoplateforme direct query failed on attempt %d/%d for query=%s | error=%s | retry in %.1fs",
                attempt,
                max_retries,
                query,
                exc,
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None

            if status_code in {502, 503, 504} and attempt < max_retries:
                logger.warning(
                    "Geoplateforme direct query returned HTTP %s on attempt %d/%d for query=%s | retry in %.1fs",
                    status_code,
                    attempt,
                    max_retries,
                    query,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue

            logger.warning(
                "Geoplateforme direct query failed for query=%s with HTTP %s",
                query,
                status_code,
            )
            return []


def reverse_search(
    *,
    latitude: float,
    longitude: float,
    config: GeoplateformeConfig,
    limit: int = 3,
    max_retries: int = 3,
    retry_delay_seconds: float = 3.0,
) -> list[dict[str, Any]]:
    url = f"{config.base_url}/reverse"

    params = {
        "lat": latitude,
        "lon": longitude,
        "limit": limit,
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=config.timeout_seconds,
                headers={"Accept": "application/json"},
            )

            if response.status_code in {502, 503, 504}:
                raise requests.exceptions.HTTPError(
                    f"Temporary server error: {response.status_code}",
                    response=response,
                )

            response.raise_for_status()

            payload = response.json()
            features = payload.get("features", [])

            if not isinstance(features, list):
                return []

            return features

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt == max_retries:
                logger.warning(
                    "Geoplateforme reverse query failed after %d attempt(s) for lat=%s lon=%s | error=%s",
                    attempt,
                    latitude,
                    longitude,
                    exc,
                )
                return []

            logger.warning(
                "Geoplateforme reverse query failed on attempt %d/%d for lat=%s lon=%s | error=%s | retry in %.1fs",
                attempt,
                max_retries,
                latitude,
                longitude,
                exc,
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None

            if status_code in {502, 503, 504} and attempt < max_retries:
                logger.warning(
                    "Geoplateforme reverse query returned HTTP %s on attempt %d/%d for lat=%s lon=%s | retry in %.1fs",
                    status_code,
                    attempt,
                    max_retries,
                    latitude,
                    longitude,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue

            logger.warning(
                "Geoplateforme reverse query failed for lat=%s lon=%s with HTTP %s",
                latitude,
                longitude,
                status_code,
            )
            return []


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).strip().lower().split())


def _similarity(a: str | None, b: str | None) -> float:
    from difflib import SequenceMatcher

    return SequenceMatcher(None, _normalize_text(a), _normalize_text(b)).ratio()


def score_feature(
    *,
    row_address: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    feature: dict[str, Any],
) -> float:
    properties = feature.get("properties", {}) or {}

    feature_label = properties.get("label")
    feature_city = properties.get("city")
    feature_postcode = properties.get("postcode")

    address_score = _similarity(row_address, feature_label)
    city_score = _similarity(row_city, feature_city)

    postal_score = 0.0
    clean_row_postal = _clean_text(row_postal_code)
    clean_feature_postal = _clean_text(feature_postcode)
    if clean_row_postal and clean_feature_postal:
        postal_score = 1.0 if clean_row_postal == clean_feature_postal else 0.0

    return 0.50 * address_score + 0.30 * city_score + 0.20 * postal_score


def select_best_feature(
    *,
    row_address: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    features: list[dict[str, Any]],
    min_score: float = 0.55,
) -> dict[str, Any] | None:
    best_feature: dict[str, Any] | None = None
    best_score = -1.0

    for feature in features:
        score = score_feature(
            row_address=row_address,
            row_city=row_city,
            row_postal_code=row_postal_code,
            feature=feature,
        )
        if score > best_score:
            best_score = score
            best_feature = feature

    if best_feature is None or best_score < min_score:
        return None

    return best_feature


def enrich_row_with_geoplateforme(
    *,
    row_address: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_latitude: float | None,
    row_longitude: float | None,
    config: GeoplateformeConfig,
) -> dict[str, Any] | None:
    query = build_address_query(
        address=row_address,
        city=row_city,
        postal_code=row_postal_code,
    )

    direct_features: list[dict[str, Any]] = []
    if query:
        logger.info("Geoplateforme direct query: %s", query)
        direct_features = search_address(
            query=query,
            config=config,
            limit=5,
        )

    if direct_features:
        best = select_best_feature(
            row_address=row_address,
            row_city=row_city,
            row_postal_code=row_postal_code,
            features=direct_features,
        )
        if best is not None:
            properties = best.get("properties", {}) or {}
            geometry = best.get("geometry", {}) or {}
            coordinates = geometry.get("coordinates", [None, None])

            return {
                "ban_address": properties.get("label"),
                "ban_city": properties.get("city"),
                "ban_postal_code": properties.get("postcode"),
                "ban_latitude": coordinates[1] if len(coordinates) > 1 else None,
                "ban_longitude": coordinates[0] if len(coordinates) > 1 else None,
                "ban_score": properties.get("score"),
                "ban_source": "direct",
            }

    if row_latitude is not None and row_longitude is not None:
        logger.info(
            "Geoplateforme reverse query: lat=%s lon=%s",
            row_latitude,
            row_longitude,
        )
        reverse_features = reverse_search(
            latitude=float(row_latitude),
            longitude=float(row_longitude),
            config=config,
            limit=3,
        )

        if reverse_features:
            feature = reverse_features[0]
            properties = feature.get("properties", {}) or {}
            geometry = feature.get("geometry", {}) or {}
            coordinates = geometry.get("coordinates", [None, None])

            return {
                "ban_address": properties.get("label"),
                "ban_city": properties.get("city"),
                "ban_postal_code": properties.get("postcode"),
                "ban_latitude": coordinates[1] if len(coordinates) > 1 else None,
                "ban_longitude": coordinates[0] if len(coordinates) > 1 else None,
                "ban_score": properties.get("score"),
                "ban_source": "reverse",
            }

    return None