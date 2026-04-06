from __future__ import annotations

from dataclasses import dataclass
from time import sleep
from typing import Any

from src.config.settings import settings
from src.config.sources import SourceName
from src.transformers.map_categories import extract_osm_subcategory, map_osm_category
from src.transformers.normalize_address import merge_address_parts, normalize_address_fields
from src.transformers.normalize_phone import normalize_phone
from src.transformers.normalize_text import (
    clean_text,
    normalize_city,
    normalize_country,
    normalize_website,
)
from src.transformers.standard_schema import build_standard_record
from src.utils.http import HttpClientConfig, HttpClientError, create_session, polite_sleep, request_json
from src.utils.logger import get_logger


logger = get_logger(__name__)


OVERPASS_RETRYABLE_STATUS_CODES = {429, 504}
OVERPASS_MAX_ATTEMPTS = 4
OVERPASS_BACKOFF_SECONDS = [15, 30, 60, 120]
OVERPASS_INTER_REQUEST_SLEEP_SECONDS = 3.0


@dataclass(frozen=True)
class OverpassQueryParams:
    city: str
    country: str = "France"
    timeout_seconds: int | None = None


@dataclass(frozen=True)
class OverpassDepartmentQueryParams:
    department_name: str
    country: str = "France"
    timeout_seconds: int | None = None


def build_overpass_query(params: OverpassQueryParams) -> str:
    timeout_clause = params.timeout_seconds or settings.overpass.default_timeout_clause_seconds

    city = params.city.replace('"', '\\"')
    country = params.country.replace('"', '\\"')

    return f"""
[out:json][timeout:{timeout_clause}];

area["name"="{country}"]["boundary"="administrative"]->.country;
area["name"="{city}"]["boundary"="administrative"](area.country)->.searchArea;

(
  node["amenity"~"^(restaurant|fast_food|cafe|bar|pub|ice_cream|food_court)$"](area.searchArea);
  way["amenity"~"^(restaurant|fast_food|cafe|bar|pub|ice_cream|food_court)$"](area.searchArea);
  relation["amenity"~"^(restaurant|fast_food|cafe|bar|pub|ice_cream|food_court)$"](area.searchArea);

  node["shop"~"^(bakery|pastry|confectionery|butcher|cheese|greengrocer|supermarket|convenience|mall|department_store|clothes|hairdresser|beauty|florist|pharmacy|wine|coffee)$"](area.searchArea);
  way["shop"~"^(bakery|pastry|confectionery|butcher|cheese|greengrocer|supermarket|convenience|mall|department_store|clothes|hairdresser|beauty|florist|pharmacy|wine|coffee)$"](area.searchArea);
  relation["shop"~"^(bakery|pastry|confectionery|butcher|cheese|greengrocer|supermarket|convenience|mall|department_store|clothes|hairdresser|beauty|florist|pharmacy|wine|coffee)$"](area.searchArea);
);

out center tags;
""".strip()


def build_overpass_department_query(params: OverpassDepartmentQueryParams) -> str:
    timeout_clause = params.timeout_seconds or settings.overpass.default_timeout_clause_seconds

    department_name = params.department_name.replace('"', '\\"')
    country = params.country.replace('"', '\\"')

    return f"""
[out:json][timeout:{timeout_clause}];

area["name"="{country}"]["boundary"="administrative"]->.country;
area["name"="{department_name}"]["boundary"="administrative"]["admin_level"="6"](area.country)->.searchArea;

(
  node["amenity"~"^(restaurant|fast_food|cafe|bar|pub|ice_cream|food_court)$"](area.searchArea);
  way["amenity"~"^(restaurant|fast_food|cafe|bar|pub|ice_cream|food_court)$"](area.searchArea);
  relation["amenity"~"^(restaurant|fast_food|cafe|bar|pub|ice_cream|food_court)$"](area.searchArea);

  node["shop"~"^(bakery|pastry|confectionery|butcher|cheese|greengrocer|supermarket|convenience|mall|department_store|clothes|hairdresser|beauty|florist|pharmacy|wine|coffee)$"](area.searchArea);
  way["shop"~"^(bakery|pastry|confectionery|butcher|cheese|greengrocer|supermarket|convenience|mall|department_store|clothes|hairdresser|beauty|florist|pharmacy|wine|coffee)$"](area.searchArea);
  relation["shop"~"^(bakery|pastry|confectionery|butcher|cheese|greengrocer|supermarket|convenience|mall|department_store|clothes|hairdresser|beauty|florist|pharmacy|wine|coffee)$"](area.searchArea);
);

out center tags;
""".strip()


def _build_session():
    return create_session(
        HttpClientConfig(
            timeout_seconds=settings.overpass.timeout_seconds,
            max_retries=settings.overpass.max_retries,
            backoff_factor=1.0,
        ),
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
    )


def _extract_status_code_from_exception(exc: Exception) -> int | None:
    text = str(exc)
    for status_code in OVERPASS_RETRYABLE_STATUS_CODES:
        if f"HTTP {status_code}" in text:
            return status_code
    return None


def _request_overpass_payload(*, query: str, context: str) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, OVERPASS_MAX_ATTEMPTS + 1):
        session = _build_session()

        try:
            payload = request_json(
                session,
                method="POST",
                url=settings.overpass.base_url,
                timeout_seconds=settings.overpass.timeout_seconds,
                data={"data": query},
                context=context,
            )

            polite_sleep(OVERPASS_INTER_REQUEST_SLEEP_SECONDS)
            return payload

        except HttpClientError as exc:
            last_error = exc
            status_code = _extract_status_code_from_exception(exc)

            if status_code not in OVERPASS_RETRYABLE_STATUS_CODES or attempt == OVERPASS_MAX_ATTEMPTS:
                raise

            wait_seconds = OVERPASS_BACKOFF_SECONDS[min(attempt - 1, len(OVERPASS_BACKOFF_SECONDS) - 1)]

            logger.warning(
                "%s failed with HTTP %s on attempt %d/%d. Retrying in %d second(s).",
                context,
                status_code,
                attempt,
                OVERPASS_MAX_ATTEMPTS,
                wait_seconds,
            )
            sleep(wait_seconds)

    if last_error is not None:
        raise last_error

    raise RuntimeError(f"{context} failed with unknown retry state.")


def fetch_overpass_data(params: OverpassQueryParams) -> dict[str, Any]:
    query = build_overpass_query(params)

    logger.info(
        "Fetching Overpass data for city=%s, country=%s",
        params.city,
        params.country,
    )

    return _request_overpass_payload(
        query=query,
        context=f"Overpass API call for city={params.city}",
    )


def fetch_overpass_department_data(params: OverpassDepartmentQueryParams) -> dict[str, Any]:
    query = build_overpass_department_query(params)

    logger.info(
        "Fetching Overpass data for department=%s, country=%s",
        params.department_name,
        params.country,
    )

    return _request_overpass_payload(
        query=query,
        context=f"Overpass API call for department={params.department_name}",
    )


def _extract_coordinates(element: dict[str, Any]) -> tuple[float | None, float | None]:
    if "lat" in element and "lon" in element:
        return _safe_float(element.get("lat")), _safe_float(element.get("lon"))

    center = element.get("center")
    if isinstance(center, dict):
        return _safe_float(center.get("lat")), _safe_float(center.get("lon"))

    return None, None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_osm_address(
    tags: dict[str, Any],
    *,
    fallback_city: str | None,
    fallback_country: str,
) -> dict[str, str | None]:
    address = merge_address_parts(
        house_number=tags.get("addr:housenumber") or tags.get("contact:housenumber"),
        street=tags.get("addr:street") or tags.get("contact:street"),
        address=tags.get("addr:full"),
    )

    return normalize_address_fields(
        address=address,
        postal_code=tags.get("addr:postcode") or tags.get("contact:postcode"),
        city=tags.get("addr:city") or tags.get("contact:city") or fallback_city,
        country=tags.get("addr:country") or fallback_country,
    )


def parse_overpass_elements(
    payload: dict[str, Any],
    *,
    fallback_city: str | None,
    fallback_country: str,
) -> list[dict[str, Any]]:
    elements = payload.get("elements", [])
    if not isinstance(elements, list):
        raise ValueError("Invalid Overpass payload: 'elements' must be a list.")

    records: list[dict[str, Any]] = []

    for element in elements:
        if not isinstance(element, dict):
            continue

        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            tags = {}

        name = clean_text(tags.get("name"))
        if not name:
            continue

        latitude, longitude = _extract_coordinates(element)

        address_fields = _extract_osm_address(
            tags,
            fallback_city=fallback_city,
            fallback_country=fallback_country,
        )

        amenity = tags.get("amenity")
        shop = tags.get("shop")

        category = map_osm_category(amenity=amenity, shop=shop)
        subcategory = extract_osm_subcategory(amenity=amenity, shop=shop)

        source_id = None
        element_type = clean_text(element.get("type"))
        element_id = element.get("id")
        if element_type and element_id is not None:
            source_id = f"{element_type}/{element_id}"

        record = build_standard_record(
            source=SourceName.OVERPASS.value,
            source_id=source_id,
            name=name,
            category=category,
            subcategory=subcategory,
            address=address_fields["address"],
            postal_code=address_fields["postal_code"],
            city=normalize_city(address_fields["city"]) if address_fields["city"] else None,
            country=normalize_country(address_fields["country"]) or fallback_country,
            latitude=latitude,
            longitude=longitude,
            phone=normalize_phone(tags.get("phone") or tags.get("contact:phone")),
            email=clean_text(tags.get("email") or tags.get("contact:email")),
            website=normalize_website(tags.get("website") or tags.get("contact:website")),
            opening_hours=clean_text(tags.get("opening_hours")),
            cuisine=clean_text(tags.get("cuisine")),
            rating=_safe_float(tags.get("rating")),
            review_count=_safe_int(tags.get("review_count")),
            siret=clean_text(tags.get("ref:FR:SIRET")),
            business_status=clean_text(tags.get("operational_status")),
            raw_payload=element,
        )
        records.append(record)

    logger.info("Parsed %d standard record(s) from Overpass payload.", len(records))
    return records


def fetch_and_parse_overpass(
    *,
    city: str | None = None,
    country: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    resolved_city = city or settings.pipeline.target_city
    resolved_country = country or settings.pipeline.target_country

    params = OverpassQueryParams(
        city=resolved_city,
        country=resolved_country,
    )

    payload = fetch_overpass_data(params)
    records = parse_overpass_elements(
        payload,
        fallback_city=resolved_city,
        fallback_country=resolved_country,
    )
    return payload, records


def fetch_and_parse_overpass_department(
    *,
    department_name: str,
    country: str = "France",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    params = OverpassDepartmentQueryParams(
        department_name=department_name,
        country=country,
    )

    payload = fetch_overpass_department_data(params)
    records = parse_overpass_elements(
        payload,
        fallback_city=None,
        fallback_country=country,
    )
    return payload, records