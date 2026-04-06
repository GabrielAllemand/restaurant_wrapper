from __future__ import annotations

import re
from typing import Any

from src.transformers.normalize_text import clean_text, normalize_city, normalize_country, normalize_postal_code


_MULTISPACE_RE = re.compile(r"\s+")


def normalize_address_line(value: Any) -> str | None:
    """
    Normalise une ligne d'adresse libre.
    """
    text = clean_text(value)
    if text is None:
        return None

    text = text.replace("\n", " ").replace("\r", " ")
    text = _MULTISPACE_RE.sub(" ", text).strip(" ,;")
    return text or None


def merge_address_parts(
    *,
    house_number: Any = None,
    street: Any = None,
    address: Any = None,
) -> str | None:
    """
    Construit une adresse lisible à partir de composants simples.
    Priorité à une adresse déjà fournie.
    """
    direct_address = normalize_address_line(address)
    if direct_address:
        return direct_address

    parts: list[str] = []

    house_number_text = clean_text(house_number)
    street_text = clean_text(street)

    if house_number_text:
        parts.append(house_number_text)
    if street_text:
        parts.append(street_text)

    if not parts:
        return None

    return " ".join(parts)


def normalize_address_fields(
    *,
    address: Any = None,
    postal_code: Any = None,
    city: Any = None,
    country: Any = None,
) -> dict[str, str | None]:
    """
    Normalise les principaux champs d'adresse dans un format homogène.
    """
    normalized_address = normalize_address_line(address)
    normalized_postal_code = normalize_postal_code(postal_code)
    normalized_city = normalize_city(city)
    normalized_country = normalize_country(country)

    return {
        "address": normalized_address,
        "postal_code": normalized_postal_code,
        "city": normalized_city,
        "country": normalized_country,
    }


def build_full_address(
    *,
    address: Any = None,
    postal_code: Any = None,
    city: Any = None,
    country: Any = None,
) -> str | None:
    """
    Construit une adresse complète en une seule chaîne.
    """
    normalized = normalize_address_fields(
        address=address,
        postal_code=postal_code,
        city=city,
        country=country,
    )

    parts = [
        normalized["address"],
        " ".join(part for part in [normalized["postal_code"], normalized["city"]] if part),
        normalized["country"],
    ]

    final_parts = [part for part in parts if part]
    if not final_parts:
        return None

    return ", ".join(final_parts)