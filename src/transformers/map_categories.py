from __future__ import annotations

from typing import Any


AMENITY_TO_CATEGORY: dict[str, str] = {
    "restaurant": "restaurant",
    "fast_food": "restauration_rapide",
    "cafe": "cafe",
    "bar": "bar",
    "pub": "bar",
    "biergarten": "bar",
    "ice_cream": "glacier",
    "food_court": "restauration",
    "bakery": "boulangerie",
}

SHOP_TO_CATEGORY: dict[str, str] = {
    "bakery": "boulangerie",
    "pastry": "patisserie",
    "confectionery": "confiserie",
    "butcher": "boucherie",
    "cheese": "fromagerie",
    "greengrocer": "primeur",
    "supermarket": "superette",
    "convenience": "superette",
    "mall": "commerce",
    "department_store": "commerce",
    "clothes": "habillement",
    "hairdresser": "coiffure",
    "beauty": "beaute",
    "florist": "fleuriste",
    "pharmacy": "pharmacie",
    "wine": "caviste",
    "coffee": "cafe",
}

DEFAULT_CATEGORY = "commerce"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip().lower()
    return cleaned or None


def map_osm_category(*, amenity: Any = None, shop: Any = None) -> str | None:
    """
    Mappe les tags OSM vers une catégorie métier interne.
    """
    amenity_value = _clean(amenity)
    shop_value = _clean(shop)

    if amenity_value and amenity_value in AMENITY_TO_CATEGORY:
        return AMENITY_TO_CATEGORY[amenity_value]

    if shop_value and shop_value in SHOP_TO_CATEGORY:
        return SHOP_TO_CATEGORY[shop_value]

    if amenity_value:
        return amenity_value

    if shop_value:
        return shop_value

    return None


def extract_osm_subcategory(*, amenity: Any = None, shop: Any = None) -> str | None:
    """
    Retourne la sous-catégorie technique d'origine OSM.
    Priorité à amenity, puis shop.
    """
    amenity_value = _clean(amenity)
    if amenity_value:
        return amenity_value

    shop_value = _clean(shop)
    if shop_value:
        return shop_value

    return None