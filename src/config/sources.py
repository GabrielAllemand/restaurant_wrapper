from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class SourceName(StrEnum):
    OVERPASS = "overpass"
    GEOCODING = "geocoding"
    SIRENE = "sirene"
    PAGES_JAUNES = "pages_jaunes"


@dataclass(frozen=True)
class SourceDefinition:
    name: SourceName
    label: str
    description: str
    source_type: str
    enabled_by_default: bool = True


SOURCES: Final[dict[SourceName, SourceDefinition]] = {
    SourceName.OVERPASS: SourceDefinition(
        name=SourceName.OVERPASS,
        label="OpenStreetMap Overpass API",
        description=(
            "Source principale de collecte des établissements via les objets "
            "et tags OSM (amenity, shop, cuisine, opening_hours, etc.)."
        ),
        source_type="api",
        enabled_by_default=True,
    ),
    SourceName.GEOCODING: SourceDefinition(
        name=SourceName.GEOCODING,
        label="Geocoding API",
        description=(
            "Source d'enrichissement géographique et de normalisation "
            "des adresses / coordonnées."
        ),
        source_type="api",
        enabled_by_default=True,
    ),
    SourceName.SIRENE: SourceDefinition(
        name=SourceName.SIRENE,
        label="SIRENE / API Entreprise",
        description=(
            "Source d'enrichissement administratif pour validation "
            "et récupération d'informations officielles si disponibles."
        ),
        source_type="api",
        enabled_by_default=True,
    ),
    SourceName.PAGES_JAUNES: SourceDefinition(
        name=SourceName.PAGES_JAUNES,
        label="PagesJaunes",
        description=(
            "Source de scraping portée par les autres membres du groupe. "
            "Présente ici pour garantir un schéma de sources unifié."
        ),
        source_type="scraping",
        enabled_by_default=False,
    ),
}


API_SOURCE_NAMES: Final[tuple[SourceName, ...]] = (
    SourceName.OVERPASS,
    SourceName.GEOCODING,
    SourceName.SIRENE,
)

SCRAPING_SOURCE_NAMES: Final[tuple[SourceName, ...]] = (
    SourceName.PAGES_JAUNES,
)


def get_source_definition(source_name: SourceName | str) -> SourceDefinition:
    """
    Retourne la définition d'une source à partir de son enum ou de sa chaîne.
    """
    normalized = SourceName(source_name)
    return SOURCES[normalized]


def is_api_source(source_name: SourceName | str) -> bool:
    """
    Indique si la source est une source API.
    """
    normalized = SourceName(source_name)
    return normalized in API_SOURCE_NAMES


def is_scraping_source(source_name: SourceName | str) -> bool:
    """
    Indique si la source est une source de scraping.
    """
    normalized = SourceName(source_name)
    return normalized in SCRAPING_SOURCE_NAMES


def list_enabled_sources(*, include_disabled: bool = False) -> list[SourceDefinition]:
    """
    Retourne la liste des sources connues.
    """
    if include_disabled:
        return list(SOURCES.values())
    return [source for source in SOURCES.values() if source.enabled_by_default]