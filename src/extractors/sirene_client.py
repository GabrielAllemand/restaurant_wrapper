from __future__ import annotations

import os
import time
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any


import requests

from src.utils.logger import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class SireneConfig:
    base_url: str
    api_key: str
    timeout_seconds: int = 30


def load_sirene_config() -> SireneConfig:
    base_url = os.getenv("SIRENE_BASE_URL", "").strip()
    api_key = os.getenv("SIRENE_API_KEY", "").strip()
    timeout_seconds = int(os.getenv("SIRENE_TIMEOUT_SECONDS", "30"))

    if not base_url:
        raise ValueError("Missing SIRENE_BASE_URL in environment.")
    if not api_key:
        raise ValueError("Missing SIRENE_API_KEY in environment.")

    return SireneConfig(
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        timeout_seconds=timeout_seconds,
    )


def _clean_optional_string(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.lower() in {"nan", "none", "null"}:
        return None

    return text


def _escape_query_value(value: str) -> str:
    return value.replace('"', '\\"')


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_business_text(value: str | None) -> str:
    if not value:
        return ""

    text = _strip_accents(str(value).lower())
    text = text.replace("&", " and ")
    text = text.replace("°", " ")
    text = text.replace("st", "saint")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    weak_words = {
        "bar",
        "restaurant",
        "cafe",
        "café",
        "brasserie",
        "bistro",
        "le",
        "la",
        "les",
        "de",
        "du",
        "des",
        "au",
        "aux",
    }

    tokens = [token for token in text.split() if token not in weak_words]
    return " ".join(tokens).strip()


def normalize_address_text(value: str | None) -> str:
    if not value:
        return ""

    text = _strip_accents(str(value).lower())
    text = text.replace(",", " ")
    text = re.sub(r"\br\b", "rue", text)
    text = re.sub(r"\bav\b", "avenue", text)
    text = re.sub(r"\bbd\b", "boulevard", text)
    text = re.sub(r"\bpl\b", "place", text)
    text = re.sub(r"\bst\b", "saint", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_sirene_query(
    *,
    field_name: str,
    name: str | None,
    city: str | None,
    postal_code: str | None,
) -> str:
    clauses: list[str] = []

    safe_name = _clean_optional_string(name)
    safe_city = _clean_optional_string(city)
    safe_postal_code = _clean_optional_string(postal_code)

    if safe_name:
        clauses.append(f'{field_name}:"{_escape_query_value(safe_name)}"')

    if safe_city:
        clauses.append(
            f'libelleCommuneEtablissement:"{_escape_query_value(safe_city)}"'
        )

    if safe_postal_code:
        clauses.append(
            f'codePostalEtablissement:"{_escape_query_value(safe_postal_code)}"'
        )

    if not clauses:
        raise ValueError("Cannot build Sirene query without at least one criterion.")

    return " AND ".join(clauses)


def build_sirene_queries(
    *,
    name: str | None,
    city: str | None,
    postal_code: str | None,
) -> list[str]:
    clean_name = _clean_optional_string(name)
    clean_city = _clean_optional_string(city)
    clean_postal_code = _clean_optional_string(postal_code)

    query_specs = [
        ("denominationUniteLegale", clean_name, clean_city, clean_postal_code),
        ("denominationUniteLegale", clean_name, clean_city, None),
        ("denominationUniteLegale", clean_name, None, None),
    ]

    queries: list[str] = []
    seen: set[str] = set()

    for field_name, q_name, q_city, q_postal_code in query_specs:
        try:
            query = build_sirene_query(
                field_name=field_name,
                name=q_name,
                city=q_city,
                postal_code=q_postal_code,
            )
        except ValueError:
            continue

        if query not in seen:
            seen.add(query)
            queries.append(query)

    return queries

def search_establishments(
    *,
    query: str,
    config: SireneConfig,
    per_page: int = 10,
    max_retries: int = 3,
    retry_delay_seconds: float = 3.0,
) -> list[dict[str, Any]]:
    url = f"{config.base_url}/siret"

    headers = {
        "Accept": "application/json",
        "X-INSEE-Api-Key-Integration": config.api_key,
    }

    params = {
        "q": query,
        "nombre": per_page,
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=config.timeout_seconds,
            )

            if response.status_code == 404:
                return []

            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.exceptions.HTTPError(
                    f"Temporary server error: {response.status_code}",
                    response=response,
                )

            response.raise_for_status()

            payload = response.json()
            etablissements = payload.get("etablissements", [])

            if not isinstance(etablissements, list):
                return []

            return etablissements

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt == max_retries:
                logger.warning(
                    "Sirene query failed after %d attempt(s) for query=%s | error=%s",
                    attempt,
                    query,
                    exc,
                )
                return []

            logger.warning(
                "Sirene query failed on attempt %d/%d for query=%s | error=%s | retry in %.1fs",
                attempt,
                max_retries,
                query,
                exc,
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None

            if status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
                logger.warning(
                    "Sirene query returned HTTP %s on attempt %d/%d for query=%s | retry in %.1fs",
                    status_code,
                    attempt,
                    max_retries,
                    query,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue

            logger.warning(
                "Sirene query failed for query=%s with HTTP %s",
                query,
                status_code,
            )
            return []


def _similarity(a: str | None, b: str | None, *, mode: str = "business") -> float:
    if mode == "address":
        left = normalize_address_text(a)
        right = normalize_address_text(b)
    else:
        left = normalize_business_text(a)
        right = normalize_business_text(b)

    if not left or not right:
        return 0.0

    return SequenceMatcher(None, left, right).ratio()


def _extract_candidate_name(candidate: dict[str, Any]) -> str | None:
    unite_legale = candidate.get("uniteLegale", {}) or {}

    legal_name = unite_legale.get("denominationUniteLegale")
    if legal_name:
        return str(legal_name).strip()

    for field_name in (
        "enseigne1Etablissement",
        "enseigne2Etablissement",
        "enseigne3Etablissement",
    ):
        value = candidate.get(field_name)
        if value:
            return str(value).strip()

    composed_name = " ".join(
        part
        for part in [
            unite_legale.get("prenomUsuelUniteLegale"),
            unite_legale.get("nomUniteLegale"),
        ]
        if part
    ).strip()

    return composed_name or None


def _extract_candidate_city(candidate: dict[str, Any]) -> str | None:
    address = candidate.get("adresseEtablissement", {}) or {}
    value = address.get("libelleCommuneEtablissement")
    return _clean_optional_string(value)


def _extract_candidate_postal_code(candidate: dict[str, Any]) -> str | None:
    address = candidate.get("adresseEtablissement", {}) or {}
    value = address.get("codePostalEtablissement")
    return _clean_optional_string(value)


def _extract_candidate_address(candidate: dict[str, Any]) -> str | None:
    address = candidate.get("adresseEtablissement", {}) or {}

    parts = [
        address.get("numeroVoieEtablissement"),
        address.get("indiceRepetitionEtablissement"),
        address.get("typeVoieEtablissement"),
        address.get("libelleVoieEtablissement"),
    ]
    cleaned = [str(part).strip() for part in parts if _clean_optional_string(part)]
    return " ".join(cleaned) if cleaned else None


def _extract_candidate_business_status(candidate: dict[str, Any]) -> str | None:
    return _clean_optional_string(candidate.get("etatAdministratifEtablissement"))


def _extract_candidate_siret(candidate: dict[str, Any]) -> str | None:
    siren = _clean_optional_string(candidate.get("siren"))
    nic = _clean_optional_string(candidate.get("nic"))
    if siren and nic:
        return f"{siren}{nic}"

    return _clean_optional_string(candidate.get("siret"))


def _is_short_or_ambiguous_name(name: str | None) -> bool:
    cleaned = normalize_business_text(name)
    if not cleaned:
        return True

    if cleaned.isdigit():
        return True

    if len(cleaned) <= 4:
        return True

    if len(cleaned.split()) == 1 and len(cleaned) <= 6:
        return True

    return False


def score_candidate(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_address: str | None,
    candidate: dict[str, Any],
) -> float:
    candidate_name = _extract_candidate_name(candidate)
    candidate_city = _extract_candidate_city(candidate)
    candidate_postal_code = _extract_candidate_postal_code(candidate)
    candidate_address = _extract_candidate_address(candidate)

    name_score = _similarity(row_name, candidate_name)
    city_score = _similarity(row_city, candidate_city)
    address_score = _similarity(row_address, candidate_address)

    postal_score = 0.0
    if row_postal_code and candidate_postal_code:
        postal_score = 1.0 if str(row_postal_code) == str(candidate_postal_code) else 0.0

    if row_address:
        return (
            0.40 * name_score
            + 0.20 * city_score
            + 0.20 * postal_score
            + 0.20 * address_score
        )

    return 0.55 * name_score + 0.30 * city_score + 0.15 * postal_score

def select_best_candidate(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_address: str | None,
    candidates: list[dict[str, Any]],
    min_score: float = 0.75,
) -> dict[str, Any] | None:
    best_candidate: dict[str, Any] | None = None
    best_score = -1.0

    for candidate in candidates:
        candidate_name = _extract_candidate_name(candidate)
        candidate_city = _extract_candidate_city(candidate)
        candidate_postal_code = _extract_candidate_postal_code(candidate)
        candidate_address = _extract_candidate_address(candidate)

        name_score = _similarity(row_name, candidate_name)
        city_score = _similarity(row_city, candidate_city)
        address_score = _similarity(row_address, candidate_address)

        postal_match = False
        if row_postal_code and candidate_postal_code:
            postal_match = str(row_postal_code) == str(candidate_postal_code)

        score = score_candidate(
            row_name=row_name,
            row_city=row_city,
            row_postal_code=row_postal_code,
            row_address=row_address,
            candidate=candidate,
        )

        # garde-fou 1 : le nom doit déjà être assez proche
        if name_score < 0.70:
            continue

        # garde-fou 2 : si on a un code postal source, il doit matcher
        if row_postal_code and candidate_postal_code and not postal_match:
            continue

        # garde-fou 3 : si on a une adresse source ET une adresse candidate,
        # on exige une vraie cohérence d'adresse
        if row_address and candidate_address and address_score < 0.65:
            continue

        # garde-fou 4 : si on a au moins ville + adresse, la ville doit être cohérente
        if row_city and city_score < 0.60:
            continue

        if score > best_score:
            best_score = score
            best_candidate = candidate

    if best_candidate is None or best_score < min_score:
        return None

    best_candidate["_match_score"] = round(best_score, 4)
    return best_candidate


def enrich_row_with_sirene(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_address: str | None,
    config: SireneConfig,
) -> dict[str, Any] | None:
    queries = build_sirene_queries(
        name=row_name,
        city=row_city,
        postal_code=row_postal_code,
    )

    if not queries:
        logger.info(
            "Sirene skipped: no query could be built for name=%s city=%s postal_code=%s",
            row_name,
            row_city,
            row_postal_code,
        )
        return None

    all_candidates: list[dict[str, Any]] = []

    for query in queries:
        logger.info("Sirene query: %s", query)

        candidates = search_establishments(
            query=query,
            config=config,
            per_page=10,
        )

        logger.info(
            "Sirene returned %d candidate(s) for name=%s",
            len(candidates),
            row_name,
        )

        if candidates:
            all_candidates = candidates
            break

    if not all_candidates:
        logger.info(
            "Sirene found no candidates for name=%s city=%s postal_code=%s",
            row_name,
            row_city,
            row_postal_code,
        )
        return None

    for candidate in all_candidates[:5]:
        logger.info(
            "Sirene candidate | row_name=%s | candidate_name=%s | candidate_city=%s | candidate_postal_code=%s | candidate_address=%s",
            row_name,
            _extract_candidate_name(candidate),
            _extract_candidate_city(candidate),
            _extract_candidate_postal_code(candidate),
            _extract_candidate_address(candidate),
        )

    best = select_best_candidate(
        row_name=row_name,
        row_city=row_city,
        row_postal_code=row_postal_code,
        row_address=row_address,
        candidates=all_candidates,
    )

    if best is None:
        best_score = max(
            [
                score_candidate(
                    row_name=row_name,
                    row_city=row_city,
                    row_postal_code=row_postal_code,
                    row_address=row_address,
                    candidate=candidate,
                )
                for candidate in all_candidates
            ],
            default=0.0,
        )

        logger.info(
            "Sirene candidates found but no candidate passed scoring for name=%s city=%s postal_code=%s address=%s | best_score=%.4f",
            row_name,
            row_city,
            row_postal_code,
            row_address,
            best_score,
        )
        return None

    return {
        "sirene_siret": _extract_candidate_siret(best),
        "sirene_business_status": _extract_candidate_business_status(best),
        "sirene_address": _extract_candidate_address(best),
        "sirene_city": _extract_candidate_city(best),
        "sirene_postal_code": _extract_candidate_postal_code(best),
        "sirene_name": _extract_candidate_name(best),
        "sirene_match_score": best.get("_match_score"),
    }