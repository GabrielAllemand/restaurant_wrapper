from __future__ import annotations

import re
import time
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from src.utils.logger import get_logger


logger = get_logger(__name__)

PAGESJAUNES_SEARCH_URL = "https://www.pagesjaunes.fr/annuaire/chercherlespros"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def _similarity(a: str | None, b: str | None) -> float:
    return SequenceMatcher(None, _normalize_text(a), _normalize_text(b)).ratio()


def build_pagesjaunes_search_url(
    *,
    name: str | None,
    city: str | None,
    postal_code: str | None,
) -> str:
    query_parts: list[str] = []

    if name:
        query_parts.append(str(name).strip())

    if postal_code and str(postal_code).strip().lower() != "nan":
        query_parts.append(str(postal_code).strip())
    elif city:
        query_parts.append(str(city).strip())

    what = " ".join(query_parts).strip()
    if not what:
        raise ValueError("Cannot build PagesJaunes query without a name.")

    return f"{PAGESJAUNES_SEARCH_URL}?quoiqui={quote_plus(what)}"


def _extract_text(element: Any) -> str | None:
    if element is None:
        return None
    text = element.get_text(" ", strip=True)
    return text or None


def _extract_phone_from_card(card: Any) -> str | None:
    text = card.get_text(" ", strip=True)
    if not text:
        return None

    patterns = [
        r"\b0[1-9](?:[\s\.\-]?\d{2}){4}\b",
        r"\+33[\s\.\-]?[1-9](?:[\s\.\-]?\d{2}){4}\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)

    return None


def _extract_website_from_card(card: Any) -> str | None:
    for link in card.select("a[href]"):
        href = link.get("href", "")
        text = _extract_text(link) or ""
        if not href:
            continue

        href_lower = href.lower()
        text_lower = text.lower()

        if "site" in text_lower or "website" in text_lower:
            return href

        if href_lower.startswith("http") and "pagesjaunes.fr" not in href_lower:
            return href

    return None


def _extract_detail_url_from_card(card: Any) -> str | None:
    for link in card.select("a[href]"):
        href = link.get("href", "")
        if not href:
            continue

        if href.startswith("/pros/"):
            return f"https://www.pagesjaunes.fr{href}"

        if href.startswith("https://www.pagesjaunes.fr/pros/"):
            return href

    return None


def parse_pagesjaunes_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")

    cards = soup.select("li.bi-bloc, div.bi-bloc, article")
    results: list[dict[str, Any]] = []

    for card in cards:
        name_el = (
            card.select_one("a.denomination-links")
            or card.select_one("h2")
            or card.select_one(".denomination-links")
        )
        address_el = (
            card.select_one(".adresse-container")
            or card.select_one(".adresse")
            or card.select_one("[class*='adresse']")
        )

        name = _extract_text(name_el)
        address = _extract_text(address_el)
        phone = _extract_phone_from_card(card)
        website = _extract_website_from_card(card)
        detail_url = _extract_detail_url_from_card(card)

        if not name:
            continue

        results.append(
            {
                "name": name,
                "address": address,
                "phone": phone,
                "website": website,
                "detail_url": detail_url,
            }
        )

    return results


def search_pagesjaunes(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    sleep_seconds: float = 1.0,
) -> list[dict[str, Any]]:
    url = build_pagesjaunes_search_url(
        name=row_name,
        city=row_city,
        postal_code=row_postal_code,
    )

    logger.info("PagesJaunes search URL: %s", url)

    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=20,
    )
    response.raise_for_status()

    time.sleep(sleep_seconds)

    return parse_pagesjaunes_results(response.text)


def score_pagesjaunes_candidate(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_address: str | None,
    candidate: dict[str, Any],
) -> float:
    candidate_name = candidate.get("name")
    candidate_address = candidate.get("address")

    name_score = _similarity(row_name, candidate_name)
    address_score = _similarity(row_address, candidate_address)

    city_score = 0.0
    postal_score = 0.0

    candidate_address_norm = _normalize_text(candidate_address)
    row_city_norm = _normalize_text(row_city)
    row_postal_norm = _normalize_text(row_postal_code)

    if row_city_norm and row_city_norm in candidate_address_norm:
        city_score = 1.0

    if row_postal_norm and row_postal_norm != "nan" and row_postal_norm in candidate_address_norm:
        postal_score = 1.0

    return 0.45 * name_score + 0.30 * address_score + 0.15 * city_score + 0.10 * postal_score


def select_best_pagesjaunes_candidate(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_address: str | None,
    candidates: list[dict[str, Any]],
    min_score: float = 0.70,
) -> dict[str, Any] | None:
    best_candidate: dict[str, Any] | None = None
    best_score = -1.0

    for candidate in candidates:
        score = score_pagesjaunes_candidate(
            row_name=row_name,
            row_city=row_city,
            row_postal_code=row_postal_code,
            row_address=row_address,
            candidate=candidate,
        )

        candidate["match_score"] = round(score, 4)

        if score > best_score:
            best_score = score
            best_candidate = candidate

    if best_candidate is None or best_score < min_score:
        return None

    return best_candidate


def enrich_row_with_pagesjaunes(
    *,
    row_name: str | None,
    row_city: str | None,
    row_postal_code: str | None,
    row_address: str | None,
) -> dict[str, Any] | None:
    if not row_name:
        return None

    candidates = search_pagesjaunes(
        row_name=row_name,
        row_city=row_city,
        row_postal_code=row_postal_code,
    )

    logger.info(
        "PagesJaunes returned %d candidate(s) for name=%s",
        len(candidates),
        row_name,
    )

    for candidate in candidates[:5]:
        logger.info(
            "PagesJaunes candidate | row_name=%s | candidate_name=%s | candidate_address=%s | candidate_phone=%s | score=%s",
            row_name,
            candidate.get("name"),
            candidate.get("address"),
            candidate.get("phone"),
            candidate.get("match_score"),
        )

    best = select_best_pagesjaunes_candidate(
        row_name=row_name,
        row_city=row_city,
        row_postal_code=row_postal_code,
        row_address=row_address,
        candidates=candidates,
    )

    if best is None:
        return None

    return {
        "pj_url": best.get("detail_url"),
        "pj_phone": best.get("phone"),
        "pj_website": best.get("website"),
        "pj_opening_hours": None,
        "pj_match_score": best.get("match_score"),
    }