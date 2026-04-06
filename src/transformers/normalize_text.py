from __future__ import annotations

import re
import unicodedata
from typing import Any


_MULTISPACE_RE = re.compile(r"\s+")
_EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)


def clean_text(value: Any) -> str | None:
    """
    Nettoie une chaîne :
    - cast en str
    - trim
    - suppression des espaces multiples
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = _MULTISPACE_RE.sub(" ", text)
    return text


def normalize_name(value: Any) -> str | None:
    """
    Normalise un nom d'établissement sans détruire la casse de lecture.
    """
    text = clean_text(value)
    if text is None:
        return None
    return text


def normalize_city(value: Any) -> str | None:
    """
    Normalise une ville en conservant une écriture lisible.
    """
    text = clean_text(value)
    if text is None:
        return None
    return text.title()


def normalize_country(value: Any) -> str | None:
    """
    Normalise le pays en format lisible.
    """
    text = clean_text(value)
    if text is None:
        return None
    return text.title()


def normalize_postal_code(value: Any) -> str | None:
    """
    Normalise un code postal simple.
    """
    text = clean_text(value)
    if text is None:
        return None

    text = text.replace(" ", "")
    return text or None


def normalize_website(value: Any) -> str | None:
    """
    Normalise un site web sans validation trop agressive.
    """
    text = clean_text(value)
    if text is None:
        return None

    if text.startswith("www."):
        return f"https://{text}"

    return text


def normalize_email(value: Any) -> str | None:
    """
    Nettoie et valide minimalement un email.
    """
    text = clean_text(value)
    if text is None:
        return None

    email = text.lower()
    return email if _EMAIL_RE.match(email) else None


def ascii_fold(value: Any) -> str | None:
    """
    Supprime les accents pour du matching technique.
    """
    text = clean_text(value)
    if text is None:
        return None

    normalized = unicodedata.normalize("NFKD", text)
    folded = normalized.encode("ascii", "ignore").decode("ascii")
    folded = _MULTISPACE_RE.sub(" ", folded).strip()
    return folded or None


def normalize_for_matching(value: Any) -> str | None:
    """
    Forme agressivement normalisée pour comparaison/fuzzy matching.
    """
    text = ascii_fold(value)
    if text is None:
        return None

    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9 ]+", " ", lowered)
    lowered = _MULTISPACE_RE.sub(" ", lowered).strip()
    return lowered or None