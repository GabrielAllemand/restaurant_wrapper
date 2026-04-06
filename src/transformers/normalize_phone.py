from __future__ import annotations

import re
from typing import Any

from src.transformers.normalize_text import clean_text


_NON_DIGIT_RE = re.compile(r"\D+")


def extract_phone_digits(value: Any) -> str | None:
    """
    Extrait uniquement les chiffres d'un numéro de téléphone.
    """
    text = clean_text(value)
    if text is None:
        return None

    digits = _NON_DIGIT_RE.sub("", text)
    return digits or None


def normalize_phone(value: Any, *, default_country_code: str = "+33") -> str | None:
    """
    Normalise un téléphone dans un format international simple.
    Cas gérés principalement pour la France :
    - 01 23 45 67 89 -> +33123456789
    - +33 1 23 45 67 89 -> +33123456789
    - 33123456789 -> +33123456789
    """
    digits = extract_phone_digits(value)
    if digits is None:
        return None

    if digits.startswith("00"):
        return f"+{digits[2:]}"

    if digits.startswith("33") and len(digits) >= 11:
        return f"+{digits}"

    if digits.startswith("0") and len(digits) == 10 and default_country_code == "+33":
        return f"+33{digits[1:]}"

    if str(value).strip().startswith("+"):
        return f"+{digits}"

    if default_country_code and digits:
        return f"{default_country_code}{digits}"

    return digits


def is_valid_phone(value: Any) -> bool:
    """
    Validation minimale d'un téléphone normalisé.
    """
    normalized = normalize_phone(value)
    if normalized is None:
        return False

    digits = normalized.lstrip("+")
    return 9 <= len(digits) <= 15