from __future__ import annotations

from datetime import datetime, timezone


def now_utc() -> datetime:
    """
    Retourne la date/heure courante en UTC, timezone-aware.
    """
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """
    Retourne la date/heure courante en ISO 8601 UTC.
    Exemple : 2026-03-15T10:30:45Z
    """
    return now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def compact_utc_timestamp() -> str:
    """
    Retourne un timestamp compact UTC.
    Exemple : 20260315T103045Z
    """
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def parse_iso_datetime(value: str) -> datetime:
    """
    Parse une chaîne ISO 8601 en datetime.
    Supporte notamment le suffixe 'Z'.
    """
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)