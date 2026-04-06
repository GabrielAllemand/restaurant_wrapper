from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.logger import get_logger


logger = get_logger(__name__)


DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": "restaurant-wrapper/1.0 (+academic-project)",
    "Accept": "application/json, text/plain, */*",
}


@dataclass(frozen=True)
class HttpClientConfig:
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_factor: float = 1.0
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    verify_ssl: bool = True


class HttpClientError(RuntimeError):
    """
    Exception métier pour encapsuler les erreurs HTTP du projet.
    """


def build_retry_strategy(
    *,
    max_retries: int,
    backoff_factor: float,
    status_forcelist: tuple[int, ...],
) -> Retry:
    """
    Construit une stratégie de retry compatible requests/urllib3.
    """
    return Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )


def create_session(
    config: HttpClientConfig | None = None,
    *,
    headers: dict[str, str] | None = None,
) -> Session:
    """
    Crée une session requests configurée avec retries et headers par défaut.
    """
    config = config or HttpClientConfig()

    session = requests.Session()
    retry_strategy = build_retry_strategy(
        max_retries=config.max_retries,
        backoff_factor=config.backoff_factor,
        status_forcelist=config.status_forcelist,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    merged_headers = DEFAULT_HEADERS.copy()
    if headers:
        merged_headers.update(headers)
    session.headers.update(merged_headers)

    return session


def _raise_for_bad_response(response: Response, *, context: str | None = None) -> None:
    """
    Transforme une réponse HTTP invalide en erreur métier explicite.
    """
    if response.ok:
        return

    context_prefix = f"{context} - " if context else ""
    body_preview = response.text[:500].replace("\n", " ").strip()

    raise HttpClientError(
        f"{context_prefix}HTTP {response.status_code} for {response.request.method} "
        f"{response.url}. Response preview: {body_preview}"
    )


def request_json(
    session: Session,
    *,
    method: str,
    url: str,
    timeout_seconds: int,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    data: dict[str, Any] | str | None = None,
    headers: dict[str, str] | None = None,
    context: str | None = None,
) -> dict[str, Any]:
    """
    Envoie une requête HTTP et retourne le JSON parsé.
    Lève une exception claire si la requête échoue ou si le JSON est invalide.
    """
    method_upper = method.upper()

    try:
        response = session.request(
            method=method_upper,
            url=url,
            params=params,
            json=json_body,
            data=data,
            headers=headers,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        context_prefix = f"{context} - " if context else ""
        raise HttpClientError(
            f"{context_prefix}Request failed for {method_upper} {url}: {exc}"
        ) from exc

    _raise_for_bad_response(response, context=context)

    try:
        payload = response.json()
    except ValueError as exc:
        context_prefix = f"{context} - " if context else ""
        preview = response.text[:500].replace("\n", " ").strip()
        raise HttpClientError(
            f"{context_prefix}Invalid JSON response for {method_upper} {url}. "
            f"Response preview: {preview}"
        ) from exc

    if not isinstance(payload, dict):
        raise HttpClientError(
            f"{context or 'HTTP request'} - Expected a JSON object from {url}, "
            f"got {type(payload).__name__}."
        )

    return payload


def request_json_any(
    session: Session,
    *,
    method: str,
    url: str,
    timeout_seconds: int,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    data: dict[str, Any] | str | None = None,
    headers: dict[str, str] | None = None,
    context: str | None = None,
) -> Any:
    """
    Variante plus permissive qui retourne n'importe quelle structure JSON
    (dict, list, etc.).
    """
    method_upper = method.upper()

    try:
        response = session.request(
            method=method_upper,
            url=url,
            params=params,
            json=json_body,
            data=data,
            headers=headers,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        context_prefix = f"{context} - " if context else ""
        raise HttpClientError(
            f"{context_prefix}Request failed for {method_upper} {url}: {exc}"
        ) from exc

    _raise_for_bad_response(response, context=context)

    try:
        return response.json()
    except ValueError as exc:
        context_prefix = f"{context} - " if context else ""
        preview = response.text[:500].replace("\n", " ").strip()
        raise HttpClientError(
            f"{context_prefix}Invalid JSON response for {method_upper} {url}. "
            f"Response preview: {preview}"
        ) from exc


def request_text(
    session: Session,
    *,
    method: str,
    url: str,
    timeout_seconds: int,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    data: dict[str, Any] | str | None = None,
    headers: dict[str, str] | None = None,
    context: str | None = None,
) -> str:
    """
    Envoie une requête HTTP et retourne le corps brut en texte.
    """
    method_upper = method.upper()

    try:
        response = session.request(
            method=method_upper,
            url=url,
            params=params,
            json=json_body,
            data=data,
            headers=headers,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        context_prefix = f"{context} - " if context else ""
        raise HttpClientError(
            f"{context_prefix}Request failed for {method_upper} {url}: {exc}"
        ) from exc

    _raise_for_bad_response(response, context=context)
    return response.text


def polite_sleep(seconds: float) -> None:
    """
    Petite temporisation utilitaire entre deux appels externes.
    """
    if seconds <= 0:
        return
    logger.debug("Sleeping for %.2f second(s) before next request.", seconds)
    time.sleep(seconds)