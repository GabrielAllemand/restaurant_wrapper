from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests

from src.utils.logger import get_logger


logger = get_logger(__name__)


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class InpiRneConfig:
    base_url: str
    username: str
    password: str
    timeout_seconds: int = 30
    max_retries: int = 4
    retry_delay_seconds: float = 2.0
    inter_request_sleep_seconds: float = 0.25


def load_inpi_rne_config() -> InpiRneConfig:
    base_url = os.getenv("INPI_RNE_BASE_URL", "https://registre-national-entreprises.inpi.fr").strip()
    username = os.getenv("INPI_USERNAME", "").strip()
    password = os.getenv("INPI_PASSWORD", "").strip()
    timeout_seconds = int(os.getenv("INPI_RNE_TIMEOUT_SECONDS", "30"))
    max_retries = int(os.getenv("INPI_RNE_MAX_RETRIES", "4"))
    retry_delay_seconds = float(os.getenv("INPI_RNE_RETRY_DELAY_SECONDS", "2.0"))
    inter_request_sleep_seconds = float(os.getenv("INPI_RNE_INTER_REQUEST_SLEEP_SECONDS", "0.25"))

    if not username:
        raise ValueError("Missing INPI_USERNAME in environment.")
    if not password:
        raise ValueError("Missing INPI_PASSWORD in environment.")

    return InpiRneConfig(
        base_url=base_url.rstrip("/"),
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        inter_request_sleep_seconds=inter_request_sleep_seconds,
    )


class InpiRneClient:
    def __init__(self, config: InpiRneConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self._token: str | None = None

    def login(self) -> str:
        url = f"{self.config.base_url}/api/sso/login"
        payload = {
            "username": self.config.username,
            "password": self.config.password,
        }

        response = self.session.post(
            url,
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()

        data = response.json()
        token = data.get("token")
        if not token:
            raise ValueError("INPI login succeeded but no token was returned.")

        self._token = token
        logger.info("INPI RNE login succeeded.")
        return token

    def _auth_headers(self) -> dict[str, str]:
        if not self._token:
            self.login()
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    def get_company_by_siren(self, siren: str) -> dict[str, Any] | None:
        clean_siren = normalize_siren(siren)
        if clean_siren is None:
            return None

        url = f"{self.config.base_url}/api/companies/{clean_siren}"
        last_exception: Exception | None = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    headers=self._auth_headers(),
                    timeout=self.config.timeout_seconds,
                )

                if response.status_code == 401:
                    if attempt == self.config.max_retries:
                        response.raise_for_status()

                    logger.warning(
                        "INPI token expired or invalid for siren=%s on attempt %d/%d. Re-authenticating.",
                        clean_siren,
                        attempt,
                        self.config.max_retries,
                    )
                    self.login()
                    time.sleep(self.config.retry_delay_seconds)
                    continue

                if response.status_code == 404:
                    return None

                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt == self.config.max_retries:
                        response.raise_for_status()

                    logger.warning(
                        "INPI RNE GET /companies/%s returned HTTP %s on attempt %d/%d. Retrying in %.1fs.",
                        clean_siren,
                        response.status_code,
                        attempt,
                        self.config.max_retries,
                        self.config.retry_delay_seconds,
                    )
                    time.sleep(self.config.retry_delay_seconds)
                    continue

                response.raise_for_status()
                payload = response.json()

                if self.config.inter_request_sleep_seconds > 0:
                    time.sleep(self.config.inter_request_sleep_seconds)

                return payload

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                last_exception = exc

                if attempt == self.config.max_retries:
                    logger.warning(
                        "INPI RNE GET /companies/%s failed after %d attempt(s) with network error: %s",
                        clean_siren,
                        attempt,
                        exc,
                    )
                    return None

                logger.warning(
                    "INPI RNE GET /companies/%s network error on attempt %d/%d: %s. Retrying in %.1fs.",
                    clean_siren,
                    attempt,
                    self.config.max_retries,
                    exc,
                    self.config.retry_delay_seconds,
                )
                time.sleep(self.config.retry_delay_seconds)

            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None

                if status_code in RETRYABLE_STATUS_CODES and attempt < self.config.max_retries:
                    logger.warning(
                        "INPI RNE GET /companies/%s returned HTTP %s on attempt %d/%d. Retrying in %.1fs.",
                        clean_siren,
                        status_code,
                        attempt,
                        self.config.max_retries,
                        self.config.retry_delay_seconds,
                    )
                    time.sleep(self.config.retry_delay_seconds)
                    continue

                raise

        if last_exception is not None:
            return None

        return None


def normalize_siren(value: Any) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) != 9:
        return None
    return digits


def normalize_siret(value: Any) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) != 14:
        return None
    return digits


def _get_nested(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _parse_date(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if len(text) >= 10:
        return text[:10]

    return text


def _parse_datetime(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.isoformat()
    except ValueError:
        return text


def _join_person_name(last_name: Any, first_names: Any) -> str | None:
    parts: list[str] = []

    if isinstance(first_names, list):
        parts.extend(str(x).strip() for x in first_names if str(x).strip())
    elif first_names:
        parts.append(str(first_names).strip())

    if last_name and str(last_name).strip():
        parts.append(str(last_name).strip())

    full_name = " ".join(parts).strip()
    return full_name or None


def _extract_company_name(formality: dict[str, Any]) -> str | None:
    return _first_non_empty(
        _get_nested(formality, "content", "exploitation", "identite", "entreprise", "nomExploitation"),
        _get_nested(formality, "content", "personneMorale", "identite", "entreprise", "denomination"),
        _get_nested(formality, "content", "personneMorale", "identite", "entreprise", "nomCommercial"),
        _get_nested(formality, "content", "personnePhysique", "identite", "entreprise", "nomExploitation"),
        _get_nested(formality, "content", "personnePhysique", "identite", "entreprise", "denomination"),
    )


def _extract_main_establishment(formality: dict[str, Any]) -> dict[str, Any] | None:
    return _first_non_empty(
        _get_nested(formality, "content", "exploitation", "etablissementPrincipal"),
        _get_nested(formality, "content", "personneMorale", "etablissementPrincipal"),
        _get_nested(formality, "content", "personnePhysique", "etablissementPrincipal"),
    )


def _extract_main_activity(establishment: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(establishment, dict):
        return None

    activities = establishment.get("activites")
    if not isinstance(activities, list) or not activities:
        return None

    principal = [a for a in activities if isinstance(a, dict) and a.get("indicateurPrincipal") is True]
    if principal:
        return principal[0]

    for activity in activities:
        if isinstance(activity, dict):
            return activity

    return None


def _extract_powers(formality: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        _get_nested(formality, "content", "exploitation", "composition", "pouvoirs"),
        _get_nested(formality, "content", "personneMorale", "composition", "pouvoirs"),
        _get_nested(formality, "content", "personnePhysique", "composition", "pouvoirs"),
    ]

    for powers in candidates:
        if isinstance(powers, list):
            return [p for p in powers if isinstance(p, dict)]

    return []


def _extract_representative_name_and_role(formality: dict[str, Any]) -> tuple[str | None, str | None]:
    for power in _extract_powers(formality):
        role = _first_non_empty(power.get("roleEntreprise"), power.get("secondRoleEntreprise"))

        representative = power.get("representant")
        if isinstance(representative, dict):
            person = representative.get("descriptionPersonne", {})
            if isinstance(person, dict):
                name = _join_person_name(person.get("nom"), person.get("prenoms"))
                if name:
                    return name, role

        individual = power.get("individu")
        if isinstance(individual, dict):
            person = individual.get("descriptionPersonne", {})
            if isinstance(person, dict):
                name = _join_person_name(person.get("nom"), person.get("prenoms"))
                if name:
                    return name, role

        company = power.get("entreprise")
        if isinstance(company, dict):
            name = _first_non_empty(
                company.get("denomination"),
                company.get("nomCommercial"),
            )
            if name:
                return str(name), role

    return None, None


def _extract_last_history_event(formality: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    history = formality.get("historique")
    if not isinstance(history, list) or not history:
        return None, None, None

    valid_rows = [row for row in history if isinstance(row, dict)]
    if not valid_rows:
        return None, None, None

    def sort_key(row: dict[str, Any]) -> str:
        return str(_first_non_empty(row.get("dateIntegration"), row.get("dateEffet"), ""))

    last_row = sorted(valid_rows, key=sort_key)[-1]

    return (
        _first_non_empty(last_row.get("codeEvenement")),
        _first_non_empty(last_row.get("libelleEvenement")),
        _parse_date(_first_non_empty(last_row.get("dateEffet"), last_row.get("dateIntegration"))),
    )


def extract_inpi_rne_fields(payload: dict[str, Any]) -> dict[str, Any]:
    formality = payload.get("formality", payload)
    if not isinstance(formality, dict):
        formality = {}

    main_establishment = _extract_main_establishment(formality)
    main_establishment_desc = (
        main_establishment.get("descriptionEtablissement", {})
        if isinstance(main_establishment, dict)
        else {}
    )
    main_establishment_address = (
        main_establishment.get("adresse", {})
        if isinstance(main_establishment, dict)
        else {}
    )
    main_activity = _extract_main_activity(main_establishment)

    rep_name, rep_role = _extract_representative_name_and_role(formality)
    last_event_code, last_event_label, last_event_date = _extract_last_history_event(formality)

    company_identity = _first_non_empty(
        _get_nested(formality, "content", "exploitation", "identite", "entreprise"),
        _get_nested(formality, "content", "personneMorale", "identite", "entreprise"),
        _get_nested(formality, "content", "personnePhysique", "identite", "entreprise"),
    )
    if not isinstance(company_identity, dict):
        company_identity = {}

    return {
        "inpi_rne_siren": _first_non_empty(formality.get("siren"), payload.get("siren")),
        "inpi_rne_type_personne": formality.get("typePersonne"),
        "inpi_rne_diffusion_commerciale": formality.get("diffusionCommerciale"),
        "inpi_rne_diffusion_insee": formality.get("diffusionINSEE"),
        "inpi_rne_updated_at": _parse_datetime(payload.get("updatedAt")),
        "inpi_rne_date_creation": _parse_date(_get_nested(formality, "content", "natureCreation", "dateCreation")),
        "inpi_rne_forme_juridique": _first_non_empty(
            formality.get("formeJuridique"),
            _get_nested(formality, "content", "natureCreation", "formeJuridique"),
            company_identity.get("formeJuridique"),
        ),
        "inpi_rne_company_name": _extract_company_name(formality),
        "inpi_rne_main_siret": normalize_siret(main_establishment_desc.get("siret")),
        "inpi_rne_main_code_ape": _first_non_empty(
            main_establishment_desc.get("codeApe"),
            main_activity.get("codeApe") if isinstance(main_activity, dict) else None,
        ),
        "inpi_rne_main_status": main_establishment_desc.get("statutPourFormalite"),
        "inpi_rne_main_postal_code": main_establishment_address.get("codePostal"),
        "inpi_rne_main_city": main_establishment_address.get("commune"),
        "inpi_rne_activity_start_date": _parse_date(
            _first_non_empty(
                company_identity.get("dateDebutExploitation"),
                company_identity.get("dateDebutActiv"),
                main_activity.get("dateDebut") if isinstance(main_activity, dict) else None,
            )
        ),
        "inpi_rne_nombre_representants_actifs": payload.get("nombreRepresentantsActifs"),
        "inpi_rne_nombre_etablissements_ouverts": payload.get("nombreEtablissementsOuverts"),
        "inpi_rne_representative_name": rep_name,
        "inpi_rne_representative_role": rep_role,
        "inpi_rne_last_event_code": last_event_code,
        "inpi_rne_last_event_label": last_event_label,
        "inpi_rne_last_event_date_effet": last_event_date,
        "inpi_rne_payload": payload,
    }