from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

# Charge les variables d'environnement depuis .env à la racine du projet
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
ENV_FILE: Final[Path] = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)


def _get_env(name: str, default: str | None = None, *, required: bool = False) -> str:
    """
    Récupère une variable d'environnement de manière centralisée.
    """
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return (value or "").strip()


def _get_bool(name: str, default: bool = False) -> bool:
    """
    Convertit proprement une variable d'environnement en booléen.
    Valeurs acceptées pour True : 1, true, yes, y, on
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    """
    Convertit une variable d'environnement en entier avec message d'erreur clair.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got: {raw!r}") from exc


@dataclass(frozen=True)
class Paths:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    interim_dir: Path
    processed_dir: Path
    logs_dir: Path

    raw_overpass_dir: Path
    raw_geocoding_dir: Path
    raw_sirene_dir: Path

    processed_csv_path: Path
    processed_parquet_path: Path

    def ensure_directories(self) -> None:
        """
        Crée tous les dossiers nécessaires si absents.
        """
        directories = [
            self.data_dir,
            self.raw_dir,
            self.interim_dir,
            self.processed_dir,
            self.logs_dir,
            self.raw_overpass_dir,
            self.raw_geocoding_dir,
            self.raw_sirene_dir,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class OverpassConfig:
    base_url: str
    timeout_seconds: int
    sleep_seconds: float
    max_retries: int
    default_timeout_clause_seconds: int
    default_radius_meters: int
    default_city: str
    default_country: str


@dataclass(frozen=True)
class GeocodingConfig:
    search_url: str
    reverse_url: str
    timeout_seconds: int
    max_retries: int
    limit: int


@dataclass(frozen=True)
class SireneConfig:
    base_url: str
    token: str
    timeout_seconds: int
    max_retries: int


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    echo_sql: bool

    @property
    def sqlalchemy_url(self) -> str:
        """
        URL SQLAlchemy compatible avec psycopg2.
        """
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class PipelineConfig:
    target_city: str
    target_country: str
    write_csv: bool
    write_parquet: bool
    save_raw_payloads: bool
    log_level: str


@dataclass(frozen=True)
class Settings:
    paths: Paths
    overpass: OverpassConfig
    geocoding: GeocodingConfig
    sirene: SireneConfig
    postgres: PostgresConfig
    pipeline: PipelineConfig


def build_settings() -> Settings:
    data_dir = PROJECT_ROOT / "data"
    raw_dir = data_dir / "raw"
    interim_dir = data_dir / "interim"
    processed_dir = data_dir / "processed"
    logs_dir = PROJECT_ROOT / "logs"

    paths = Paths(
        project_root=PROJECT_ROOT,
        data_dir=data_dir,
        raw_dir=raw_dir,
        interim_dir=interim_dir,
        processed_dir=processed_dir,
        logs_dir=logs_dir,
        raw_overpass_dir=raw_dir / "overpass",
        raw_geocoding_dir=raw_dir / "geocoding",
        raw_sirene_dir=raw_dir / "sirene",
        processed_csv_path=processed_dir / "api_establishments.csv",
        processed_parquet_path=processed_dir / "api_establishments.parquet",
    )

    overpass = OverpassConfig(
        base_url=_get_env(
            "OVERPASS_URL",
            "https://overpass-api.de/api/interpreter",
        ),
        timeout_seconds=_get_int("OVERPASS_TIMEOUT_SECONDS", 120),
        sleep_seconds=float(_get_env("OVERPASS_SLEEP_SECONDS", "1.0")),
        max_retries=_get_int("OVERPASS_MAX_RETRIES", 3),
        default_timeout_clause_seconds=_get_int("OVERPASS_QUERY_TIMEOUT_SECONDS", 60),
        default_radius_meters=_get_int("OVERPASS_DEFAULT_RADIUS_METERS", 0),
        default_city=_get_env("TARGET_CITY", "Paris"),
        default_country=_get_env("TARGET_COUNTRY", "France"),
    )

    geocoding = GeocodingConfig(
        search_url=_get_env(
            "GEOCODING_SEARCH_URL",
            "https://data.geopf.fr/geocodage/search",
        ),
        reverse_url=_get_env(
            "GEOCODING_REVERSE_URL",
            "https://data.geopf.fr/geocodage/reverse",
        ),
        timeout_seconds=_get_int("GEOCODING_TIMEOUT_SECONDS", 30),
        max_retries=_get_int("GEOCODING_MAX_RETRIES", 3),
        limit=_get_int("GEOCODING_LIMIT", 1),
    )

    sirene = SireneConfig(
        base_url=_get_env(
            "SIRENE_BASE_URL",
            "https://entreprise.api.gouv.fr/v3/insee/sirene",
        ),
        token=_get_env("API_ENTREPRISE_TOKEN", ""),
        timeout_seconds=_get_int("SIRENE_TIMEOUT_SECONDS", 30),
        max_retries=_get_int("SIRENE_MAX_RETRIES", 3),
    )

    postgres = PostgresConfig(
        host=_get_env("POSTGRES_HOST", "localhost"),
        port=_get_int("POSTGRES_PORT", 5432),
        database=_get_env("POSTGRES_DB", "restaurant_wrapper"),
        user=_get_env("POSTGRES_USER", "postgres"),
        password=_get_env("POSTGRES_PASSWORD", "postgres"),
        echo_sql=_get_bool("POSTGRES_ECHO_SQL", False),
    )

    pipeline = PipelineConfig(
        target_city=_get_env("TARGET_CITY", "Paris"),
        target_country=_get_env("TARGET_COUNTRY", "France"),
        write_csv=_get_bool("WRITE_CSV", True),
        write_parquet=_get_bool("WRITE_PARQUET", True),
        save_raw_payloads=_get_bool("SAVE_RAW_PAYLOADS", True),
        log_level=_get_env("LOG_LEVEL", "INFO").upper(),
    )

    settings = Settings(
        paths=paths,
        overpass=overpass,
        geocoding=geocoding,
        sirene=sirene,
        postgres=postgres,
        pipeline=pipeline,
    )
    settings.paths.ensure_directories()
    return settings


settings = build_settings()