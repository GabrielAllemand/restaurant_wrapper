from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def ensure_directory(path: Path) -> Path:
    """
    Crée un dossier s'il n'existe pas et retourne ce même chemin.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent_directory(path: Path) -> Path:
    """
    Crée le dossier parent d'un fichier s'il n'existe pas.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def utc_timestamp() -> str:
    """
    Retourne un timestamp UTC stable pour les noms de fichiers.
    Format : YYYYMMDDTHHMMSSZ
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_timestamped_filename(
    prefix: str,
    extension: str,
    *,
    suffix: str | None = None,
) -> str:
    """
    Construit un nom de fichier horodaté.
    Exemple :
    - overpass_20260315T101530Z.json
    - overpass_paris_20260315T101530Z.json
    """
    normalized_extension = extension.lstrip(".")
    timestamp = utc_timestamp()

    parts = [prefix]
    if suffix:
        parts.append(suffix)
    parts.append(timestamp)

    return f"{'_'.join(parts)}.{normalized_extension}"


def write_json(
    payload: Any,
    output_path: Path,
    *,
    indent: int = 2,
    ensure_ascii: bool = False,
) -> Path:
    """
    Écrit un objet Python en JSON.
    """
    ensure_parent_directory(output_path)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=indent, ensure_ascii=ensure_ascii, default=str)

    logger.info("JSON written to %s", output_path)
    return output_path


def read_json(input_path: Path) -> Any:
    """
    Lit un fichier JSON et retourne l'objet Python correspondant.
    """
    with input_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    logger.debug("JSON loaded from %s", input_path)
    return payload


def write_text(content: str, output_path: Path) -> Path:
    """
    Écrit un contenu texte dans un fichier UTF-8.
    """
    ensure_parent_directory(output_path)

    with output_path.open("w", encoding="utf-8") as file:
        file.write(content)

    logger.info("Text file written to %s", output_path)
    return output_path


def read_text(input_path: Path) -> str:
    """
    Lit un fichier texte UTF-8.
    """
    content = input_path.read_text(encoding="utf-8")
    logger.debug("Text file loaded from %s", input_path)
    return content


def write_dataframe_csv(
    dataframe: pd.DataFrame,
    output_path: Path,
    *,
    index: bool = False,
) -> Path:
    """
    Sauvegarde un DataFrame en CSV UTF-8.
    """
    ensure_parent_directory(output_path)
    dataframe.to_csv(output_path, index=index, encoding="utf-8")

    logger.info("CSV written to %s (%d rows, %d columns)", output_path, len(dataframe), len(dataframe.columns))
    return output_path


def write_dataframe_parquet(
    dataframe: pd.DataFrame,
    output_path: Path,
    *,
    index: bool = False,
) -> Path:
    """
    Sauvegarde un DataFrame en Parquet.
    """
    ensure_parent_directory(output_path)
    dataframe.to_parquet(output_path, index=index)

    logger.info(
        "Parquet written to %s (%d rows, %d columns)",
        output_path,
        len(dataframe),
        len(dataframe.columns),
    )
    return output_path


def read_dataframe_csv(input_path: Path, **kwargs: Any) -> pd.DataFrame:
    """
    Lit un CSV dans un DataFrame pandas.
    """
    dataframe = pd.read_csv(input_path, **kwargs)
    logger.debug(
        "CSV loaded from %s (%d rows, %d columns)",
        input_path,
        len(dataframe),
        len(dataframe.columns),
    )
    return dataframe


def read_dataframe_parquet(input_path: Path, **kwargs: Any) -> pd.DataFrame:
    """
    Lit un fichier Parquet dans un DataFrame pandas.
    """
    dataframe = pd.read_parquet(input_path, **kwargs)
    logger.debug(
        "Parquet loaded from %s (%d rows, %d columns)",
        input_path,
        len(dataframe),
        len(dataframe.columns),
    )
    return dataframe


def sanitize_filename(value: str) -> str:
    """
    Nettoie une chaîne pour l'utiliser dans un nom de fichier simple et stable.
    """
    normalized = value.strip().lower()
    allowed_chars = []

    for char in normalized:
        if char.isalnum():
            allowed_chars.append(char)
        elif char in {" ", "-", "_"}:
            allowed_chars.append("_")

    sanitized = "".join(allowed_chars)
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")

    return sanitized.strip("_") or "unnamed"