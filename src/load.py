from datetime import datetime
import json
from pathlib import Path
import logging

import pandas as pd


def save_raw_data(data, output_dir):
    """
    Salva os registros extraídos em um arquivo JSON
    na camada raw e retorna o caminho do arquivo criado.
    """

    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"users_raw_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    logging.info(f"Arquivo bruto salvo em: {file_path}")

    return file_path


def save_processed_data(data, output_dir):
    """
    Converte os registros transformados em DataFrame
    e salva o resultado em um arquivo CSV.
    """

    if data is None:
        raise ValueError(
            "Nenhum dado foi recebido para persistência processada."
        )

    if not isinstance(data, list):
        raise TypeError(
            "save_processed_data deveria receber uma lista de dicionários."
        )

    if not data:
        raise ValueError(
            "A lista recebida para persistência está vazia."
        )

    if not all(isinstance(record, dict) for record in data):
        raise TypeError(
            "Todos os elementos recebidos devem ser dicionários."
        )

    df = pd.DataFrame(data)

    if df.empty:
        raise ValueError(
            "O DataFrame criado para persistência está vazio."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"users_processed_{timestamp}.csv"

    df.to_csv(
        file_path,
        index=False,
        encoding="utf-8-sig",
    )

    logging.info(
        "Arquivo processado salvo em: %s",
        file_path,
    )

    logging.info(
        "Registros gravados no CSV: %s",
        len(df),
    )

    return file_path


def read_raw_data(file_path: str | Path) -> list[dict]:
    """
    Lê um arquivo JSON da camada raw
    e retorna os registros como uma lista de dicionários.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Raw data file not found: {path}"
        )

    if not path.is_file():
        raise ValueError(
            f"Raw data path is not a file: {path}"
        )

    if path.suffix.lower() != ".json":
        raise ValueError(
            f"Raw data file must be JSON: {path}"
        )

    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list):
        raise ValueError(
            "Raw data must contain a JSON list of records."
        )

    return data
