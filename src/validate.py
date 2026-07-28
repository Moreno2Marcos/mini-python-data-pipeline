import logging

from pathlib import Path

from src.contracts import RawMetadata, ValidationMetadata
from src.load import read_raw_data

def validate_users(users):
    logging.info("Iniciando validação dos dados brutos.")

    required_fields = ["id", "name", "email", "address", "company"]

    if not users:
        raise ValueError("A lista de usuários está vazia.")

    invalid_records = []

    for index, user in enumerate(users, start=1):
        missing_fields = []

        for field in required_fields:
            if field not in user:
                missing_fields.append(field)

        if missing_fields:
            invalid_records.append(
                {
                    "record_position": index,
                    "missing_fields": missing_fields,
                }
            )

    if invalid_records:
        raise ValueError(f"Registros inválidos encontrados: {invalid_records}")

    logging.info("Validação concluída com sucesso.")

    return True


def validate_raw_users_artifact(
    raw_metadata: RawMetadata,
) -> ValidationMetadata:
    """
    Valida os metadados e o arquivo JSON da camada raw
    e retorna os metadados do artefato validado.
    """
    required_fields = {
        "raw_file_path",
        "record_count",
        "file_size_bytes",
    }

    missing_fields = required_fields - raw_metadata.keys()

    if missing_fields:
        missing_fields_text = ", ".join(
            sorted(missing_fields)
        )

        raise ValueError(
            "Raw metadata is missing required fields: "
            f"{missing_fields_text}"
        )

    raw_path = Path(raw_metadata["raw_file_path"])
    expected_record_count = raw_metadata["record_count"]
    expected_file_size = raw_metadata["file_size_bytes"]

    if expected_record_count <= 0:
        raise ValueError(
            "Raw metadata record_count must be greater than zero."
        )

    if expected_file_size <= 0:
        raise ValueError(
            "Raw metadata file_size_bytes must be greater than zero."
        )

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw data file not found: {raw_path}"
        )

    if not raw_path.is_file():
        raise ValueError(
            f"Raw data path is not a file: {raw_path}"
        )

    if raw_path.suffix.lower() != ".json":
        raise ValueError(
            f"Raw data file must be JSON: {raw_path}"
        )

    actual_file_size = raw_path.stat().st_size

    if actual_file_size != expected_file_size:
        raise ValueError(
            "Raw file size mismatch: "
            f"metadata={expected_file_size}, "
            f"file={actual_file_size}"
        )

    extracted_users = read_raw_data(raw_path)

    validate_users(extracted_users)

    actual_record_count = len(extracted_users)

    if actual_record_count != expected_record_count:
        raise ValueError(
            "Raw record count mismatch: "
            f"metadata={expected_record_count}, "
            f"json={actual_record_count}"
        )

    return {
        "is_valid": True,
        "raw_file_path": str(raw_path),
        "record_count": actual_record_count,
    }
