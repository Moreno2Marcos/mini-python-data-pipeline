import logging


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