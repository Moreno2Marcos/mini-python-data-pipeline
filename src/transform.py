import logging


def transform_users(users, processed_at):
    logging.info("Iniciando transformação dos dados.")

    transformed_users = []

    for user in users:
        transformed_user = {
            "user_id": user["id"],
            "name": user["name"],
            "email": user["email"],
            "city": user["address"]["city"],
            "zipcode": user["address"]["zipcode"],
            "latitude": user["address"]["geo"]["lat"],
            "longitude": user["address"]["geo"]["lng"],
            "company_name": user["company"]["name"],
            "processed_at": processed_at,
        }

        transformed_users.append(transformed_user)

    logging.info(
        f"Transformação concluída. Registros transformados: {len(transformed_users)}"
    )

    return transformed_users
