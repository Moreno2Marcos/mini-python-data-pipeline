import logging

import requests


def extract_users(api_url, timeout_seconds):
    logging.info("Iniciando extração de dados da API.")
    logging.info(f"Endpoint consultado: {api_url}")

    headers = {
        "Accept": "application/json",
        "User-Agent": "mini-pipeline-python"
    }

    try:
        response = requests.get(
            api_url,
            headers=headers,
            timeout=timeout_seconds
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Resposta da API não está no formato esperado de lista.")

        logging.info(f"Extração concluída. Registros extraídos: {len(data)}")

        return data

    except requests.exceptions.Timeout as error:
        logging.error(f"Timeout ao consultar API: {error}")
        raise

    except requests.exceptions.ConnectionError as error:
        logging.error(f"Erro de conexão ao consultar API: {error}")
        raise

    except requests.exceptions.HTTPError as error:
        logging.error(f"Erro HTTP ao consultar API: {error}")
        raise

    except requests.exceptions.RequestException as error:
        logging.error(f"Erro inesperado na requisição HTTP: {error}")
        raise