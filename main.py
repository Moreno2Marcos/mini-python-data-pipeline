import logging
import sys

import pandas as pd

from src.config import API_TIMEOUT_SECONDS, API_URL, DB_PATH, LOGS_DIR, PROCESSED_DIR, PROJECT_ROOT, RAW_DIR
from src.extract import extract_users
from src.load import save_processed_data, save_raw_data
from src.logger_config import setup_logging
from src.transform import transform_users
from src.validate import validate_users
from src.database import create_users_table, count_users_in_database, load_users_to_database


def main():

    log_file_path = setup_logging(LOGS_DIR)

    logging.info("Mini-pipeline Python iniciado.")
    logging.info(f"Arquivo de log: {log_file_path}")
    logging.info(f"Pasta do projeto: {PROJECT_ROOT}")
    logging.info(f"Python: {sys.version.split()[0]}")
    logging.info(f"Pandas: {pd.__version__}")

    users = extract_users(API_URL, API_TIMEOUT_SECONDS)

    save_raw_data(users, RAW_DIR)

    validate_users(users)

    transformed_users = transform_users(users)

    df_users = pd.DataFrame(transformed_users)

    save_processed_data(df_users, PROCESSED_DIR)

    create_users_table(DB_PATH)

    load_users_to_database(df_users, DB_PATH)

    total_users_db = count_users_in_database(DB_PATH)

    logging.info(f"Total de registros na tabela users: {total_users_db}")

    logging.info("Mini-pipeline finalizado com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logging.exception(f"Pipeline finalizado com erro: {error}")
        raise