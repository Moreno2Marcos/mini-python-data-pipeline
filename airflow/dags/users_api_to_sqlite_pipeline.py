import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import dag, task

from src.config import (
    API_TIMEOUT_SECONDS,
    API_URL,
    DB_PATH,
    PROCESSED_DIR,
    RAW_DIR,
)
from src.database import (
    count_users_in_database,
    create_users_table,
    load_users_to_database,
)
from src.extract import extract_users
from src.load import save_processed_data, save_raw_data, read_raw_data
from src.transform import transform_users
from src.validate import validate_users

from src.contracts import RawMetadata, ValidationMetadata


@dag(
    dag_id="users_api_to_sqlite_pipeline",
    description=(
        "Orquestra extração, validação, transformação "
        "e carga de usuários no SQLite."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "users", "sqlite", "taskflow"],
)
def users_api_to_sqlite_pipeline():
    """
    Extrai usuários da API JSONPlaceholder, preserva o JSON bruto,
    valida e transforma os registros, salva o CSV processado,
    carrega a tabela users no SQLite e valida o resultado final.
    """

    @task
    def extract_and_save_raw_task() -> RawMetadata:
        extracted_users = extract_users(API_URL, API_TIMEOUT_SECONDS)

        if not extracted_users:
            raise ValueError(
                "The API returned no users."
            )

        raw_file_path = save_raw_data(extracted_users, RAW_DIR)
        raw_path = Path(raw_file_path).resolve()

        if not raw_path.exists():
            raise FileNotFoundError(
                f"Raw file was not created: {raw_path}"
            )

        file_size_bytes = raw_path.stat().st_size

        if file_size_bytes <= 0:
            raise ValueError(
                f"Raw file is empty: {raw_path}"
            )

        raw_metadata: RawMetadata = {
            "raw_file_path": str(raw_path),
            "record_count": len(extracted_users),
            "file_size_bytes": file_size_bytes,
        }

        return raw_metadata


    @task
    def validate_users_task(
        raw_metadata: RawMetadata,
    ) -> ValidationMetadata:
        raw_file_path = raw_metadata["raw_file_path"]

        extracted_users = read_raw_data(raw_file_path)

        validate_users(extracted_users)

        actual_record_count = len(extracted_users)
        expected_record_count = raw_metadata["record_count"]

        if actual_record_count != expected_record_count:
            raise ValueError(
                "Raw record count mismatch: "
                f"metadata={expected_record_count}, "
                f"json={actual_record_count}"
            )

        raw_validation_metadata: ValidationMetadata = {
            "is_valid": True,
            "raw_file_path": raw_file_path,
            "record_count": actual_record_count,
        }

        return raw_validation_metadata


    @task
    def transform_users_task(
        validation_result: ValidationMetadata,
    ) -> list[dict]:
        if not validation_result["is_valid"]:
            raise ValueError(
                "Raw data was not validated."
            )

        raw_file_path = validation_result["raw_file_path"]
        extracted_users = read_raw_data(raw_file_path)

        processed_at = datetime.now(timezone.utc).isoformat()

        transformed_records = transform_users(
            extracted_users,
            processed_at,
        )

        return transformed_records


    @task(task_id="save_processed_data")
    def save_processed_data_task(
        transformed_records: list[dict],
    ) -> dict:
        """
        Salva os registros transformados em CSV e retorna
        metadados sobre o arquivo criado.
        """

        if not transformed_records:
            raise ValueError(
                "Nenhum registro transformado foi recebido para salvar."
            )

        if not isinstance(transformed_records, list):
            raise TypeError(
                "A persistência processada deveria receber uma lista."
            )

        if not all(
            isinstance(record, dict)
            for record in transformed_records
        ):
            raise TypeError(
                "A persistência processada deveria receber "
                "uma lista de dicionários."
            )

        logging.info(
            "Iniciando persistência processada. "
            "Registros recebidos: %s",
            len(transformed_records),
        )

        processed_file_path = save_processed_data(
            data=transformed_records,
            output_dir=PROCESSED_DIR,
        )

        if processed_file_path is None:
            raise ValueError(
                "A função save_processed_data não retornou "
                "o caminho do arquivo."
            )

        processed_path = Path(processed_file_path)

        if not processed_path.exists():
            raise FileNotFoundError(
                "O arquivo processado não foi encontrado: "
                f"{processed_path}"
            )

        if processed_path.suffix.lower() != ".csv":
            raise ValueError(
                "O arquivo processado não possui extensão CSV: "
                f"{processed_path}"
            )

        file_size_bytes = processed_path.stat().st_size

        if file_size_bytes == 0:
            raise ValueError(
                f"O arquivo CSV foi criado vazio: {processed_path}"
            )

        processed_metadata = {
            "processed_file_path": str(processed_path),
            "record_count": len(transformed_records),
            "file_size_bytes": file_size_bytes,
        }

        logging.info(
            "Persistência processada concluída. Arquivo: %s",
            processed_path,
        )

        logging.info(
            "Quantidade de registros persistidos: %s",
            len(transformed_records),
        )

        logging.info(
            "Tamanho do arquivo em bytes: %s",
            file_size_bytes,
        )

        return processed_metadata

    @task(task_id="create_users_table")
    def create_users_table_task() -> dict:
        """
        Cria a tabela users caso ela ainda não exista
        e retorna metadados de disponibilidade da tabela.
        """

        logging.info(
            "Iniciando criação ou verificação da tabela users."
        )

        logging.info(
            "Caminho configurado para o banco SQLite: %s",
            DB_PATH,
        )

        create_users_table(DB_PATH)

        if not DB_PATH.exists():
            raise FileNotFoundError(
                "O arquivo do banco SQLite não foi encontrado "
                f"depois da criação da tabela: {DB_PATH}"
            )

        table_metadata = {
            "db_path": str(DB_PATH),
            "table_name": "users",
            "table_ready": True,
        }

        logging.info(
            "Tabela users criada ou confirmada com sucesso."
        )

        return table_metadata

    @task(task_id="load_users_to_database")
    def load_users_to_database_task(
        transformed_records: list[dict],
        table_metadata: dict,
    ) -> dict:
        """
        Converte os registros transformados para DataFrame
        e executa a carga full refresh no SQLite.
        """

        if not table_metadata.get("table_ready"):
            raise ValueError(
                "A carga não pode continuar porque a tabela "
                "users não está disponível."
            )

        if not transformed_records:
            raise ValueError(
                "Nenhum registro transformado foi recebido "
                "para a carga no SQLite."
            )

        if not isinstance(transformed_records, list):
            raise TypeError(
                "A carga deveria receber uma lista."
            )

        if not all(
            isinstance(record, dict)
            for record in transformed_records
        ):
            raise TypeError(
                "A carga deveria receber uma lista de dicionários."
            )

        db_path_value = table_metadata.get("db_path")

        if not db_path_value:
            raise ValueError(
                "O metadado da tabela não contém o caminho do banco."
            )

        db_path = Path(db_path_value)

        if not db_path.exists():
            raise FileNotFoundError(
                f"O banco SQLite não foi encontrado: {db_path}"
            )

        logging.info(
            "Convertendo %s registros transformados para DataFrame.",
            len(transformed_records),
        )

        df_users = pd.DataFrame(transformed_records)

        if df_users.empty:
            raise ValueError(
                "O DataFrame criado para a carga está vazio."
            )

        expected_columns = [
            "user_id",
            "name",
            "email",
            "city",
            "zipcode",
            "latitude",
            "longitude",
            "company_name",
            "processed_at",
        ]

        missing_columns = [
            column
            for column in expected_columns
            if column not in df_users.columns
        ]

        if missing_columns:
            raise ValueError(
                "O DataFrame não contém todas as colunas esperadas. "
                f"Colunas ausentes: {missing_columns}"
            )

        df_users = df_users[expected_columns]

        logging.info(
            "Iniciando carga full refresh no SQLite."
        )

        logging.info(
            "A tabela users terá os registros existentes removidos "
            "antes da nova inserção."
        )

        load_users_to_database(
            df=df_users,
            db_path=db_path,
        )

        load_metadata = {
            "db_path": str(db_path),
            "table_name": table_metadata["table_name"],
            "records_sent_to_load": len(df_users),
            "load_strategy": "full_refresh",
        }

        logging.info(
            "Carga enviada ao SQLite. Registros enviados: %s",
            len(df_users),
        )

        return load_metadata

    @task(task_id="validate_database_load")
    def validate_database_load_task(
        processed_metadata: dict,
        load_metadata: dict,
    ) -> dict:
        """
        Compara a quantidade processada, enviada à carga
        e efetivamente armazenada no SQLite.
        """

        if not isinstance(processed_metadata, dict):
            raise TypeError(
                "Os metadados do arquivo processado deveriam "
                "ser um dicionário."
            )

        if not isinstance(load_metadata, dict):
            raise TypeError(
                "Os metadados da carga deveriam ser um dicionário."
            )

        processed_file_path = processed_metadata.get(
            "processed_file_path"
        )
        processed_record_count = processed_metadata.get(
            "record_count"
        )

        db_path_value = load_metadata.get("db_path")
        table_name = load_metadata.get("table_name")
        records_sent_to_load = load_metadata.get(
            "records_sent_to_load"
        )
        load_strategy = load_metadata.get("load_strategy")

        if not processed_file_path:
            raise ValueError(
                "Os metadados processados não contêm "
                "o caminho do arquivo CSV."
            )

        if processed_record_count is None:
            raise ValueError(
                "Os metadados processados não contêm "
                "a quantidade de registros."
            )

        if not db_path_value:
            raise ValueError(
                "Os metadados da carga não contêm "
                "o caminho do banco."
            )

        if not table_name:
            raise ValueError(
                "Os metadados da carga não contêm "
                "o nome da tabela."
            )

        if records_sent_to_load is None:
            raise ValueError(
                "Os metadados da carga não contêm "
                "a quantidade enviada ao banco."
            )

        processed_path = Path(processed_file_path)
        db_path = Path(db_path_value)

        if not processed_path.exists():
            raise FileNotFoundError(
                "O arquivo processado não foi encontrado durante "
                f"a validação final: {processed_path}"
            )

        if not db_path.exists():
            raise FileNotFoundError(
                "O banco SQLite não foi encontrado durante "
                f"a validação final: {db_path}"
            )

        if table_name != "users":
            raise ValueError(
                "A validação esperava a tabela users, "
                f"mas recebeu: {table_name}"
            )

        logging.info(
            "Iniciando validação final da carga."
        )

        logging.info(
            "Registros informados pelo processamento: %s",
            processed_record_count,
        )

        logging.info(
            "Registros enviados para a carga: %s",
            records_sent_to_load,
        )

        database_record_count = count_users_in_database(
            db_path
        )

        logging.info(
            "Registros encontrados no SQLite: %s",
            database_record_count,
        )

        if processed_record_count != records_sent_to_load:
            raise ValueError(
                "Falha de consistência: a quantidade processada "
                f"foi {processed_record_count}, mas a quantidade "
                f"enviada para a carga foi {records_sent_to_load}."
            )

        if records_sent_to_load != database_record_count:
            raise ValueError(
                "Falha na validação da carga: foram enviados "
                f"{records_sent_to_load} registros, mas o banco "
                f"contém {database_record_count}."
            )

        validation_metadata = {
            "pipeline_valid": True,
            "processed_file_path": str(processed_path),
            "processed_record_count": processed_record_count,
            "records_sent_to_load": records_sent_to_load,
            "database_record_count": database_record_count,
            "db_path": str(db_path),
            "table_name": table_name,
            "load_strategy": load_strategy,
        }

        logging.info(
            "Validação final concluída com sucesso."
        )

        logging.info(
            "Consistência confirmada: %s registros processados, "
            "carregados e encontrados no banco.",
            database_record_count,
        )

        return validation_metadata


    raw_metadata = extract_and_save_raw_task()

    validation_result = validate_users_task(
        raw_metadata
    )

    transformed_records = transform_users_task(
        validation_result
    )

    processed_metadata = save_processed_data_task(
        transformed_records
    )

    table_metadata = create_users_table_task()

    processed_metadata >> table_metadata

    load_metadata = load_users_to_database_task(
        transformed_records,
        table_metadata,
    )

    validate_database_load_task(
        processed_metadata,
        load_metadata,
    )


users_pipeline_dag = users_api_to_sqlite_pipeline()
