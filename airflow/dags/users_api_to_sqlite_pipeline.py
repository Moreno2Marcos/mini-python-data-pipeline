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
    table_exists_in_database,
)
from src.extract import extract_users
from src.load import (
    read_processed_data,
    read_raw_data,
    save_processed_data,
    save_raw_data,
)
from src.transform import transform_users
from src.validate import (
    validate_processed_users_dataframe,
    validate_raw_users_artifact,
)
from src.contracts import (
    LoadMetadata,
    ProcessedMetadata,
    RawMetadata,
    TableMetadata,
    ValidationMetadata,
)


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
    def validate_raw_users_task(
        raw_metadata: RawMetadata,
    ) -> ValidationMetadata:
        """
        Orquestra a validação do artefato JSON da camada raw.
        """
        return validate_raw_users_artifact(
            raw_metadata
        )


    @task
    def transform_and_save_processed_task(
        validation_metadata: ValidationMetadata,
    ) -> ProcessedMetadata:
        """
        Lê o JSON raw validado, transforma os registros,
        salva o resultado em CSV e retorna seus metadados.
        """
        if not validation_metadata["is_valid"]:
            raise ValueError(
                "Raw data was not validated."
            )

        raw_file_path = validation_metadata["raw_file_path"]

        extracted_users = read_raw_data(
            raw_file_path
        )

        expected_record_count = validation_metadata["record_count"]
        actual_raw_record_count = len(extracted_users)

        if actual_raw_record_count != expected_record_count:
            raise ValueError(
                "Validated raw record count mismatch: "
                f"metadata={expected_record_count}, "
                f"json={actual_raw_record_count}"
            )

        processed_at = datetime.now(
            timezone.utc
        ).isoformat()

        transformed_records = transform_users(
            extracted_users,
            processed_at,
        )

        if not transformed_records:
            raise ValueError(
                "The transformation returned no records."
            )

        transformed_record_count = len(
            transformed_records
        )

        if transformed_record_count != actual_raw_record_count:
            raise ValueError(
                "Transformation record count mismatch: "
                f"raw={actual_raw_record_count}, "
                f"processed={transformed_record_count}"
            )

        processed_file_path = save_processed_data(
            transformed_records,
            PROCESSED_DIR,
        )

        processed_path = Path(
            processed_file_path
        ).resolve()

        if not processed_path.exists():
            raise FileNotFoundError(
                f"Processed file was not created: {processed_path}"
            )

        file_size_bytes = processed_path.stat().st_size

        if file_size_bytes <= 0:
            raise ValueError(
                f"Processed file is empty: {processed_path}"
            )

        return {
            "processed_file_path": str(processed_path),
            "source_raw_file_path": raw_file_path,
            "record_count": transformed_record_count,
            "file_size_bytes": file_size_bytes,
        }


    @task(task_id="create_users_table")
    def create_users_table_task() -> TableMetadata:
        """
        Cria a tabela users e confirma que o destino
        está disponível para receber a carga.
        """
        db_path = Path(DB_PATH).resolve()
        table_name = "users"

        create_users_table(
            db_path=db_path,
        )

        if not db_path.exists():
            raise FileNotFoundError(
                f"O banco SQLite não foi criado: {db_path}"
            )

        if not table_exists_in_database(
            db_path=db_path,
            table_name=table_name,
        ):
            raise ValueError(
                "A tabela users não foi encontrada "
                "após sua criação."
            )

        table_metadata: TableMetadata = {
            "db_path": str(db_path),
            "table_name": table_name,
            "table_ready": True,
        }

        logging.info(
            "Destino SQLite confirmado. Banco: %s. Tabela: %s.",
            db_path,
            table_name,
        )

        return table_metadata


    @task(task_id="load_users_to_database")
    def load_users_to_database_task(
        processed_metadata: ProcessedMetadata,
        table_metadata: TableMetadata,
    ) -> LoadMetadata:
        """
        Lê e valida o CSV processado e executa
        a carga full refresh na tabela users.
        """
        if not table_metadata.get("table_ready"):
            raise ValueError(
                "A carga não pode continuar porque "
                "a tabela users não está disponível."
            )

        processed_file_path_value = processed_metadata.get(
            "processed_file_path"
        )

        if not processed_file_path_value:
            raise ValueError(
                "Os metadados processados não contêm "
                "o caminho do arquivo CSV."
            )

        expected_record_count = processed_metadata.get(
            "record_count"
        )

        if not isinstance(expected_record_count, int):
            raise TypeError(
                "O campo record_count deve ser um número inteiro."
            )

        processed_file_path = Path(
            processed_file_path_value
        )

        logging.info(
            "Lendo arquivo processado para carga: %s",
            processed_file_path,
        )

        df_users = read_processed_data(
            processed_file_path
        )

        df_users = validate_processed_users_dataframe(
            dataframe=df_users,
            expected_record_count=expected_record_count,
        )

        db_path_value = table_metadata.get("db_path")
        table_name = table_metadata.get("table_name")

        if not db_path_value:
            raise ValueError(
                "Os metadados da tabela não contêm "
                "o caminho do banco."
            )

        if not table_name:
            raise ValueError(
                "Os metadados da tabela não contêm "
                "o nome da tabela."
            )

        db_path = Path(db_path_value)

        if not db_path.exists():
            raise FileNotFoundError(
                f"O banco SQLite não foi encontrado: {db_path}"
            )

        if not table_exists_in_database(
            db_path=db_path,
            table_name=table_name,
        ):
            raise ValueError(
                f"A tabela {table_name} não existe no SQLite."
            )

        records_sent_to_load = len(df_users)

        logging.info(
            "Iniciando carga full refresh de %s registros.",
            records_sent_to_load,
        )

        load_users_to_database(
            df=df_users,
            db_path=db_path,
        )

        load_metadata: LoadMetadata = {
            "db_path": str(db_path),
            "table_name": table_name,
            "processed_file_path": str(
                processed_file_path
            ),
            "records_sent_to_load": records_sent_to_load,
            "load_strategy": "full_refresh",
        }

        logging.info(
            "Carga full refresh concluída. "
            "Registros enviados: %s.",
            records_sent_to_load,
        )

        return load_metadata


    @task(task_id="validate_database_load")
    def validate_database_load_task(
        processed_metadata: ProcessedMetadata,
        load_metadata: LoadMetadata,
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

    validation_metadata = validate_raw_users_task(
        raw_metadata
    )

    processed_metadata = transform_and_save_processed_task(
        validation_metadata
    )

    table_metadata = create_users_table_task()

    processed_metadata >> table_metadata

    load_metadata = load_users_to_database_task(
        processed_metadata,
        table_metadata,
    )

    validate_database_load_task(
        processed_metadata,
        load_metadata,
    )


users_pipeline_dag = users_api_to_sqlite_pipeline()
