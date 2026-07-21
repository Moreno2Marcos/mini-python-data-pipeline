import logging

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="minimal_airflow_validation",
    description="Valida o carregamento e a execução básica do ambiente Airflow.",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    tags=["portfolio", "validation"],
)
def minimal_airflow_validation():

    @task(task_id="validate_airflow_execution")
    def validate_execution():
        logging.info("DAG mínima executada com sucesso.")
        logging.info(
            "O ambiente Airflow está preparado para iniciar a orquestração do pipeline."
        )

    validate_execution()


minimal_airflow_validation()