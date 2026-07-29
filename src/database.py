import logging
import sqlite3
from pathlib import Path


def create_users_table(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                city TEXT,
                zipcode TEXT,
                latitude TEXT,
                longitude TEXT,
                company_name TEXT,
                processed_at TEXT NOT NULL
            )
            """
        )

        connection.commit()

    logging.info("Tabela users criada ou já existente no banco SQLite.")

def load_users_to_database(df, db_path):
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()

        cursor.execute("DELETE FROM users")

        df.to_sql(
            name="users",
            con=connection,
            if_exists="append",
            index=False
        )

        connection.commit()

    logging.info(f"Dados carregados na tabela users. Registros carregados: {len(df)}")

def count_users_in_database(db_path):
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM users")
        result = cursor.fetchone()

    return result[0]


def table_exists_in_database(
    db_path: str | Path,
    table_name: str,
) -> bool:
    """
    Verifica se uma tabela existe no banco SQLite.
    """
    database_path = Path(db_path)

    if not database_path.exists():
        return False

    with sqlite3.connect(database_path) as connection:
        result = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()

    return result is not None
