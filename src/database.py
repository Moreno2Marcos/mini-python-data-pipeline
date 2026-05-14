import logging
import sqlite3


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