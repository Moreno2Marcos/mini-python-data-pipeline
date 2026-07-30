import pandas as pd
import pytest

from src.database import (
    count_users_in_database,
    create_users_table,
    load_users_to_database,
    table_exists_in_database,
)
from src.validate import (
    EXPECTED_PROCESSED_COLUMNS,
    validate_processed_users_dataframe,
)


def create_valid_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "user_id": 1,
                "name": "User One",
                "email": "user1@example.com",
                "city": "Recife",
                "zipcode": "50000-000",
                "latitude": "-8.0000",
                "longitude": "-34.0000",
                "company_name": "Company One",
                "processed_at": "2026-07-29T12:00:00+00:00",
            },
            {
                "user_id": 2,
                "name": "User Two",
                "email": "user2@example.com",
                "city": "Olinda",
                "zipcode": "53000-000",
                "latitude": "-7.0000",
                "longitude": "-34.0000",
                "company_name": "Company Two",
                "processed_at": "2026-07-29T12:00:00+00:00",
            },
        ]
    )


def test_validate_processed_dataframe_success():
    dataframe = create_valid_dataframe()

    result = validate_processed_users_dataframe(
        dataframe=dataframe,
        expected_record_count=2,
    )

    assert len(result) == 2
    assert list(result.columns) == EXPECTED_PROCESSED_COLUMNS


def test_validate_processed_dataframe_count_mismatch():
    dataframe = create_valid_dataframe()

    with pytest.raises(
        ValueError,
        match="não corresponde",
    ):
        validate_processed_users_dataframe(
            dataframe=dataframe,
            expected_record_count=3,
        )


def test_validate_processed_dataframe_missing_column():
    dataframe = create_valid_dataframe().drop(
        columns=["company_name"]
    )

    with pytest.raises(
        ValueError,
        match="Colunas ausentes",
    ):
        validate_processed_users_dataframe(
            dataframe=dataframe,
            expected_record_count=2,
        )


def test_validate_processed_dataframe_duplicate_user_id():
    dataframe = create_valid_dataframe()
    dataframe.loc[1, "user_id"] = 1

    with pytest.raises(
        ValueError,
        match="valores duplicados",
    ):
        validate_processed_users_dataframe(
            dataframe=dataframe,
            expected_record_count=2,
        )


def test_create_users_table_confirms_destination(
    tmp_path,
):
    db_path = tmp_path / "test_pipeline.db"

    create_users_table(
        db_path=db_path,
    )

    assert table_exists_in_database(
        db_path=db_path,
        table_name="users",
    )


def test_full_refresh_load_is_idempotent(tmp_path):
    db_path = tmp_path / "test_pipeline.db"
    dataframe = create_valid_dataframe()

    create_users_table(
        db_path=db_path,
    )

    load_users_to_database(
        df=dataframe,
        db_path=db_path,
    )

    first_count = count_users_in_database(
        db_path
    )

    load_users_to_database(
        df=dataframe,
        db_path=db_path,
    )

    second_count = count_users_in_database(
        db_path
    )

    assert first_count == 2
    assert second_count == 2


def test_validate_processed_dataframe_null_user_id():
    dataframe = create_valid_dataframe()
    dataframe.loc[1, "user_id"] = None

    with pytest.raises(
        ValueError,
        match="valores nulos",
    ):
        validate_processed_users_dataframe(
            dataframe=dataframe,
            expected_record_count=2,
        )


def test_validate_processed_dataframe_invalid_expected_count():
    dataframe = create_valid_dataframe()

    with pytest.raises(
        ValueError,
        match="maior que zero",
    ):
        validate_processed_users_dataframe(
            dataframe=dataframe,
            expected_record_count=0,
        )


def test_load_users_to_database_propagates_write_failure(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "test_pipeline.db"
    dataframe = create_valid_dataframe()

    create_users_table(
        db_path=db_path,
    )

    def raise_simulated_write_failure(
        self,
        *args,
        **kwargs,
    ):
        raise RuntimeError(
            "Simulated SQLite write failure."
        )

    monkeypatch.setattr(
        pd.DataFrame,
        "to_sql",
        raise_simulated_write_failure,
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated SQLite write failure",
    ):
        load_users_to_database(
            df=dataframe,
            db_path=db_path,
        )
