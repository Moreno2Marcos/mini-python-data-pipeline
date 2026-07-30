import pandas as pd
import pytest

from src.load import read_processed_data


def test_read_processed_data_success(tmp_path):
    processed_file_path = (
        tmp_path / "users_processed_test.csv"
    )

    expected_dataframe = pd.DataFrame(
        [
            {
                "user_id": 1,
                "name": "Test User",
                "email": "test@example.com",
            }
        ]
    )

    expected_dataframe.to_csv(
        processed_file_path,
        index=False,
    )

    result = read_processed_data(
        processed_file_path
    )

    assert len(result) == 1
    assert result.iloc[0]["user_id"] == 1
    assert result.iloc[0]["name"] == "Test User"


def test_read_processed_data_missing_file(tmp_path):
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(
        FileNotFoundError,
        match="Processed data file not found",
    ):
        read_processed_data(
            missing_file
        )


def test_read_processed_data_wrong_extension(
    tmp_path,
):
    invalid_file = tmp_path / "users.json"

    invalid_file.write_text(
        "[]",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Processed data file must be CSV",
    ):
        read_processed_data(
            invalid_file
        )


def test_read_processed_data_empty_csv(tmp_path):
    empty_file = tmp_path / "empty.csv"

    empty_file.write_text(
        "user_id,name,email\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Processed data file is empty",
    ):
        read_processed_data(
            empty_file
        )


def test_read_processed_data_zero_byte_file(
    tmp_path,
):
    empty_file = tmp_path / "empty.csv"

    empty_file.write_bytes(b"")

    with pytest.raises(
        ValueError,
        match="has no CSV content",
    ):
        read_processed_data(
            empty_file
        )


def test_read_processed_data_path_is_directory(
    tmp_path,
):
    directory_path = tmp_path / "users.csv"
    directory_path.mkdir()

    with pytest.raises(
        ValueError,
        match="path is not a file",
    ):
        read_processed_data(
            directory_path
        )
