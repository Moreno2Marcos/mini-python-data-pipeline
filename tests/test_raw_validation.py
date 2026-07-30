import json

import pytest

from src.validate import validate_raw_users_artifact


VALID_USER = {
    "id": 1,
    "name": "Test User",
    "username": "testuser",
    "email": "test@example.com",
    "address": {
        "street": "Test Street",
        "suite": "Suite 1",
        "city": "Recife",
        "zipcode": "50000-000",
        "geo": {
            "lat": "0",
            "lng": "0",
        },
    },
    "phone": "0000-0000",
    "website": "example.com",
    "company": {
        "name": "Test Company",
        "catchPhrase": "Test",
        "bs": "testing",
    },
}


def create_raw_artifact(tmp_path, records):
    raw_file_path = tmp_path / "users_raw_test.json"

    raw_file_path.write_text(
        json.dumps(
            records,
            ensure_ascii=False,
            indent=4,
        ),
        encoding="utf-8",
    )

    raw_metadata = {
        "raw_file_path": str(raw_file_path),
        "record_count": len(records),
        "file_size_bytes": raw_file_path.stat().st_size,
    }

    return raw_file_path, raw_metadata


def test_validate_raw_users_artifact_success(tmp_path):
    raw_file_path, raw_metadata = create_raw_artifact(
        tmp_path,
        [VALID_USER],
    )

    result = validate_raw_users_artifact(
        raw_metadata
    )

    assert result == {
        "is_valid": True,
        "raw_file_path": str(raw_file_path),
        "record_count": 1,
    }


def test_validate_raw_users_artifact_missing_file(
    tmp_path,
):
    missing_file = tmp_path / "missing.json"

    raw_metadata = {
        "raw_file_path": str(missing_file),
        "record_count": 1,
        "file_size_bytes": 100,
    }

    with pytest.raises(
        FileNotFoundError,
        match="Raw data file not found",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_count_mismatch(
    tmp_path,
):
    _, raw_metadata = create_raw_artifact(
        tmp_path,
        [VALID_USER],
    )

    raw_metadata["record_count"] = 2

    with pytest.raises(
        ValueError,
        match="Raw record count mismatch",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_size_mismatch(
    tmp_path,
):
    _, raw_metadata = create_raw_artifact(
        tmp_path,
        [VALID_USER],
    )

    raw_metadata["file_size_bytes"] += 1

    with pytest.raises(
        ValueError,
        match="Raw file size mismatch",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_missing_metadata_field(
    tmp_path,
):
    _, raw_metadata = create_raw_artifact(
        tmp_path,
        [VALID_USER],
    )

    del raw_metadata["file_size_bytes"]

    with pytest.raises(
        ValueError,
        match="missing required fields",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_wrong_extension(
    tmp_path,
):
    raw_file_path = tmp_path / "users_raw_test.txt"

    raw_file_path.write_text(
        json.dumps([VALID_USER]),
        encoding="utf-8",
    )

    raw_metadata = {
        "raw_file_path": str(raw_file_path),
        "record_count": 1,
        "file_size_bytes": raw_file_path.stat().st_size,
    }

    with pytest.raises(
        ValueError,
        match="must be JSON",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_invalid_json(
    tmp_path,
):
    raw_file_path = tmp_path / "users_raw_invalid.json"

    raw_file_path.write_text(
        '{"id": 1',
        encoding="utf-8",
    )

    raw_metadata = {
        "raw_file_path": str(raw_file_path),
        "record_count": 1,
        "file_size_bytes": raw_file_path.stat().st_size,
    }

    with pytest.raises(
        ValueError,
        match="contains invalid JSON",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_root_is_not_list(
    tmp_path,
):
    raw_file_path = tmp_path / "users_raw_object.json"

    raw_file_path.write_text(
        json.dumps(VALID_USER),
        encoding="utf-8",
    )

    raw_metadata = {
        "raw_file_path": str(raw_file_path),
        "record_count": 1,
        "file_size_bytes": raw_file_path.stat().st_size,
    }

    with pytest.raises(
        ValueError,
        match="must contain a JSON list",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )


def test_validate_raw_users_artifact_empty_list(
    tmp_path,
):
    raw_file_path = tmp_path / "users_raw_empty.json"

    raw_file_path.write_text(
        json.dumps([]),
        encoding="utf-8",
    )

    raw_metadata = {
        "raw_file_path": str(raw_file_path),
        "record_count": 1,
        "file_size_bytes": raw_file_path.stat().st_size,
    }

    with pytest.raises(
        ValueError,
        match="está vazia",
    ):
        validate_raw_users_artifact(
            raw_metadata
        )
