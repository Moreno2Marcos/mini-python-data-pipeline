from typing import TypedDict


class RawMetadata(TypedDict):
    raw_file_path: str
    record_count: int
    file_size_bytes: int


class ValidationMetadata(TypedDict):
    is_valid: bool
    raw_file_path: str
    record_count: int
