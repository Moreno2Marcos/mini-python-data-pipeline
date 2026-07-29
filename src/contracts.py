from typing import Literal, TypedDict


class RawMetadata(TypedDict):
    raw_file_path: str
    record_count: int
    file_size_bytes: int


class ValidationMetadata(TypedDict):
    is_valid: bool
    raw_file_path: str
    record_count: int


class ProcessedMetadata(TypedDict):
    processed_file_path: str
    source_raw_file_path: str
    record_count: int
    file_size_bytes: int


class TableMetadata(TypedDict):
    db_path: str
    table_name: str
    table_ready: bool


class LoadMetadata(TypedDict):
    db_path: str
    table_name: str
    processed_file_path: str
    records_sent_to_load: int
    load_strategy: Literal["full_refresh"]
