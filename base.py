from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from queron.runtime_models import ColumnMappingRecord

DEFAULT_QUERY_CHUNK_SIZE = 200


class PgConnectRequest(BaseModel):
    connection_id: str | None = None
    name: str | None = None
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    username: str = "postgres"
    password: str = ""
    url: str | None = None
    auth_mode: str | None = None
    sslmode: str | None = None
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None
    sslpassword: str | None = None
    connect_timeout_seconds: int | None = None
    statement_timeout_ms: int | None = None
    save_password: bool = True


class Db2ConnectRequest(BaseModel):
    connection_id: str | None = None
    name: str | None = None
    host: str = "localhost"
    port: int = 50000
    database: str = "SAMPLE"
    username: str = "db2inst1"
    password: str = ""
    url: str | None = None
    auth_mode: str | None = None
    ssl_server_certificate: str | None = None
    ssl_client_keystoredb: str | None = None
    ssl_client_keystash: str | None = None
    ssl_client_keystore_password: str | None = None
    ssl_client_label: str | None = None
    connect_timeout_seconds: int | None = None
    save_password: bool = True


class DuckDbConnectRequest(BaseModel):
    name: str | None = None
    database: str = ":memory:"
    url: str | None = None
    save_password: bool = True


class PgQueryRequest(BaseModel):
    connection_id: str
    sql: str
    chunk_size: int = Field(default=DEFAULT_QUERY_CHUNK_SIZE, ge=1, le=5000)


class PgQueryChunkRequest(BaseModel):
    chunk_size: int | None = Field(default=None, ge=1, le=5000)
    load_all: bool = False


class DuckDbIngestQueryResponse(BaseModel):
    pipeline_id: str
    target_table: str
    row_count: int
    source_type: str
    source_connection_id: str
    warnings: list[str] = Field(default_factory=list)
    column_mappings: list[ColumnMappingRecord] = Field(default_factory=list)
    created_at: float
    schema_changed: bool = False


class ConnectorEgressResponse(BaseModel):
    target_name: str
    row_count: int
    warnings: list[str] = Field(default_factory=list)
    created_at: float
    schema_changed: bool = False


class DuckDbExportResponse(BaseModel):
    output_path: str
    row_count: int
    file_size_bytes: int | None = None
    export_format: str
    warnings: list[str] = Field(default_factory=list)
    created_at: float


class TestConnectionResponse(BaseModel):
    success: bool
    message: str


class ConnectResponse(BaseModel):
    connection_id: str
    message: str


class ColumnMeta(BaseModel):
    name: str
    data_type: str
    source_table: str = ""
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    nullable: bool = True
    auto_increment: bool = False
    description: str = ""
    is_primary_key: bool = False
    is_unique: bool = False
    fk_ref_schema: str | None = None
    fk_ref_table: str | None = None
    fk_ref_column: str | None = None


class QueryResponse(BaseModel):
    columns: list[str]
    column_meta: list[ColumnMeta] = Field(default_factory=list)
    rows: list[dict[str, Any]]
    row_count: int
    query_session_id: str | None = None
    has_more: bool = False
    chunk_size: int | None = None
    mode: str | None = None
    message: str | None = None
    schema_changed: bool = False
