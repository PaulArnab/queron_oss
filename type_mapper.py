"""
Shared type normalization and DuckDB mapping helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


@dataclass
class MappingPolicy:
    strict: bool = False
    enum_as_varchar: bool = True
    json_mode: str = "varchar"  # varchar | json
    timestamp_tz_mode: str = "preserve"  # preserve | utc_timestamp
    max_decimal_precision: int = 38
    on_overflow: str = "double"  # fail | double | varchar
    on_cast_error: str = "widen_to_varchar"  # fail | null | widen_to_varchar


@dataclass
class TypeMappingResult:
    canonical_type: str
    duckdb_type: str
    nullable: bool = True
    warnings: list[str] | None = None
    lossy: bool = False


@dataclass
class MappedColumn:
    name: str
    source_type: str
    canonical_type: str
    duckdb_type: str
    nullable: bool = True
    warnings: list[str] | None = None
    lossy: bool = False


_DECIMAL_WITH_ARGS_RE = re.compile(r"^(?:numeric|decimal)\s*\((\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
_DBAPI_TYPE_OBJECT_RE = re.compile(r"^DBAPITYPEOBJECT\((.+)\)$", re.IGNORECASE)


def _normalize_source_name(source: str) -> str:
    s = (source or "").strip().lower()
    if s in {"postgresql", "pg"}:
        return "postgres"
    return s


def normalize_display_type(source: str, raw_type: str) -> str:
    src = _normalize_source_name(source)
    raw = (raw_type or "").strip()
    if not raw:
        return raw

    if src == "postgres":
        lower = raw.lower()
        canonical_map = {
            "boolean": "BOOLEAN",
            "smallint": "SMALLINT",
            "integer": "INTEGER",
            "bigint": "BIGINT",
            "real": "REAL",
            "double precision": "DOUBLE PRECISION",
            "money": "MONEY",
            "text": "TEXT",
            "json": "JSON",
            "jsonb": "JSONB",
            "uuid": "UUID",
            "date": "DATE",
            "time": "TIME",
            "timetz": "TIMETZ",
            "timestamp": "TIMESTAMP",
            "timestamptz": "TIMESTAMPTZ",
            "numeric": "NUMERIC",
            "decimal": "DECIMAL",
            "enum": "ENUM",
            "bytea": "BYTEA",
        }
        if lower in canonical_map:
            return canonical_map[lower]
        if lower == "timestamp with time zone":
            return "TIMESTAMPTZ"
        if lower == "timestamp without time zone":
            return "TIMESTAMP"
        if lower == "time with time zone":
            return "TIMETZ"
        if lower == "time without time zone":
            return "TIME"
        if lower.startswith("character varying"):
            return "VARCHAR" + raw[len("character varying"):]
        if lower == "character varying":
            return "VARCHAR"
        if lower.startswith("character("):
            return "CHAR" + raw[len("character"):]
        if lower == "character":
            return "CHAR"
        if lower.startswith("numeric("):
            return "NUMERIC" + raw[len("numeric"):]
        if lower.startswith("decimal("):
            return "DECIMAL" + raw[len("decimal"):]
        return raw

    if src == "db2":
        match = _DBAPI_TYPE_OBJECT_RE.match(raw)
        if match:
            tokens = {token.strip().upper() for token in re.findall(r"'([^']+)'", match.group(1)) if token.strip()}
            if "BIGINT" in tokens:
                return "BIGINT"
            if "INTEGER" in tokens or "INT" in tokens:
                return "INTEGER"
            if "SMALLINT" in tokens:
                return "SMALLINT"
            if "DECIMAL" in tokens or "NUMERIC" in tokens:
                return "DECIMAL"
            if "DECFLOAT" in tokens:
                return "DECFLOAT"
            if "DOUBLE" in tokens or "DOUBLE PRECISION" in tokens:
                return "DOUBLE"
            if "REAL" in tokens:
                return "REAL"
            if "TIMESTAMP" in tokens:
                return "TIMESTAMP"
            if "DATE" in tokens:
                return "DATE"
            if "TIME" in tokens:
                return "TIME"
            if "VARCHAR" in tokens or "CHARACTER VARYING" in tokens or "CHAR VARYING" in tokens or "STRING" in tokens:
                return "VARCHAR"
            if "CHARACTER" in tokens or "CHAR" in tokens:
                return "CHAR"
            if "GRAPHIC" in tokens or "VARGRAPHIC" in tokens:
                return "VARCHAR"
            if "BLOB" in tokens or "BINARY" in tokens or "VARBINARY" in tokens:
                return "BLOB"
            if "BOOLEAN" in tokens or "BOOL" in tokens:
                return "BOOLEAN"
        upper = raw.upper()
        if upper.startswith("CHARACTER VARYING"):
            return "VARCHAR" + raw[len("CHARACTER VARYING"):]
        if upper.startswith("CHARACTER"):
            return "CHAR" + raw[len("CHARACTER"):]
        if upper.startswith("TIMESTAMP"):
            return "TIMESTAMP"
        if upper.startswith("DOUBLE"):
            return "DOUBLE"
        if upper.startswith("REAL"):
            return "REAL"
        if upper.startswith("DECIMAL"):
            return upper
        if upper.startswith("NUMERIC"):
            return upper
        if upper.startswith("INTEGER"):
            return "INTEGER"
        if upper.startswith("SMALLINT"):
            return "SMALLINT"
        if upper.startswith("BIGINT"):
            return "BIGINT"
        return upper

    return raw.upper()


def normalize_source_type(
    source: str,
    raw_type: str,
    *,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    src = _normalize_source_name(source)
    normalized = normalize_display_type(src, raw_type).strip()
    lower = normalized.lower()

    if lower in {"bool", "boolean"}:
        return "bool"
    if lower in {"smallint", "int2"}:
        return "int16"
    if lower in {"integer", "int", "int4"}:
        return "int32"
    if lower in {"bigint", "int8"}:
        return "int64"
    if lower in {"real", "float4"}:
        return "float32"
    if lower in {"double", "double precision", "float8"}:
        return "float64"
    if lower in {"date"}:
        return "date"
    if lower in {"time", "timetz"}:
        return "time"
    if lower in {"timestamp"}:
        return "timestamp"
    if lower in {"timestamptz", "timestamp with time zone"}:
        return "timestamptz"
    if lower in {"json", "jsonb"}:
        return "json"
    if lower in {"bytea", "blob", "binary", "varbinary"}:
        return "binary"
    if lower in {"uuid", "enum", "text"}:
        return "string"
    if lower.endswith("[]"):
        return "array"
    if lower.startswith("varchar") or lower.startswith("char") or lower.startswith("character"):
        return "string"
    if lower.startswith("decimal") or lower.startswith("numeric"):
        m = _DECIMAL_WITH_ARGS_RE.match(lower)
        if m:
            return f"decimal({int(m.group(1))},{int(m.group(2))})"
        if isinstance(precision, int):
            s = int(scale or 0)
            return f"decimal({precision},{s})"
        return "decimal"

    if src == "db2" and lower.startswith("graphic"):
        return "string"

    if length is not None and lower == "":
        return "string"

    return "unknown"


def canonical_to_duckdb(canonical_type: str, policy: MappingPolicy | None = None) -> TypeMappingResult:
    p = policy or MappingPolicy()
    canonical = (canonical_type or "unknown").strip().lower()
    warnings: list[str] = []
    lossy = False

    if canonical == "bool":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="BOOLEAN")
    if canonical == "int16":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="SMALLINT")
    if canonical == "int32":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="INTEGER")
    if canonical == "int64":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="BIGINT")
    if canonical == "float32":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="REAL")
    if canonical == "float64":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="DOUBLE")
    if canonical == "string":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR")
    if canonical == "date":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="DATE")
    if canonical == "time":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIME")
    if canonical == "timestamp":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIMESTAMP")
    if canonical == "timestamptz":
        if p.timestamp_tz_mode == "utc_timestamp":
            warnings.append("TIMESTAMPTZ will be normalized to UTC TIMESTAMP.")
            return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIMESTAMP", warnings=warnings)
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIMESTAMPTZ")
    if canonical == "json":
        if p.json_mode == "json":
            return TypeMappingResult(canonical_type=canonical_type, duckdb_type="JSON")
        warnings.append("JSON mapped to VARCHAR by policy.")
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR", warnings=warnings, lossy=True)
    if canonical == "binary":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="BLOB")

    m = re.match(r"^decimal\((\d+),(\d+)\)$", canonical)
    if canonical == "decimal" or m:
        if m:
            precision = int(m.group(1))
            scale = int(m.group(2))
        else:
            precision = 38
            scale = 10
            warnings.append("DECIMAL without explicit precision/scale defaulted to DECIMAL(38,10).")

        if precision <= p.max_decimal_precision:
            return TypeMappingResult(
                canonical_type=canonical_type,
                duckdb_type=f"DECIMAL({precision},{scale})",
                warnings=warnings or None,
            )

        lossy = True
        warnings.append(
            f"DECIMAL precision {precision} exceeds max {p.max_decimal_precision}. "
            f"Applying overflow policy '{p.on_overflow}'."
        )
        if p.on_overflow == "fail":
            return TypeMappingResult(canonical_type=canonical_type, duckdb_type="INVALID", warnings=warnings, lossy=lossy)
        if p.on_overflow == "varchar":
            return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR", warnings=warnings, lossy=lossy)
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="DOUBLE", warnings=warnings, lossy=lossy)

    if canonical in {"array", "struct", "unknown"}:
        warnings.append(f"Canonical type '{canonical}' mapped to VARCHAR.")
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR", warnings=warnings, lossy=True)

    warnings.append(f"Unrecognized canonical type '{canonical}' mapped to VARCHAR.")
    return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR", warnings=warnings, lossy=True)


def map_column_to_duckdb(
    *,
    name: str,
    source: str,
    raw_type: str,
    nullable: bool = True,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
    policy: MappingPolicy | None = None,
) -> MappedColumn:
    canonical = normalize_source_type(source, raw_type, length=length, precision=precision, scale=scale)
    mapped = canonical_to_duckdb(canonical, policy=policy)
    return MappedColumn(
        name=name,
        source_type=raw_type,
        canonical_type=canonical,
        duckdb_type=mapped.duckdb_type,
        nullable=nullable,
        warnings=mapped.warnings,
        lossy=mapped.lossy,
    )


def map_columns_to_duckdb(
    columns: list[Any],
    *,
    source: str,
    policy: MappingPolicy | None = None,
) -> list[MappedColumn]:
    mapped: list[MappedColumn] = []
    for col in columns:
        if isinstance(col, dict):
            name = str(col.get("name", "column"))
            raw_type = str(col.get("data_type") or col.get("type") or "UNKNOWN")
            nullable = bool(col.get("nullable", True))
            length = col.get("max_length", col.get("length"))
            precision = col.get("precision")
            scale = col.get("scale")
        else:
            name = str(getattr(col, "name", "column"))
            raw_type = str(getattr(col, "data_type", "UNKNOWN"))
            nullable = bool(getattr(col, "nullable", True))
            length = getattr(col, "max_length", None)
            precision = getattr(col, "precision", None)
            scale = getattr(col, "scale", None)

        mapped.append(
            map_column_to_duckdb(
                name=name,
                source=source,
                raw_type=raw_type,
                nullable=nullable,
                length=length if isinstance(length, int) else None,
                precision=precision if isinstance(precision, int) else None,
                scale=scale if isinstance(scale, int) else None,
                policy=policy,
            )
        )
    return mapped


def build_duckdb_create_table_sql(table_name: str, columns: list[MappedColumn], *, replace: bool = True) -> str:
    def q(identifier: str) -> str:
        return '"' + str(identifier).replace('"', '""') + '"'

    def q_table(identifier: str) -> str:
        parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
        if not parts:
            raise RuntimeError("Target table name is required.")
        return ".".join(q(part) for part in parts)

    mode = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    col_defs: list[str] = []
    for col in columns:
        null_part = "" if col.nullable else " NOT NULL"
        col_defs.append(f"{q(col.name)} {col.duckdb_type}{null_part}")
    return f"{mode} {q_table(table_name)} ({', '.join(col_defs)})"
