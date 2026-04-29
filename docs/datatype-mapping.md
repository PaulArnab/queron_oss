# Datatype Mapping

Queron records datatype mappings during ingress and egress. These mappings are exposed in node inspection and persisted in the artifact database.

The mapping record answers two practical questions:

- When a source column entered a DuckDB artifact, what DuckDB type did Queron choose?
- When a DuckDB artifact was written to another system, what target type did Queron choose?

Mappings are visible through `queron.inspect_node(...)` and the `queron inspect_node ...` CLI. For egress nodes, Queron stores the mapping against the local artifact table for that node so inspect can show how the exported columns were typed.

Each mapping records:

| Field | Description |
|---|---|
| `source_column` | Original source column name. |
| `source_type` | Type reported by source driver or DuckDB query. |
| `target_column` | Target column name. |
| `target_type` | Type used in DuckDB or the egress target. |
| `connector_type` | Connector name, for example `postgres`, `db2`, `oracle`. |
| `mapping_mode` | Mapping strategy. |
| `warnings` | Mapping warnings. |
| `lossy` | Whether the mapping can lose type semantics. |

Example inspect shape:

```json
{
  "source_column": "premium_amount",
  "source_type": "NUMERIC(12,2)",
  "target_column": "premium_amount",
  "target_type": "DECIMAL(12,2)",
  "connector_type": "postgres",
  "mapping_mode": "ingress",
  "warnings": [],
  "lossy": false
}
```

## Mapping Modes

| Mode | Meaning |
|---|---|
| `python_mapped` | Connector read source metadata and created the DuckDB table explicitly. |
| `ingress` | Connector-specific ingress mapping path. |
| `adbc_passthrough` | Transport path provided DuckDB-compatible data and metadata. |
| `egress_inferred` | Egress target type was inferred from DuckDB query result metadata. |
| `egress_remote_schema` | Existing remote table schema was inspected and used. |

When egress mode appends to or uses an existing remote table, `egress_remote_schema` takes precedence over inferred mapping.

## Egress Mode Behavior

The target datatype depends on the egress mode and whether the remote table already exists.

| Egress mode | Remote table state | Datatype behavior |
|---|---|---|
| `replace` | Table may or may not exist. | Queron drops/recreates or replaces the target table using types inferred from the DuckDB result. The mapping mode is usually `egress_inferred`. |
| `append` | Table must already exist. | Queron does not change the remote table datatype. It reads the existing remote schema and records `egress_remote_schema`. |
| `append` | Incoming value does not fit the existing remote type. | The load fails at the connector/database layer. Queron records the failure instead of widening or changing the target column. |
| `replace` with remote schema inspection available | Existing table can be inspected before write. | Queron may record the inspected target type as `egress_remote_schema`, but the write is still allowed to replace the table depending on connector behavior. |

In simple terms:

- `replace` means Queron is allowed to choose the target table shape from the artifact data.
- `append` means the target table shape already owns the contract, so Queron must fit the artifact data into that existing schema.
- If append data is incompatible, for example a DuckDB `VARCHAR` value is too long for a remote `VARCHAR(20)`, Queron should not silently widen the column. The run fails so the user can fix the target schema or source data.

## Egress String Lengths

For egress to DB2, MSSQL, MySQL, MariaDB, and Oracle, Queron measures `VARCHAR` result values before creating a new target table when it can. This avoids creating maximum-size text columns for short values.

Measured string mappings use padding so the target column has room for later values that are slightly longer than the rows seen in the current run.

The padding rule is:

```text
target_length = measured_max_length * 2
```

For example, if the longest value in the DuckDB result is 18 characters, Queron creates a 36-character target column when the connector supports that size:

```text
measured_max_length = 18
target_length = 18 * 2 = 36
```

This padding is only used when Queron is creating or replacing a target table from the current result shape. It does not apply to append mode, because append mode must use the existing remote table schema.

If the padded length is too large for the target database, Queron falls back to that connector's large text type or maximum supported bounded type and records a warning.

| Target | Measured `VARCHAR` length 18 | No measured length | Oversized measured length |
|---|---|---|---|
| DB2 | `VARCHAR(36)` | `VARCHAR(32672)` with warning | `VARCHAR(32672)` with warning |
| MSSQL | `NVARCHAR(36)` | `NVARCHAR(MAX)` with warning | `NVARCHAR(MAX)` with warning |
| MySQL | `VARCHAR(36)` | `LONGTEXT` with warning | `LONGTEXT` with warning |
| MariaDB | `VARCHAR(36)` | `LONGTEXT` with warning | `LONGTEXT` with warning |
| Oracle | `VARCHAR2(36)` | `VARCHAR2(4000)` with warning | `CLOB` with warning |

If Queron can inspect an existing remote table, the remote column type is recorded as `egress_remote_schema` and stale fallback warnings are cleared.

## PostgreSQL

### PostgreSQL Ingress to DuckDB

| PostgreSQL source type | DuckDB type | Notes |
|---|---|---|
| `boolean`, `bool` | `BOOLEAN` | |
| `smallint`, `int2` | `SMALLINT` | |
| `integer`, `int`, `int4` | `INTEGER` | |
| `bigint`, `int8` | `BIGINT` | |
| `real`, `float4` | `REAL` | |
| `double precision`, `float8`, `double` | `DOUBLE` | |
| `date` | `DATE` | |
| `time`, `time without time zone` | `TIME` | |
| `timetz`, `time with time zone` | `TIME` | Lossy warning. |
| `timestamp`, `timestamp without time zone` | `TIMESTAMP` | |
| `timestamptz`, `timestamp with time zone` | `TIMESTAMPTZ` | |
| `interval` | `INTERVAL` | |
| `uuid` | `UUID` | |
| `bytea` | `BLOB` | |
| `numeric(p,s)`, `decimal(p,s)` | `DECIMAL(p,s)` | If precision <= 38. |
| `numeric(p,s)`, `decimal(p,s)` | `VARCHAR` | If precision > 38; lossy warning. |
| `numeric`, `decimal` | `DECIMAL(38,10)` | Default precision and scale. |
| PostgreSQL arrays | `VARCHAR` | Lossy warning. |
| `json`, `jsonb` | `VARCHAR` | Lossy warning. |
| `character varying`, `varchar`, `character`, `char` | `VARCHAR` | |
| `text`, `citext`, `name`, `xml` | `VARCHAR` | |
| unknown | `VARCHAR` | Lossy warning. |

### DuckDB to PostgreSQL Egress

| DuckDB source type | PostgreSQL target type | Notes |
|---|---|---|
| `BOOLEAN` | `BOOLEAN` | |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INTEGER` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `REAL` | |
| `DOUBLE` | `DOUBLE PRECISION` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `TIMESTAMP` | `TIMESTAMP` | |
| `TIMESTAMPTZ` | `TIMESTAMPTZ` | |
| `UUID` | `UUID` | |
| `INTERVAL` | `INTERVAL` | |
| `VARCHAR`, `VARCHAR(n)` | `TEXT` | |
| `JSON` | `TEXT` | |
| `BLOB`, `BYTEA` | `BYTEA` | |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | |
| `DECIMAL` | `NUMERIC(precision,scale)` | Uses metadata or defaults. |
| unknown | `TEXT` | Warning. |

## DB2

### DB2 Ingress to DuckDB

| DB2 source type | DuckDB type | Notes |
|---|---|---|
| `BOOLEAN`, `BOOL` | `BOOLEAN` | |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER`, `INT` | `INTEGER` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `REAL` | |
| `DOUBLE`, `DOUBLE PRECISION`, `FLOAT` | `DOUBLE` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `TIMESTAMP...` | `TIMESTAMP` | Any timestamp prefix. |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `DECIMAL(p,s)` | If precision <= 38. |
| decimal precision > 38 | `DOUBLE` | Lossy warning. |
| decimal without precision/scale | `DECIMAL(38,10)` | Warning. |
| `DECFLOAT` | `DOUBLE` | |
| `CHAR`, `VARCHAR`, `CLOB`, `DBCLOB` | `VARCHAR` | |
| `GRAPHIC`, `VARGRAPHIC` | `VARCHAR` | |
| `XML` | `VARCHAR` | |
| `BLOB`, `BINARY`, `VARBINARY` | `BLOB` | |
| unknown | `VARCHAR` | Lossy warning. |

### DuckDB to DB2 Egress

| DuckDB source type | DB2 target type | Notes |
|---|---|---|
| `BOOLEAN`, `BOOL` | `BOOLEAN` | |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INTEGER` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `REAL` | |
| `DOUBLE` | `DOUBLE` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `TIMESTAMP` | `TIMESTAMP` | |
| `TIMESTAMPTZ` | `TIMESTAMP` | Warning. |
| `UUID` | `VARCHAR(32672)` | |
| `VARCHAR`, `VARCHAR(n)` | `VARCHAR(2 * measured_length)` | If measured length is available and within DB2 limits. |
| `VARCHAR`, `VARCHAR(n)` | `VARCHAR(32672)` | Fallback when length is unknown, or cap for oversized values; warning. |
| `JSON` | `VARCHAR(32672)` | |
| `BLOB`, `BYTEA` | `BLOB` | |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | If precision <= 31. |
| decimal precision > 31 | `DOUBLE` | Warning. |
| unknown | `VARCHAR(32672)` | Warning. |

## MSSQL

### MSSQL Ingress to DuckDB

| MSSQL source type | DuckDB type | Notes |
|---|---|---|
| int-like type except big/small/tiny | `INTEGER` | Driver metadata is string-matched. |
| `bigint` | `BIGINT` | |
| `smallint` | `SMALLINT` | |
| `tinyint` | `SMALLINT` | |
| `bit`, bool | `BOOLEAN` | |
| `decimal`, `numeric` | `DECIMAL(38,10)` | |
| `float`, `double` | `DOUBLE` | |
| `real` | `REAL` | |
| `date` | `DATE` | |
| time-like type excluding datetime | `TIME` | |
| datetime/timestamp-like type | `TIMESTAMP` | |
| binary/bytes-like type | `BLOB` | |
| string/char/text/nchar/nvarchar/varchar | `VARCHAR` | |
| unknown | `VARCHAR` | Lossy warning. |

### DuckDB to MSSQL Egress

| DuckDB source type | MSSQL target type | Notes |
|---|---|---|
| `BOOLEAN`, `BOOL` | `BIT` | |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `REAL` | |
| `DOUBLE` | `FLOAT` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `TIMESTAMP` | `DATETIME2` | |
| `TIMESTAMPTZ` | `DATETIME2` | Warning. |
| `VARCHAR`, `VARCHAR(n)` | `NVARCHAR(2 * measured_length)` | If measured length is available and within MSSQL limits. |
| `VARCHAR`, `VARCHAR(n)` | `NVARCHAR(MAX)` | Fallback when length is unknown, or for oversized values; warning. |
| `JSON` | `NVARCHAR(MAX)` | |
| `UUID` | `UNIQUEIDENTIFIER` | |
| `BLOB`, `BYTEA` | `VARBINARY(MAX)` | |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | |
| decimal precision > 38 | `FLOAT` | Warning. |
| unknown | `NVARCHAR(MAX)` | Warning. |

## MySQL

### MySQL Ingress to DuckDB

| MySQL source type | DuckDB type | Notes |
|---|---|---|
| `TINY` with internal size 1 | `BOOLEAN` | |
| `TINY`, `SHORT` | `SMALLINT` | |
| `LONG`, `INT24` | `INTEGER` | |
| `LONGLONG` | `BIGINT` | |
| `FLOAT` | `REAL` | |
| `DOUBLE` | `DOUBLE` | |
| `DECIMAL`, `NEWDECIMAL` | `DECIMAL(p,s)` | If precision <= 38. |
| decimal precision > 38 | `DOUBLE` | Lossy warning. |
| `DATE`, `NEWDATE` | `DATE` | |
| `TIME` | `TIME` | |
| `DATETIME`, `TIMESTAMP` | `TIMESTAMP` | |
| `TINY_BLOB`, `MEDIUM_BLOB`, `LONG_BLOB`, `BLOB` | `BLOB` | |
| `BIT` | `BLOB` | Lossy. |
| `JSON` | `VARCHAR` | Lossy warning. |
| `VAR_STRING`, `STRING`, `VARCHAR` | `VARCHAR` | |
| `ENUM`, `SET` | `VARCHAR` | |
| unknown | `VARCHAR` | Lossy warning. |

### DuckDB to MySQL Egress

| DuckDB source type | MySQL target type | Notes |
|---|---|---|
| `BOOLEAN`, `BOOL` | `BOOLEAN` | |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `FLOAT` | |
| `DOUBLE` | `DOUBLE` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `TIMESTAMP` | `DATETIME` | |
| `TIMESTAMPTZ` | `DATETIME` | Warning. |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | |
| `DECIMAL` | `DECIMAL(min(p,65), min(s,30))` | Uses metadata/defaults. |
| `BLOB`, `BYTEA` | `LONGBLOB` | |
| `VARCHAR`, `VARCHAR(n)` | `VARCHAR(2 * measured_length)` | If measured length is available and within MySQL limits. |
| `VARCHAR`, `VARCHAR(n)` | `LONGTEXT` | Fallback when length is unknown, or for oversized values; warning. |
| `JSON` | `LONGTEXT` | |
| `UUID` | `VARCHAR(36)` | |
| unknown | `LONGTEXT` | Warning. |

## MariaDB

MariaDB uses the same mapping rules as MySQL, with MariaDB-specific warning messages.

### MariaDB Ingress to DuckDB

| MariaDB source type | DuckDB type | Notes |
|---|---|---|
| `TINY` with internal size 1 | `BOOLEAN` | |
| `TINY`, `SHORT` | `SMALLINT` | |
| `LONG`, `INT24` | `INTEGER` | |
| `LONGLONG` | `BIGINT` | |
| `FLOAT` | `REAL` | |
| `DOUBLE` | `DOUBLE` | |
| `DECIMAL`, `NEWDECIMAL` | `DECIMAL(p,s)` | If precision <= 38. |
| decimal precision > 38 | `DOUBLE` | Lossy warning. |
| `DATE`, `NEWDATE` | `DATE` | |
| `TIME` | `TIME` | |
| `DATETIME`, `TIMESTAMP` | `TIMESTAMP` | |
| `TINY_BLOB`, `MEDIUM_BLOB`, `LONG_BLOB`, `BLOB` | `BLOB` | |
| `BIT` | `BLOB` | Lossy. |
| `JSON` | `VARCHAR` | Lossy warning. |
| `VAR_STRING`, `STRING`, `VARCHAR` | `VARCHAR` | |
| `ENUM`, `SET` | `VARCHAR` | |
| unknown | `VARCHAR` | Lossy warning. |

### DuckDB to MariaDB Egress

| DuckDB source type | MariaDB target type | Notes |
|---|---|---|
| `BOOLEAN`, `BOOL` | `BOOLEAN` | |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `FLOAT` | |
| `DOUBLE` | `DOUBLE` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `TIMESTAMP` | `DATETIME` | |
| `TIMESTAMPTZ` | `DATETIME` | Warning. |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | |
| `DECIMAL` | `DECIMAL(min(p,65), min(s,30))` | Uses metadata/defaults. |
| `BLOB`, `BYTEA` | `LONGBLOB` | |
| `VARCHAR`, `VARCHAR(n)` | `VARCHAR(2 * measured_length)` | If measured length is available and within MariaDB limits. |
| `VARCHAR`, `VARCHAR(n)` | `LONGTEXT` | Fallback when length is unknown, or for oversized values; warning. |
| `JSON` | `LONGTEXT` | |
| `UUID` | `VARCHAR(36)` | |
| unknown | `LONGTEXT` | Warning. |

## Oracle

### Oracle Ingress to DuckDB

| Oracle source type | DuckDB type | Notes |
|---|---|---|
| `NUMBER(p,s)` | `DECIMAL(p,s)` | If precision <= 38. |
| `NUMBER(p,s)` with precision > 38 | `DOUBLE` | Lossy warning. |
| unconstrained `NUMBER` | `DOUBLE` | Lossy warning. |
| `BINARY_FLOAT`, `BINARY_DOUBLE`, `FLOAT` | `DOUBLE` | |
| `CHAR`, `NCHAR`, `VARCHAR`, `VARCHAR2` | `VARCHAR` | |
| `NVARCHAR`, `NVARCHAR2`, `CLOB`, `NCLOB`, `LONG` | `VARCHAR` | |
| `DATE` | `TIMESTAMP` | Oracle `DATE` includes time. |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP_TZ` | `TIMESTAMP` | Lossy warning. |
| `TIMESTAMP WITH LOCAL TIME ZONE`, `TIMESTAMP_LTZ` | `TIMESTAMP` | Lossy warning. |
| timestamp-like type | `TIMESTAMP` | |
| `BLOB`, `RAW`, `LONG_RAW` | `BLOB` | |
| unknown | `VARCHAR` | Lossy warning. |

### DuckDB to Oracle Egress

| DuckDB source type | Oracle target type | Notes |
|---|---|---|
| `BOOLEAN`, `BOOL` | `NUMBER(1)` | |
| `SMALLINT` | `NUMBER(5)` | |
| `INTEGER` | `NUMBER(10)` | |
| `BIGINT` | `NUMBER(19)` | |
| `REAL`, `FLOAT`, `DOUBLE` | `BINARY_DOUBLE` | |
| `DATE` | `DATE` | |
| `TIMESTAMP` | `TIMESTAMP` | |
| `TIMESTAMPTZ` | `TIMESTAMP` | Warning. |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | If precision <= 38. |
| decimal precision > 38 | `BINARY_DOUBLE` | Warning. |
| `BLOB`, `BYTEA` | `BLOB` | |
| `VARCHAR`, `VARCHAR(n)` | `VARCHAR2(2 * measured_length)` | If measured length is available and <= 4000 after padding. |
| `VARCHAR`, `VARCHAR(n)` with oversized measured length | `CLOB` | Warning. |
| unbounded `VARCHAR` without measured length | `VARCHAR2(4000)` | Warning. |
| `UUID` | `VARCHAR2(36)` | |
| `JSON` | `CLOB` | |
| unknown | `CLOB` | Warning. |

## File Ingress and Egress

File nodes are DuckDB-backed:

- CSV ingress uses DuckDB CSV reading with optional explicit `columns`.
- JSONL ingress reads JSON lines into DuckDB-compatible columns.
- Parquet ingress preserves Parquet schema through DuckDB.
- CSV, JSONL, and Parquet egress export DuckDB query results to files.

When CSV `header=False`, explicit `columns` are required.
