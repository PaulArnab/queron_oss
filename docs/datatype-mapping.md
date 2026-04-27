# Datatype Mapping

Queron records datatype mappings during ingress and egress. These mappings are exposed in node inspection and persisted in the artifact database.

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

## Mapping Modes

| Mode | Meaning |
|---|---|
| `python_mapped` | Connector read source metadata and created the DuckDB table explicitly. |
| `ingress` | Connector-specific ingress mapping path. |
| `adbc_passthrough` | Transport path provided DuckDB-compatible data and metadata. |
| `egress_inferred` | Egress target type was inferred from DuckDB query result metadata. |
| `egress_remote_schema` | Existing remote table schema was inspected and used. |

When egress mode appends to or uses an existing remote table, `egress_remote_schema` takes precedence over inferred mapping.

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
| `VARCHAR`, `VARCHAR(n)` | `VARCHAR(32672)` | |
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
| `VARCHAR`, `VARCHAR(n)` | `NVARCHAR(MAX)` | |
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
| `VARCHAR`, `VARCHAR(n)` | `LONGTEXT` | |
| `JSON` | `LONGTEXT` | |
| `UUID` | `LONGTEXT` | |
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
| `VARCHAR`, `VARCHAR(n)` | `LONGTEXT` | |
| `JSON` | `LONGTEXT` | |
| `UUID` | `LONGTEXT` | |
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
| `VARCHAR(n)` | `VARCHAR2(n)` | If length <= 4000. |
| `VARCHAR(n)` with length > 4000 | `CLOB` | Warning. |
| unbounded `VARCHAR` | `VARCHAR2(4000)` | |
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

