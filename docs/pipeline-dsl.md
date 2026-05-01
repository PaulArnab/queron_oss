# Pipeline DSL Reference

Import the DSL:

```python
import queron
```

Decorators attach metadata to functions. Most decorated functions should use `pass`; Queron uses the decorator payload, not the function body. `queron.python.ingress` calls the function at runtime.

## Template Helpers

### `queron.ref(name)`

Returns a template placeholder for a local pipeline artifact.

Use `queron.ref(...)` whenever SQL reads an artifact produced by another Queron node. Raw references to Queron-managed artifacts are not allowed; this keeps dependency tracking, run selection, reset/resume behavior, and artifact versioning unambiguous.

Arguments:

| Argument | Type | Required | Description |
|---|---|---:|---|
| `name` | `str` | yes | Logical output name produced by another node's `out`. |

Example:

```python
query=f"SELECT * FROM {queron.ref('customers')}"
```

### `queron.source(name)`

Returns a template placeholder for a configured remote source relation. This helper is optional for stable source tables that do not change between targets: database ingress SQL may use a raw external table name such as `public.policy` or `DB2INST1.POLICY` directly.

Use `queron.source(...)` when the physical source relation changes by target/environment. Raw external table names are allowed only in database ingress SQL; model SQL, checks, and internal Queron artifact reads must use `queron.ref(...)`.

Arguments:

| Argument | Type | Required | Description |
|---|---|---:|---|
| `name` | `str` | yes | Logical source name from `configurations.yaml` `sources`. |

### `queron.lookup(name)`

Returns a template placeholder for a configured remote lookup relation.

Arguments:

| Argument | Type | Required | Description |
|---|---|---:|---|
| `name` | `str` | yes | Logical lookup name from `configurations.yaml` `lookup`. |

Lookups are only valid in database ingress SQL nodes.

### `queron.var(name, *, log_value=False, mutable_after_start=False, default=...)`

Returns a runtime variable placeholder. At execution time it becomes a parameterized SQL placeholder.

Arguments:

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Runtime variable name. |
| `log_value` | `bool` | no | `False` | Whether the value may be logged. |
| `mutable_after_start` | `bool` | no | `False` | Whether value may change when resuming an existing run. |
| `default` | Python literal | no | unset | Default value if not supplied at runtime. |

Runtime vars are allowed only in SQL value positions.

## Local Model

### `queron.model.sql(...)`

Creates a DuckDB SQL model node.

```python
@queron.model.sql(
    name="customer_summary",
    out="customer_summary",
    query=f"""
SELECT customer_id, COUNT(*) AS order_count
FROM {queron.ref("orders")}
GROUP BY customer_id
""",
)
def customer_summary():
    pass
```

Arguments:

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Unique node name. |
| `query` | `str` | yes | | DuckDB SQL query. |
| `out` | `str` | yes | | Logical artifact name produced by this node. |
| `materialized` | `str` | no | `artifact` | Materialization mode. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependency node names. |

## Python Ingress

### `queron.python.ingress(...)`

Runs a zero-required-argument Python function and ingests its return value.

```python
@queron.python.ingress(name="service_playbook", out="service_playbook")
def service_playbook():
    import pandas as pd
    return pd.DataFrame([
        {"product_line": "AUTO", "queue_owner": "auto_team"},
    ])
```

Arguments:

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `fn` | callable | no | `None` | Callable when used as bare decorator. |
| `name` | `str or None` | no | function name | Unique node name. |
| `out` | `str` | yes | | Logical artifact name. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependency node names. |

The function must not declare required parameters.

## File Ingress

### `queron.csv.ingress(...)`

```python
@queron.csv.ingress(
    name="vip_customers",
    out="vip_customers",
    path="local_files/vip_customers.csv",
    header=True,
    delimiter=",",
    columns={"customer_number": "VARCHAR", "vip_flag": "BOOLEAN"},
)
def vip_customers():
    pass
```

Arguments:

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `out` | `str` | yes | | Logical artifact name. |
| `path` | `str` | yes | | CSV file path, relative to project root unless absolute. |
| `header` | `bool` | no | `True` | Whether first row is header. |
| `delimiter` | `str` | no | `,` | Single-character delimiter. |
| `quote` | `str or None` | no | `None` | Single-character quote override. |
| `escape` | `str or None` | no | `None` | Single-character escape override. |
| `skip_rows` | `int` | no | `0` | Rows to skip. |
| `columns` | `dict[str, str] or None` | no | `None` | Explicit column type mapping. Required when `header=False`. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

### `queron.jsonl.ingress(...)`

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `out` | `str` | yes | | Logical artifact name. |
| `path` | `str` | yes | | JSONL file path. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

### `queron.parquet.ingress(...)`

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `out` | `str` | yes | | Logical artifact name. |
| `path` | `str` | yes | | Parquet file path. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

### `queron.file.ingress(...)`

Generic file ingress. Format can be supplied explicitly or inferred from extension.

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `out` | `str` | yes | | Logical artifact name. |
| `path` | `str` | yes | | File path. |
| `format` | `str or None` | no | `None` | `csv`, `jsonl`, or `parquet`. |
| `header` | `bool` | no | `True` | CSV only. |
| `delimiter` | `str` | no | `,` | CSV only. |
| `quote` | `str or None` | no | `None` | CSV only. |
| `escape` | `str or None` | no | `None` | CSV only. |
| `skip_rows` | `int` | no | `0` | CSV only. |
| `columns` | `dict[str, str] or None` | no | `None` | CSV only. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

## File Egress

### `queron.csv.egress(...)`

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `path` | `str` | yes | | Output CSV path. |
| `sql` | `str` | yes | | DuckDB SQL query to export. |
| `overwrite` | `bool` | no | `False` | Replace output file if it exists. |
| `header` | `bool` | no | `True` | Write header row. |
| `delimiter` | `str` | no | `,` | Single-character delimiter. |
| `out` | `str` | yes | | Logical artifact name for egress record/artifact. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

### `queron.jsonl.egress(...)`

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `path` | `str` | yes | | Output JSONL path. |
| `sql` | `str` | yes | | DuckDB SQL query to export. |
| `overwrite` | `bool` | no | `False` | Replace output file if it exists. |
| `out` | `str` | yes | | Logical artifact name. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

### `queron.parquet.egress(...)`

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | `str` | yes | | Node name. |
| `path` | `str` | yes | | Output Parquet path. |
| `sql` | `str` | yes | | DuckDB SQL query to export. |
| `overwrite` | `bool` | no | `False` | Replace output file if it exists. |
| `compression` | `str or None` | no | `None` | Parquet compression. |
| `out` | `str` | yes | | Logical artifact name. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

## Database Ingress

Each database namespace supports ingress:

```python
@queron.postgres.ingress(
    config="PostGres",
    name="policy_core",
    out="policy_core",
    sql=f"""
SELECT *
FROM {queron.source("pg_policy")}
""",
)
def policy_core():
    pass
```

If the source table is stable across environments, the same ingress can use the physical table directly:

```python
@queron.postgres.ingress(
    config="PostGres",
    name="policy_core",
    out="policy_core",
    sql="""
SELECT *
FROM public.policy
""",
)
def policy_core():
    pass
```

Raw external table names are limited to database ingress. Downstream models and checks should read Queron artifacts with `queron.ref(...)`, not raw table names.

Namespaces:

- `queron.postgres`
- `queron.db2`
- `queron.mssql`
- `queron.mysql`
- `queron.mariadb`
- `queron.oracle`

Arguments:

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `config` | `str` | yes | | Connection binding name. |
| `name` | `str` | yes | | Node name. |
| `out` | `str` | yes | | Logical artifact name. |
| `sql` | `str` | yes | | Source database SQL. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

## Database Egress

Each database namespace supports egress:

```python
@queron.postgres.egress(
    config="PostGres",
    name="write_policy_export",
    table="policy_export",
    sql=f"SELECT * FROM {queron.ref('policy_core')}",
    mode="replace",
    retain=False,
    out="write_policy_export",
)
def write_policy_export():
    pass
```

Arguments:

| Argument | Type | Required | Default | Description |
|---|---|---:|---|---|
| `config` | `str` | yes | | Connection binding name. |
| `name` | `str` | yes | | Node name. |
| `table` | `str` | yes | | Target table or configured egress relation name. |
| `sql` | `str` | yes | | DuckDB SQL query to write. |
| `mode` | `str` | no | `replace` | Write mode. |
| `retain` | `bool` | no | connector-specific | Retain remote lookup/egress table when applicable. |
| `out` | `str` | yes | | Logical artifact name. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | `None` | Manual dependencies. |

Supported modes:

- `replace`
- `append`
- `create`
- `create_append`

`retain` is present for PostgreSQL, MySQL, MariaDB, and Oracle egress. DB2 and MSSQL egress decorators do not expose `retain`.

## Database Lookup

Lookup nodes write a temporary or retained remote lookup table that later database ingress nodes can reference through `queron.lookup(...)`.

```python
@queron.postgres.lookup(
    config="PostGres",
    name="stage_active_policy_lookup",
    table="active_policy_lookup",
    sql=f"SELECT policy_id FROM {queron.ref('active_policies')}",
    mode="replace",
    retain=False,
    out="stage_active_policy_lookup",
)
def stage_active_policy_lookup():
    pass
```

Lookup producer and consumer must use the same connector and config binding.

DB2 and MSSQL lookup decorators do not expose `retain`; PostgreSQL, MySQL, MariaDB, and Oracle do.

## Checks

### `queron.check.fail_if_count(...)`

Runs a scalar count query. The node fails when the comparison evaluates true.

```python
@queron.check.fail_if_count(
    name="require_high_touch",
    query=f"""
SELECT COUNT(*)
FROM {queron.ref("policy_servicing_queue")}
WHERE servicing_segment = 'HIGH_TOUCH'
""",
    operator="<=",
    value=0,
)
def require_high_touch():
    pass
```

Arguments:

| Argument | Type | Required | Description |
|---|---|---:|---|
| `name` | `str` | yes | Node name. |
| `query` | `str` | yes | SQL returning exactly one numeric value. |
| `operator` | `str` | yes | One of `==`, `=`, `!=`, `>`, `>=`, `<`, `<=`. |
| `value` | `int or float` | yes | Comparison value. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | Manual dependencies. |

### `queron.check.fail_if_true(...)`

Runs a scalar boolean query. The node fails when the result is true.

| Argument | Type | Required | Description |
|---|---|---:|---|
| `name` | `str` | yes | Node name. |
| `query` | `str` | yes | SQL returning exactly one boolean value. |
| `depends_on` | `str, list[str], tuple[str, ...], or None` | no | Manual dependencies. |
