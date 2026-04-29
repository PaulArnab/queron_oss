# Queron Documentation

This folder contains the Queron OSS documentation.

## Documents

- [Architecture](architecture.md): system design, compile/runtime flow, artifact layout, and graph UI architecture.
- [Internals Guide](internals-guide.md): detailed compile, run, artifact retention, and inspect behavior.
- [Interactive Flowcharts](flowcharts.md): detailed compiler flow, run flow, run-control functions, and artifact table popups.
- [User Guide](user-guide.md): installation, project setup, pipeline authoring, configuration, running, inspection, reset, export, and graph UI usage.
- [CLI and Python Commands](commands-reference.md): Python function shapes and CLI command arguments in workflow order.
- [Pipeline DSL Reference](pipeline-dsl.md): every decorator, helper, function argument, and node type exposed by `import queron`.
- [Python API Reference](python-api.md): public API functions, result objects, runtime bindings, log events, and usage examples.
- [CLI Reference](cli-reference.md): all `queron` commands and flags.
- [Datatype Mapping](datatype-mapping.md): per-driver ingress-to-DuckDB and DuckDB-to-egress type mapping tables.
- [Examples](examples.md): complete local file, database, runtime variable, egress, and inspection examples.

## Core Concepts

Queron pipelines are Python files that declare nodes with decorators. The compiler reads those decorators, resolves dependencies and configured relations, and writes an execution contract into a DuckDB artifact database. The runtime executes the DAG, stores node outputs in DuckDB, writes run history and logs, and optionally moves data to remote egress targets.

The default artifact path is:

```text
.queron/<pipeline_id>/artifact.duckdb
```
