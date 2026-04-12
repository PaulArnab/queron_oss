from __future__ import annotations

import importlib
from pathlib import Path
import sys
import time
from typing import Any


def load_duckdb():
    backend_dir = Path(__file__).resolve().parent
    shadow_path = (backend_dir / "duckdb.py").resolve()
    existing = sys.modules.get("duckdb")
    existing_file = getattr(existing, "__file__", None)
    if existing is not None and existing_file:
        try:
            if Path(existing_file).resolve() != shadow_path:
                return existing
        except OSError:
            return existing

    original_path = list(sys.path)
    try:
        sys.modules.pop("duckdb", None)
        filtered_path: list[str] = []
        for entry in sys.path:
            try:
                if Path(entry).resolve() == backend_dir:
                    continue
            except OSError:
                pass
            filtered_path.append(entry)
        sys.path = filtered_path
        return importlib.import_module("duckdb")
    except ModuleNotFoundError as exc:
        raise RuntimeError("DuckDB driver is not installed. Install the 'duckdb' package first.") from exc
    finally:
        sys.path = original_path


def _is_retryable_duckdb_open_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return (
        "cannot open file" in text
        or "used by another process" in text
        or "being used by another process" in text
        or "database is locked" in text
        or "file is locked" in text
    )


def connect_duckdb(
    database: str | None = None,
    *,
    read_only: bool = False,
    retries: int = 20,
    retry_delay_seconds: float = 0.1,
    **kwargs: Any,
):
    duckdb = load_duckdb()
    last_error: Exception | None = None
    attempts = max(1, int(retries))
    for attempt in range(attempts):
        try:
            if database is None:
                return duckdb.connect(read_only=read_only, **kwargs)
            return duckdb.connect(database, read_only=read_only, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= attempts - 1 or not _is_retryable_duckdb_open_error(exc):
                raise
            time.sleep(max(0.01, float(retry_delay_seconds)))
    if last_error is not None:
        raise last_error
    raise RuntimeError("Failed to open DuckDB connection.")
