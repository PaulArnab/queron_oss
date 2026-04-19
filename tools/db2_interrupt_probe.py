from __future__ import annotations

import argparse
import json
import threading
import time
from typing import Any

import db2_core


DEFAULT_CONN_STR = (
    "DATABASE=LOOMDB;"
    "HOSTNAME=localhost;"
    "PORT=50000;"
    "PROTOCOL=TCPIP;"
    "UID=db2inst1;"
    "PWD=LoomDb2Pass123!;"
    "CONNECTTIMEOUT=10;"
)
DEFAULT_SQL = "SELECT * FROM DB2INST1.PERSON"


def _manual_interrupt(mode: str, conn: Any, cur: Any | None) -> dict[str, Any]:
    state = {
        "mode": mode,
        "db2_statement_handle_present": getattr(cur, "stmt_handler", None) is not None if cur is not None else False,
        "db2_connection_handle_present": getattr(conn, "conn_handler", None) is not None,
        "db2_free_result_attempted": False,
        "db2_raw_close_attempted": False,
        "db2_connection_close_attempted": False,
    }
    stmt_handler = getattr(cur, "stmt_handler", None) if cur is not None else None
    conn_handler = getattr(conn, "conn_handler", None)
    if mode in {"stmt_free_only", "combo"} and stmt_handler is not None and db2_core.ibm_db is not None:
        if hasattr(db2_core.ibm_db, "free_result"):
            state["db2_free_result_attempted"] = True
            try:
                db2_core.ibm_db.free_result(stmt_handler)
            except Exception as exc:
                state["db2_free_result_error"] = f"{type(exc).__name__}: {exc}"
    if mode in {"raw_close_only", "combo"} and conn_handler is not None and db2_core.ibm_db is not None:
        if hasattr(db2_core.ibm_db, "close"):
            state["db2_raw_close_attempted"] = True
            try:
                db2_core.ibm_db.close(conn_handler)
            except Exception as exc:
                state["db2_raw_close_error"] = f"{type(exc).__name__}: {exc}"
    if mode in {"conn_close_only", "combo"}:
        state["db2_connection_close_attempted"] = True
        try:
            conn.close()
        except Exception as exc:
            state["db2_connection_close_error"] = f"{type(exc).__name__}: {exc}"
    return state


def run_probe(
    *,
    conn_str: str,
    sql: str,
    fetch_size: int,
    interrupt_delay: float,
    join_timeout: float,
    mode: str,
    phase: str,
) -> dict[str, Any]:
    conn = db2_core._dbapi_connect(conn_str)
    cur = conn.cursor()
    result: dict[str, Any] = {
        "mode": mode,
        "phase": phase,
        "fetch_size": fetch_size,
        "interrupt_delay_seconds": interrupt_delay,
        "join_timeout_seconds": join_timeout,
        "sql": sql,
        "conn_type": type(conn).__name__,
        "cursor_type": type(cur).__name__,
        "stmt_handler_present": False,
        "conn_handler_present": getattr(conn, "conn_handler", None) is not None,
        "rows_fetched": 0,
        "current_phase": "setup",
        "started_at_monotonic": time.monotonic(),
    }
    stop_event = threading.Event()
    interrupt_result: dict[str, Any] = {}
    fetch_error: dict[str, str] = {}
    built_state: dict[str, Any] = {}

    def _run_interrupt() -> dict[str, Any]:
        if mode == "built":
            built_interrupt, _ = db2_core._build_db2_interruptor(conn, cur)
            try:
                return built_interrupt()
            except Exception as exc:
                return {"interrupt_error": f"{type(exc).__name__}: {exc}"}
        return _manual_interrupt(mode, conn, cur)

    def _fetch_loop() -> None:
        try:
            exec_started = time.monotonic()
            result["current_phase"] = "execute"
            cur.execute(sql)
            result["execute_duration_seconds"] = round(time.monotonic() - exec_started, 6)
            result["stmt_handler_present"] = getattr(cur, "stmt_handler", None) is not None
            built_state.update(
                {
                    "db2_statement_handle_present": result["stmt_handler_present"],
                    "db2_connection_handle_present": result["conn_handler_present"],
                    "db2_free_result_attempted": False,
                    "db2_raw_close_attempted": False,
                    "db2_connection_close_attempted": False,
                }
            )
            if phase == "execute_only":
                result["execute_completed"] = True
                result["current_phase"] = "done"
                return
            result["execute_completed"] = True
            result["current_phase"] = "fetch"
            while True:
                fetch_started = time.monotonic()
                rows = cur.fetchmany(fetch_size)
                result["last_fetch_duration_seconds"] = round(time.monotonic() - fetch_started, 6)
                if not rows:
                    result["fetch_completed"] = True
                    result["current_phase"] = "done"
                    break
                result["rows_fetched"] += len(rows)
        except Exception as exc:
            fetch_error["type"] = type(exc).__name__
            fetch_error["message"] = str(exc)
        finally:
            stop_event.set()

    worker = threading.Thread(target=_fetch_loop, name="db2-fetch-probe", daemon=True)
    worker.start()
    time.sleep(interrupt_delay)
    result["interrupt_started_at_seconds"] = round(time.monotonic() - result["started_at_monotonic"], 6)
    interrupt_result = _run_interrupt()
    result["interrupt_finished_at_seconds"] = round(time.monotonic() - result["started_at_monotonic"], 6)

    worker.join(timeout=join_timeout)
    result["join_finished_at_seconds"] = round(time.monotonic() - result["started_at_monotonic"], 6)
    result["thread_alive_after_join"] = worker.is_alive()
    result["interrupt_result"] = interrupt_result
    result["built_interrupt_state"] = dict(built_state)
    if fetch_error:
        result["fetch_error"] = dict(fetch_error)

    try:
        cur.close()
    except Exception as exc:
        result["cursor_close_error"] = f"{type(exc).__name__}: {exc}"
    try:
        conn.close()
    except Exception as exc:
        result["connection_close_error"] = f"{type(exc).__name__}: {exc}"
    result["finished_at_seconds"] = round(time.monotonic() - result["started_at_monotonic"], 6)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe DB2 interrupt behavior directly against db2_core.")
    parser.add_argument("--conn-str", default=DEFAULT_CONN_STR)
    parser.add_argument("--sql", default=DEFAULT_SQL)
    parser.add_argument("--fetch-size", type=int, default=200)
    parser.add_argument("--interrupt-delay", type=float, default=0.5)
    parser.add_argument("--join-timeout", type=float, default=10.0)
    parser.add_argument("--phase", choices=["fetch", "execute_only"], default="fetch")
    parser.add_argument(
        "--mode",
        choices=["built", "stmt_free_only", "raw_close_only", "conn_close_only", "combo"],
        default="built",
    )
    args = parser.parse_args()
    payload = run_probe(
        conn_str=args.conn_str,
        sql=args.sql,
        fetch_size=max(1, int(args.fetch_size)),
        interrupt_delay=max(0.0, float(args.interrupt_delay)),
        join_timeout=max(0.1, float(args.join_timeout)),
        mode=str(args.mode),
        phase=str(args.phase),
    )
    print(json.dumps(payload, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
