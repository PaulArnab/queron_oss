from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping


ConfigFactory = Callable[[], Mapping[str, Any]]


def normalize_binding_type(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"postgres", "postgresql"}:
        return "postgresql"
    if raw == "db2":
        return "db2"
    raise RuntimeError(f"Unsupported runtime binding type '{value}'.")


def _coerce_mapping(value: Mapping[str, Any] | dict[str, Any], *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{label} must return a mapping.")
    return {str(key): item for key, item in dict(value).items()}


def _clean_config_values(values: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text:
                continue
            cleaned[key] = text
            continue
        cleaned[key] = value
    return cleaned


def resolve_runtime_binding_value(config_name: str, binding: Any) -> dict[str, Any]:
    if isinstance(binding, RuntimeBinding):
        return binding.resolve_config(config_name)
    if isinstance(binding, Mapping):
        return resolve_runtime_binding_payload(config_name, dict(binding))
    raise RuntimeError(
        f"Runtime binding for config '{config_name}' must be a Queron binding object or a mapping payload."
    )


def resolve_runtime_binding_payload(config_name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    binding_type = normalize_binding_type(
        str(payload.get("type") or payload.get("binding_type") or payload.get("connection_type") or "").strip()
    )
    resolved = _clean_config_values(
        {
            "type": binding_type,
            "name": payload.get("name") or config_name,
            "host": payload.get("host"),
            "port": payload.get("port"),
            "database": payload.get("database"),
            "username": payload.get("username"),
            "password": payload.get("password"),
            "url": payload.get("url"),
            "auth_mode": payload.get("auth_mode"),
            "sslmode": payload.get("sslmode"),
            "sslrootcert": payload.get("sslrootcert"),
            "sslcert": payload.get("sslcert"),
            "sslkey": payload.get("sslkey"),
            "sslpassword": payload.get("sslpassword"),
            "ssl_server_certificate": payload.get("ssl_server_certificate"),
            "ssl_client_keystoredb": payload.get("ssl_client_keystoredb"),
            "ssl_client_keystash": payload.get("ssl_client_keystash"),
            "ssl_client_keystore_password": payload.get("ssl_client_keystore_password"),
            "ssl_client_label": payload.get("ssl_client_label"),
            "connect_timeout_seconds": payload.get("connect_timeout_seconds"),
            "statement_timeout_ms": payload.get("statement_timeout_ms"),
        }
    )
    return resolved


@dataclass(slots=True)
class RuntimeBinding:
    binding_type: str
    config_factory: ConfigFactory | None = None
    name: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    url: str | None = None
    auth_mode: str | None = None
    sslmode: str | None = None
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None
    sslpassword: str | None = None
    ssl_server_certificate: str | None = None
    ssl_client_keystoredb: str | None = None
    ssl_client_keystash: str | None = None
    ssl_client_keystore_password: str | None = None
    ssl_client_label: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def resolve_config(self, config_name: str) -> dict[str, Any]:
        resolved = _clean_config_values(
            {
                "type": normalize_binding_type(self.binding_type),
                "name": self.name or config_name,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "username": self.username,
                "password": self.password,
                "url": self.url,
                "auth_mode": self.auth_mode,
                "sslmode": self.sslmode,
                "sslrootcert": self.sslrootcert,
                "sslcert": self.sslcert,
                "sslkey": self.sslkey,
                "sslpassword": self.sslpassword,
                "ssl_server_certificate": self.ssl_server_certificate,
                "ssl_client_keystoredb": self.ssl_client_keystoredb,
                "ssl_client_keystash": self.ssl_client_keystash,
                "ssl_client_keystore_password": self.ssl_client_keystore_password,
                "ssl_client_label": self.ssl_client_label,
                **self.extras,
            }
        )
        if self.config_factory is not None:
            produced = _coerce_mapping(self.config_factory(), label=f"Runtime binding factory for '{config_name}'")
            resolved.update(_clean_config_values(produced))
        resolved["type"] = normalize_binding_type(str(resolved.get("type") or self.binding_type))
        resolved.setdefault("name", self.name or config_name)
        return resolved


class PostgresBinding(RuntimeBinding):
    def __init__(
        self,
        *,
        config_factory: ConfigFactory | None = None,
        name: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
        url: str | None = None,
        auth_mode: str | None = None,
        sslmode: str | None = None,
        sslrootcert: str | None = None,
        sslcert: str | None = None,
        sslkey: str | None = None,
        sslpassword: str | None = None,
        **extras: Any,
    ) -> None:
        super().__init__(
            binding_type="postgresql",
            config_factory=config_factory,
            name=name,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            url=url,
            auth_mode=auth_mode,
            sslmode=sslmode,
            sslrootcert=sslrootcert,
            sslcert=sslcert,
            sslkey=sslkey,
            sslpassword=sslpassword,
            extras=extras,
        )


class Db2Binding(RuntimeBinding):
    def __init__(
        self,
        *,
        config_factory: ConfigFactory | None = None,
        name: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
        url: str | None = None,
        auth_mode: str | None = None,
        ssl_server_certificate: str | None = None,
        ssl_client_keystoredb: str | None = None,
        ssl_client_keystash: str | None = None,
        ssl_client_keystore_password: str | None = None,
        ssl_client_label: str | None = None,
        **extras: Any,
    ) -> None:
        super().__init__(
            binding_type="db2",
            config_factory=config_factory,
            name=name,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            url=url,
            auth_mode=auth_mode,
            ssl_server_certificate=ssl_server_certificate,
            ssl_client_keystoredb=ssl_client_keystoredb,
            ssl_client_keystash=ssl_client_keystash,
            ssl_client_keystore_password=ssl_client_keystore_password,
            ssl_client_label=ssl_client_label,
            extras=extras,
        )


PostGresBinding = PostgresBinding

