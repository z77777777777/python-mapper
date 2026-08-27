"""asyncpg pool lifecycle owned by cyt-pymapper."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol, cast

import asyncpg


class ConnectionLike(Protocol):
    """Execution surface used by the mapper runtime and test doubles."""

    async def execute(self, query: str, *args: Any, timeout: float | None = None) -> str: ...

    async def fetch(self, query: str, *args: Any, timeout: float | None = None) -> list[Any]: ...

    async def fetchval(
        self,
        query: str,
        *args: Any,
        column: int = 0,
        timeout: float | None = None,
    ) -> Any: ...

    def transaction(
        self,
        *,
        isolation: str | None = None,
        readonly: bool = False,
        deferrable: bool = False,
    ) -> Any: ...


class PoolLike(Protocol):
    """Pool surface required by the runtime."""

    def acquire(self) -> Any: ...

    async def close(self) -> None: ...


PoolFactory = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    dsn: str
    min_size: int = 1
    max_size: int = 10
    command_timeout: float | None = None
    ssl: Any = None
    statement_cache_size: int = 100


_DATABASE_CONFIG: DatabaseConfig | None = None
_POOL: PoolLike | None = None
_POOL_OWNED = False
_POOL_FACTORY: PoolFactory = asyncpg.create_pool


def _encode_json(value: Any) -> str:
    """Accept native JSON values and legacy pre-serialized JSON strings."""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


async def _initialize_connection(connection: asyncpg.Connection) -> None:
    """Decode PostgreSQL JSON/JSONB columns into native Python values."""
    for type_name in ("json", "jsonb"):
        await connection.set_type_codec(
            type_name,
            schema="pg_catalog",
            encoder=_encode_json,
            decoder=json.loads,
            format="text",
        )


def normalize_asyncpg_dsn(dsn: str) -> str:
    """Convert SQLAlchemy-style PostgreSQL URLs into asyncpg-compatible DSNs."""
    normalized = dsn.strip()
    if normalized.startswith("postgresql+asyncpg://"):
        return "postgresql://" + normalized[len("postgresql+asyncpg://"):]
    if normalized.startswith("postgres+asyncpg://"):
        return "postgres://" + normalized[len("postgres+asyncpg://"):]
    return normalized


def configure_database(
    *,
    dsn: str,
    min_size: int = 1,
    max_size: int = 10,
    command_timeout: float | None = None,
    ssl: Any = None,
    statement_cache_size: int = 100,
    pool_factory: PoolFactory | None = None,
) -> None:
    """Register pool settings. Network connections are opened by ``open_database``."""
    if _POOL is not None:
        raise RuntimeError("cannot reconfigure cyt-pymapper while its database pool is open")
    if not dsn.strip():
        raise ValueError("database dsn must not be empty")
    if min_size < 0 or max_size < 1 or min_size > max_size:
        raise ValueError("pool sizes must satisfy 0 <= min_size <= max_size")
    global _DATABASE_CONFIG, _POOL_FACTORY
    _DATABASE_CONFIG = DatabaseConfig(
        dsn=normalize_asyncpg_dsn(dsn),
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        ssl=ssl,
        statement_cache_size=statement_cache_size,
    )
    if pool_factory is not None:
        _POOL_FACTORY = pool_factory


def configure_pool(pool: PoolLike) -> None:
    """Inject an externally managed pool, primarily for tests and host integration."""
    global _POOL, _POOL_OWNED
    if _POOL is pool:
        return
    if _POOL is not None:
        raise RuntimeError("cyt-pymapper database pool is already configured")
    _POOL = pool
    _POOL_OWNED = False


async def open_database() -> PoolLike:
    """Open the configured asyncpg pool once during application startup."""
    global _POOL, _POOL_OWNED
    if _POOL is not None:
        return _POOL
    config = _DATABASE_CONFIG
    if config is None:
        raise RuntimeError("cyt-pymapper database is not configured")
    created = await _POOL_FACTORY(
        dsn=config.dsn,
        min_size=config.min_size,
        max_size=config.max_size,
        command_timeout=config.command_timeout,
        ssl=config.ssl,
        statement_cache_size=config.statement_cache_size,
        init=_initialize_connection,
    )
    _POOL = cast(PoolLike, created)
    _POOL_OWNED = True
    return _POOL


def get_pool() -> PoolLike:
    """Return the active pool or fail before any SQL is attempted."""
    if _POOL is None:
        raise RuntimeError("cyt-pymapper database pool is not open; call open_database() at startup")
    return _POOL


@asynccontextmanager
async def acquire_raw_connection() -> AsyncGenerator[ConnectionLike]:
    """Borrow one connection without opening an explicit transaction."""
    async with get_pool().acquire() as connection:
        yield cast(ConnectionLike, connection)


async def ping_database() -> bool:
    """Check the primary database through the same pool used by mappers."""
    async with acquire_raw_connection() as connection:
        return await connection.fetchval("SELECT 1") == 1


async def close_database(*, timeout: float = 10.0) -> None:
    """Close an owned pool during application shutdown."""
    global _POOL, _POOL_OWNED
    pool, owned = _POOL, _POOL_OWNED
    _POOL = None
    _POOL_OWNED = False
    if pool is None or not owned:
        return
    await asyncio.wait_for(pool.close(), timeout=timeout)


def clear_database_configuration() -> None:
    """Clear unopened configuration and injected test pools."""
    global _DATABASE_CONFIG, _POOL, _POOL_OWNED, _POOL_FACTORY
    if _POOL is not None and _POOL_OWNED:
        raise RuntimeError("close_database() must be awaited before resetting an owned pool")
    _DATABASE_CONFIG = None
    _POOL = None
    _POOL_OWNED = False
    _POOL_FACTORY = asyncpg.create_pool


__all__ = [
    "ConnectionLike",
    "DatabaseConfig",
    "PoolLike",
    "acquire_raw_connection",
    "clear_database_configuration",
    "close_database",
    "configure_database",
    "configure_pool",
    "get_pool",
    "normalize_asyncpg_dsn",
    "open_database",
    "ping_database",
]
