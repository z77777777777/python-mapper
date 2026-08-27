"""PostgreSQL XML mapper with an asyncpg pool and implicit transactions."""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from cyt_pymapper import runtime
from cyt_pymapper.base import (
    MapperBase,
    bind_connection,
    current_connection,
    require_connection,
    transactional,
    transactional_scope,
)
from cyt_pymapper.database import (
    ConnectionLike,
    PoolLike,
    acquire_raw_connection,
    close_database,
    configure_database,
    configure_pool,
    get_pool,
    open_database,
    ping_database,
)
from cyt_pymapper.errors import (
    PaginationConflictError,
    PaginationError,
    PyMapperError,
    TooManyResultsError,
)
from cyt_pymapper.extension import MapperStartupState, PyMapperExtension
from cyt_pymapper.observability import SqlLoggingPlugin
from cyt_pymapper.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    PageMetadata,
    PaginationOptions,
    QueryResult,
)
from cyt_pymapper.plugins import (
    StatementContext,
    StatementExecutor,
    StatementPlugin,
    StatementResult,
)
from cyt_pymapper.runtime import (
    AMapper,
    amapper,
    configure_mapper_paths,
    configure_plugins,
    load_all_mappers,
    load_mapper,
    query,
    render_sql,
    reset_state,
    scalar,
    validate_result_types,
)

__version__ = "0.3.0"


def configure(
    *,
    mapper_paths: Sequence[str | Path],
    database_url: str | None = None,
    pool: PoolLike | None = None,
    min_pool_size: int = 1,
    max_pool_size: int = 10,
    command_timeout: float | None = None,
    ssl=None,
    statement_cache_size: int = 100,
    plugins: Sequence[StatementPlugin] = (),
) -> None:
    """Bind one database backend and the application's XML roots."""
    if (database_url is None) == (pool is None):
        raise ValueError("configure requires exactly one of database_url or pool")
    if pool is not None:
        configure_pool(pool)
    else:
        configure_database(
            dsn=database_url or "",
            min_size=min_pool_size,
            max_size=max_pool_size,
            command_timeout=command_timeout,
            ssl=ssl,
            statement_cache_size=statement_cache_size,
        )
    configure_mapper_paths(tuple(mapper_paths))
    configure_plugins(tuple(plugins))


__all__ = [
    "AMapper",
    "ConnectionLike",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "MapperBase",
    "MapperStartupState",
    "PageMetadata",
    "PaginationConflictError",
    "PaginationError",
    "PaginationOptions",
    "PoolLike",
    "PyMapperError",
    "PyMapperExtension",
    "QueryResult",
    "SqlLoggingPlugin",
    "StatementContext",
    "StatementExecutor",
    "StatementPlugin",
    "StatementResult",
    "TooManyResultsError",
    "__version__",
    "amapper",
    "acquire_raw_connection",
    "bind_connection",
    "close_database",
    "configure",
    "current_connection",
    "get_pool",
    "load_all_mappers",
    "load_mapper",
    "render_sql",
    "open_database",
    "ping_database",
    "query",
    "require_connection",
    "reset_state",
    "runtime",
    "scalar",
    "transactional",
    "transactional_scope",
    "validate_result_types",
]
