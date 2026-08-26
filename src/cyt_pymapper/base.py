"""Implicit asyncpg connection and explicit transaction boundaries."""
from __future__ import annotations

import functools
import inspect
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from typing import ParamSpec, TypeVar

from cyt_pymapper.database import ConnectionLike, acquire_raw_connection

CallableParameters = ParamSpec("CallableParameters")
ReturnValue = TypeVar("ReturnValue")

_ACTIVE_CONNECTION: ContextVar[ConnectionLike | None] = ContextVar(
    "mapper_active_connection", default=None
)
_ACTIVE_TRANSACTION_OPTIONS: ContextVar[tuple[str | None, bool] | None] = ContextVar(
    "mapper_active_transaction_options", default=None
)
_VALID_ISOLATION_LEVELS = {"READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}


def _normalize_isolation_level(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper().replace("_", " ")
    if normalized not in _VALID_ISOLATION_LEVELS:
        raise ValueError(
            "isolation_level must be READ COMMITTED, REPEATABLE READ or SERIALIZABLE"
        )
    return normalized


def _asyncpg_isolation(value: str | None) -> str | None:
    return value.lower().replace(" ", "_") if value is not None else None


class MapperBase:
    """Base contract for mappers that consume an implicit asyncpg connection."""

    @classmethod
    @asynccontextmanager
    async def acquire_connection(cls) -> AsyncGenerator[ConnectionLike]:
        """Reuse an active transaction or borrow a connection for one statement."""
        current = _ACTIVE_CONNECTION.get()
        if current is not None:
            yield current
            return
        async with acquire_raw_connection() as connection:
            yield connection

    @classmethod
    @asynccontextmanager
    async def transaction_scope(
        cls,
        *,
        requires_new: bool = False,
        isolation_level: str | None = None,
        read_only: bool = False,
    ) -> AsyncGenerator[ConnectionLike]:
        """Open one transaction, reusing the current one for REQUIRED semantics."""
        normalized_isolation = _normalize_isolation_level(isolation_level)
        current = _ACTIVE_CONNECTION.get()
        if current is not None and not requires_new:
            active_options = _ACTIVE_TRANSACTION_OPTIONS.get()
            if (
                normalized_isolation is not None
                and active_options is not None
                and active_options[0] != normalized_isolation
            ):
                raise RuntimeError(
                    "cannot change transaction isolation while joining an active transaction"
                )
            if read_only and active_options is not None and not active_options[1]:
                raise RuntimeError("cannot join a read-write transaction as read-only")
            yield current
            return

        async with acquire_raw_connection() as connection:
            connection_token = _ACTIVE_CONNECTION.set(connection)
            options_token = _ACTIVE_TRANSACTION_OPTIONS.set(
                (normalized_isolation, read_only)
            )
            try:
                async with connection.transaction(
                    isolation=_asyncpg_isolation(normalized_isolation),
                    readonly=read_only,
                ):
                    yield connection
            finally:
                _ACTIVE_TRANSACTION_OPTIONS.reset(options_token)
                _ACTIVE_CONNECTION.reset(connection_token)


def current_connection() -> ConnectionLike | None:
    return _ACTIVE_CONNECTION.get()


def require_connection() -> ConnectionLike:
    connection = current_connection()
    if connection is None:
        raise RuntimeError("当前调用不在 pymapper 事务上下文中")
    return connection


@asynccontextmanager
async def bind_connection(connection: ConnectionLike) -> AsyncGenerator[ConnectionLike]:
    """Borrow an externally managed connection without commit, rollback or close."""
    connection_token: Token[ConnectionLike | None] = _ACTIVE_CONNECTION.set(connection)
    options_token = _ACTIVE_TRANSACTION_OPTIONS.set((None, False))
    try:
        yield connection
    finally:
        _ACTIVE_TRANSACTION_OPTIONS.reset(options_token)
        _ACTIVE_CONNECTION.reset(connection_token)


def transactional(
    *,
    propagation: str = "REQUIRED",
    isolation_level: str | None = None,
    read_only: bool = False,
):
    """Wrap an async service method in an asyncpg transaction.

    ``REQUIRED`` joins the task-local transaction. ``REQUIRES_NEW`` suspends it
    and borrows another connection for an independent transaction.

    Do not create child tasks that call mappers inside this boundary. ContextVars
    are copied into child tasks, which would make concurrent tasks share one
    asyncpg connection and fail with an InterfaceError or use it after release.
    """
    normalized = propagation.strip().upper()
    if normalized not in {"REQUIRED", "REQUIRES_NEW"}:
        raise ValueError("propagation must be REQUIRED or REQUIRES_NEW")
    normalized_isolation = _normalize_isolation_level(isolation_level)

    def decorator(
        func: Callable[CallableParameters, Awaitable[ReturnValue]],
    ) -> Callable[CallableParameters, Awaitable[ReturnValue]]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"transactional 只能装饰 async 函数, {getattr(func, '__qualname__', func)!r} "
                "是同步的"
            )

        @functools.wraps(func)
        async def wrapper(
            *args: CallableParameters.args,
            **kwargs: CallableParameters.kwargs,
        ) -> ReturnValue:
            async with MapperBase.transaction_scope(
                requires_new=normalized == "REQUIRES_NEW",
                isolation_level=normalized_isolation,
                read_only=read_only,
            ):
                return await func(*args, **kwargs)

        return wrapper

    return decorator


transactional_scope = MapperBase.transaction_scope


__all__ = [
    "MapperBase",
    "bind_connection",
    "current_connection",
    "require_connection",
    "transactional",
    "transactional_scope",
]
