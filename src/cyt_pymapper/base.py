"""Mapper session and transaction boundaries.

``MapperBase`` never stores a shared ``AsyncSession``.  The active session lives
in a ``ContextVar`` so mapper calls in the same async task can reuse a service
transaction without passing infrastructure objects through every method.
"""
from __future__ import annotations

import functools
import inspect
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from typing import ParamSpec, TypeVar

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

CallableParameters = ParamSpec("CallableParameters")
ReturnValue = TypeVar("ReturnValue")
SessionFactory = Callable[[], AsyncSession]

_SESSION_FACTORY: SessionFactory | None = None

_ACTIVE_SESSION: ContextVar[AsyncSession | None] = ContextVar(
    "mapper_active_session", default=None
)
_ACTIVE_TRANSACTION_OPTIONS: ContextVar[tuple[str | None, bool] | None] = ContextVar(
    "mapper_active_transaction_options", default=None
)
_VALID_ISOLATION_LEVELS = {"READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}


def configure_session_factory(session_factory: SessionFactory) -> None:
    """Register the project-owned SQLAlchemy async session factory."""
    if not callable(session_factory):
        raise TypeError("session_factory must be callable")
    global _SESSION_FACTORY
    _SESSION_FACTORY = session_factory


def clear_session_factory() -> None:
    """Unbind the session factory (used by ``cyt_pymapper.reset_state``)."""
    global _SESSION_FACTORY
    _SESSION_FACTORY = None


def _require_session_factory() -> SessionFactory:
    if _SESSION_FACTORY is None:
        raise RuntimeError(
            "cyt-pymapper is not configured: call configure(session_factory=..., mapper_paths=...)"
        )
    return _SESSION_FACTORY


def _normalize_isolation_level(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper().replace("_", " ")
    if normalized not in _VALID_ISOLATION_LEVELS:
        raise ValueError(
            "isolation_level must be READ COMMITTED, REPEATABLE READ or SERIALIZABLE"
        )
    return normalized


class MapperBase:
    """Base contract for mappers that consume an implicit session."""

    @classmethod
    @asynccontextmanager
    async def acquire_session(cls) -> AsyncGenerator[AsyncSession]:
        """Reuse the active transaction or own one short mapper-call session."""
        current = _ACTIVE_SESSION.get()
        if current is not None:
            yield current
            return

        async with cls._new_session_scope() as session:
            yield session

    @classmethod
    @asynccontextmanager
    async def transaction_scope(
        cls,
        *,
        requires_new: bool = False,
        isolation_level: str | None = None,
        read_only: bool = False,
    ) -> AsyncGenerator[AsyncSession]:
        """Open one transaction, reusing the current one for REQUIRED semantics."""
        normalized_isolation = _normalize_isolation_level(isolation_level)
        current = _ACTIVE_SESSION.get()
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
            yield current
            return

        async with cls._new_session_scope(
            isolation_level=normalized_isolation,
            read_only=read_only,
        ) as session:
            yield session

    @classmethod
    @asynccontextmanager
    async def _new_session_scope(
        cls,
        *,
        isolation_level: str | None = None,
        read_only: bool = False,
    ) -> AsyncGenerator[AsyncSession]:
        """Create, bind and release one primary-database session."""
        session_factory = _require_session_factory()
        async with session_factory() as session:
            session_token = _ACTIVE_SESSION.set(session)
            options_token = _ACTIVE_TRANSACTION_OPTIONS.set((isolation_level, read_only))
            try:
                async with session.begin():
                    transaction_modes = []
                    if isolation_level is not None:
                        transaction_modes.append(f"ISOLATION LEVEL {isolation_level}")
                    if read_only:
                        transaction_modes.append("READ ONLY")
                    if transaction_modes:
                        await session.execute(
                            text(f"SET TRANSACTION {', '.join(transaction_modes)}")
                        )
                    yield session
            finally:
                _ACTIVE_TRANSACTION_OPTIONS.reset(options_token)
                _ACTIVE_SESSION.reset(session_token)


def current_session() -> AsyncSession | None:
    """Return the task-local session, if a transaction/scope is active."""
    return _ACTIVE_SESSION.get()


def require_session() -> AsyncSession:
    """Return the active session for legacy consumers that still need it."""
    session = current_session()
    if session is None:
        raise RuntimeError("当前调用不在 mapper session/transaction 上下文中")
    return session


@asynccontextmanager
async def bind_session(session: AsyncSession) -> AsyncGenerator[AsyncSession]:
    """Borrow an externally managed session without taking ownership of it.

    This is primarily a compatibility and test boundary.  Commit, rollback and
    close remain the caller's responsibility.
    """
    token: Token[AsyncSession | None] = _ACTIVE_SESSION.set(session)
    # 借来的 session 隔离级别未知, options 记为 (None, False): 让 transaction_scope 的
    # "加入时不得改隔离级别"守卫在借用上下文里同样生效 —— 原先不设 options, 内层显式
    # 要求某个隔离级别会静默加入, 与 _new_session_scope 的行为不一致。
    options_token = _ACTIVE_TRANSACTION_OPTIONS.set((None, False))
    try:
        yield session
    finally:
        _ACTIVE_TRANSACTION_OPTIONS.reset(options_token)
        _ACTIVE_SESSION.reset(token)


def transactional(
    *,
    propagation: str = "REQUIRED",
    isolation_level: str | None = None,
    read_only: bool = False,
):
    """Wrap an async service method in a mapper transaction.

    ``REQUIRED`` joins an existing transaction; ``REQUIRES_NEW`` suspends the
    current context and owns a separate session/transaction for the call.

    ⚠ REQUIRED 加入即**继承外层事务的隔离级别与 read_only**(与 Spring/MyBatis 同语义);
      内层显式要求与外层不同的隔离级别会 raise, 不指定则跟随外层, 不会"降级"。
    ⚠ 事务范围内不要 ``asyncio.create_task`` 派生子任务去调 mapper: 子任务继承
      ContextVar 快照后会**并发使用同一个连接**(asyncpg 直接 InterfaceError),
      或在父事务关闭后使用已失效的 session。需要并行查询就并行开各自的 mapper 调用
      (每个自管短事务), 不要共享事务上下文。
    """
    normalized = propagation.strip().upper()
    if normalized not in {"REQUIRED", "REQUIRES_NEW"}:
        raise ValueError("propagation must be REQUIRED or REQUIRES_NEW")
    normalized_isolation = _normalize_isolation_level(isolation_level)

    def decorator(
        func: Callable[CallableParameters, Awaitable[ReturnValue]],
    ) -> Callable[CallableParameters, Awaitable[ReturnValue]]:
        # 装饰期就拒同步函数: wrapper 里 `await func(...)` 拿到非 awaitable 要等**调用时**
        # 才炸一个 "object X can't be used in 'await' expression", 冷路径可能上线数周后才踩到。
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"transactional 只能装饰 async 函数, {getattr(func, '__qualname__', func)!r} "
                "是同步的 —— 事务边界建立在协程上下文上, 同步函数请先改成 async def")

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
    "bind_session",
    "configure_session_factory",
    "current_session",
    "require_session",
    "transactional",
    "transactional_scope",
]
