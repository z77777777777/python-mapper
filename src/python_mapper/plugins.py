"""Framework-neutral statement execution plugin contracts."""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from python_mapper.database import ConnectionLike


@dataclass(slots=True)
class StatementResult:
    """Raw database result before resultType/resultMap materialization."""

    rows: list[Any] | None
    rowcount: int

    @property
    def returns_rows(self) -> bool:
        return self.rows is not None


RelatedStatementExecutor = Callable[
    [str, Mapping[str, Any], str, str | None],
    Awaitable[StatementResult],
]


@dataclass(slots=True)
class StatementContext:
    """Mutable execution state shared by one statement plugin chain."""

    statement_id: str
    statement_kind: str | None
    sql: str
    input_parameters: dict[str, Any]
    parameters: dict[str, Any]
    connection: ConnectionLike
    execute_related: RelatedStatementExecutor
    operation: str = "query"
    parent_statement_id: str | None = None
    connection_wait_ms: float = 0.0
    compiled_sql: str | None = None
    compiled_args: tuple[Any, ...] = ()
    attributes: dict[str, Any] = field(default_factory=dict)


StatementExecutor = Callable[[StatementContext], Awaitable[StatementResult]]


class StatementPlugin(Protocol):
    """Around-execution extension point.

    Plugin instances are process-level configuration and may serve concurrent
    statements. Implementations should therefore remain stateless or protect
    their own mutable state, and must call ``call_next`` exactly once unless they
    intentionally short-circuit execution.
    """

    async def execute(
        self,
        context: StatementContext,
        call_next: StatementExecutor,
    ) -> StatementResult: ...


async def run_plugin_chain(
    context: StatementContext,
    plugins: Sequence[StatementPlugin],
    terminal: StatementExecutor,
) -> StatementResult:
    """Run a reusable, non-destructive interceptor chain."""

    async def invoke(index: int, current: StatementContext) -> StatementResult:
        if index >= len(plugins):
            return await terminal(current)
        plugin = plugins[index]

        async def call_next(next_context: StatementContext) -> StatementResult:
            return await invoke(index + 1, next_context)

        return await plugin.execute(current, call_next)

    return await invoke(0, context)


__all__ = [
    "RelatedStatementExecutor",
    "StatementContext",
    "StatementExecutor",
    "StatementPlugin",
    "StatementResult",
    "run_plugin_chain",
]
