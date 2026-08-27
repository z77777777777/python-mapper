"""Structured, parameter-safe SQL execution logging."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any

from cyt_pymapper.plugins import StatementContext, StatementExecutor, StatementResult


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def _fingerprint(sql: str) -> str:
    return hashlib.sha256(_normalized_sql(sql).encode("utf-8")).hexdigest()[:16]


class SqlLoggingPlugin:
    """Observe final SQL shape without logging parameter values by default."""

    def __init__(
        self,
        *,
        slow_query_threshold_ms: float = 500.0,
        logger_name: str = "cyt_pymapper.query",
        include_sql: bool = True,
    ) -> None:
        if slow_query_threshold_ms < 0:
            raise ValueError("slow_query_threshold_ms must be non-negative")
        self.slow_query_threshold_ms = slow_query_threshold_ms
        self.logger = logging.getLogger(logger_name)
        self.include_sql = include_sql

    def _event(
        self,
        context: StatementContext,
        *,
        duration_ms: float,
        row_count: int | None,
        slow: bool,
        error_type: str | None = None,
    ) -> dict[str, Any]:
        sql = context.compiled_sql or context.sql
        event: dict[str, Any] = {
            "event": "pymapper.query",
            "statement_id": context.statement_id,
            "operation": context.operation,
            "parent_statement_id": context.parent_statement_id,
            "sql_fingerprint": _fingerprint(sql),
            "duration_ms": round(duration_ms, 3),
            "connection_wait_ms": round(context.connection_wait_ms, 3),
            "row_count": row_count,
            "parameter_names": sorted(context.parameters),
            "parameter_types": [
                type(context.parameters[name]).__name__
                for name in sorted(context.parameters)
            ],
            "slow": slow,
            "error_type": error_type,
        }
        if self.include_sql:
            event["sql"] = _normalized_sql(sql)
        return event

    async def execute(
        self,
        context: StatementContext,
        call_next: StatementExecutor,
    ) -> StatementResult:
        started_at = time.perf_counter()
        try:
            result = await call_next(context)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            duration_ms = (time.perf_counter() - started_at) * 1000
            event = self._event(
                context,
                duration_ms=duration_ms,
                row_count=None,
                slow=duration_ms >= self.slow_query_threshold_ms,
                error_type=type(error).__name__,
            )
            self.logger.exception(
                "pymapper query failed statement_id=%s fingerprint=%s duration_ms=%.3f",
                context.statement_id,
                event["sql_fingerprint"],
                duration_ms,
                extra={"pymapper": event},
            )
            raise

        duration_ms = (time.perf_counter() - started_at) * 1000
        slow = duration_ms >= self.slow_query_threshold_ms
        event = self._event(
            context,
            duration_ms=duration_ms,
            row_count=result.rowcount,
            slow=slow,
        )
        level = logging.WARNING if slow else logging.DEBUG
        self.logger.log(
            level,
            "pymapper query statement_id=%s fingerprint=%s duration_ms=%.3f rows=%d",
            context.statement_id,
            event["sql_fingerprint"],
            duration_ms,
            result.rowcount,
            extra={"pymapper": event},
        )
        return result


__all__ = ["SqlLoggingPlugin"]
