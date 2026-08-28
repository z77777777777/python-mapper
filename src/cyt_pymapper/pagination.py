"""Opt-in pagination models and statement plugin."""
from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field

from cyt_pymapper.compiler import (
    contains_top_level_keyword,
    contains_top_level_sequence,
)
from cyt_pymapper.errors import PaginationConflictError, PaginationError
from cyt_pymapper.plugins import StatementContext, StatementExecutor, StatementResult

DEFAULT_PAGE_SIZE = 30
MAX_PAGE_SIZE = 200
PAGE_MARKER = "/*__CYT_PYMAPPER_PAGE__*/"
PAGE_LIMIT_PARAMETER = "__page_size"
PAGE_OFFSET_PARAMETER = "__page_offset"


@dataclass(frozen=True, slots=True)
class PaginationOptions:
    """Per-call pagination switch; disabled calls must remain raw mapper calls."""

    enabled: bool = False
    page_number: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    include_total: bool = True

    def normalized(self) -> PaginationOptions:
        if self.page_number < 1:
            raise PaginationError("page_number must be greater than or equal to 1")
        if self.page_size < 1 or self.page_size > MAX_PAGE_SIZE:
            raise PaginationError(
                f"page_size must be between 1 and {MAX_PAGE_SIZE}"
            )
        return PaginationOptions(
            enabled=self.enabled,
            page_number=self.page_number,
            page_size=self.page_size,
            include_total=self.include_total,
        )


@dataclass(frozen=True, slots=True)
class PageMetadata:
    page_number: int
    page_size: int
    total: int | None
    total_pages: int | None
    has_next: bool


@dataclass(slots=True)
class Page[ItemValue]:
    """Framework-neutral page returned directly by a ``Page[T]`` mapper."""

    items: list[ItemValue] = field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    pages: int = 0


@dataclass(frozen=True, slots=True)
class QueryResult[ItemValue]:
    """Stable result from the optional ``query`` facade."""

    items: list[ItemValue]
    pagination: PageMetadata | None = None

    def map[MappedValue](
        self,
        converter: Callable[[ItemValue], MappedValue],
    ) -> QueryResult[MappedValue]:
        return QueryResult(
            items=[converter(item) for item in self.items],
            pagination=self.pagination,
        )


@dataclass(frozen=True, slots=True)
class PaginationSpec:
    statement_id: str
    count_statement_id: str | None = None
    marker_count: int = 0


def _scalar_count(result: StatementResult, statement_id: str) -> int:
    if not result.rows:
        raise PaginationError(
            f"pagination count statement '{statement_id}' returned no rows"
        )
    row = result.rows[0]
    values = list(dict(row).values())
    if len(values) != 1:
        raise PaginationError(
            f"pagination count statement '{statement_id}' must return exactly one column"
        )
    try:
        return int(values[0])
    except (TypeError, ValueError) as error:
        raise PaginationError(
            f"pagination count statement '{statement_id}' returned a non-integer value"
        ) from error


def _append_page_clause(sql: str, clause: str) -> str:
    stripped = sql.rstrip()
    if stripped.endswith(";"):
        return f"{stripped[:-1].rstrip()}\n{clause};"
    return f"{stripped}\n{clause}"


class PaginationPlugin:
    """Add LIMIT/OFFSET only for one explicitly enabled query invocation."""

    def __init__(self, options: PaginationOptions, spec: PaginationSpec) -> None:
        self.options = options.normalized()
        self.spec = spec

    async def execute(
        self,
        context: StatementContext,
        call_next: StatementExecutor,
    ) -> StatementResult:
        if not self.options.enabled:
            return await call_next(context)
        if context.statement_kind != "select":
            raise PaginationError(
                f"pagination only supports <select>: {context.statement_id}"
            )

        sql = context.sql
        marker_count = sql.count(PAGE_MARKER)
        has_manual_page = any(
            contains_top_level_keyword(sql, keyword)
            for keyword in ("LIMIT", "OFFSET", "FETCH")
        )
        if marker_count and has_manual_page:
            raise PaginationConflictError(
                f"mapper '{context.statement_id}' contains both <page/> and top-level "
                "LIMIT/OFFSET/FETCH"
            )
        if marker_count > 1:
            raise PaginationConflictError(
                f"mapper '{context.statement_id}' contains more than one <page/> marker"
            )
        if has_manual_page:
            raise PaginationConflictError(
                f"mapper '{context.statement_id}' already contains top-level "
                "LIMIT/OFFSET/FETCH; disable pagination or remove the manual clause"
            )
        if not contains_top_level_sequence(sql, "ORDER", "BY"):
            raise PaginationError(
                f"pagination statement '{context.statement_id}' requires a top-level ORDER BY"
            )
        if marker_count == 0 and contains_top_level_sequence(sql, "FOR", "UPDATE"):
            raise PaginationError(
                f"pagination statement '{context.statement_id}' uses FOR UPDATE; "
                "add <page/> to declare the clause position"
            )

        total: int | None = None
        if self.options.include_total:
            count_statement_id = self.spec.count_statement_id
            if count_statement_id is None:
                raise PaginationError(
                    f"pagination statement '{context.statement_id}' requires countRef "
                    "when include_total=True"
                )
            count_result = await context.execute_related(
                count_statement_id,
                context.input_parameters,
                "pagination.count",
                context.statement_id,
            )
            total = _scalar_count(count_result, count_statement_id)

        page_size = self.options.page_size
        offset = (self.options.page_number - 1) * page_size
        query_limit = page_size if total is not None else page_size + 1
        clause = f"LIMIT :{PAGE_LIMIT_PARAMETER} OFFSET :{PAGE_OFFSET_PARAMETER}"
        context.sql = (
            sql.replace(PAGE_MARKER, clause)
            if marker_count
            else _append_page_clause(sql, clause)
        )
        context.parameters[PAGE_LIMIT_PARAMETER] = query_limit
        context.parameters[PAGE_OFFSET_PARAMETER] = offset
        result = await call_next(context)

        rows = list(result.rows or [])
        if total is None:
            has_next = len(rows) > page_size
            rows = rows[:page_size]
            result = StatementResult(rows=rows, rowcount=len(rows))
            total_pages = None
        else:
            has_next = offset + len(rows) < total
            total_pages = math.ceil(total / page_size) if total else 0

        context.attributes["pagination"] = PageMetadata(
            page_number=self.options.page_number,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
            has_next=has_next,
        )
        return result


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "PAGE_MARKER",
    "Page",
    "PageMetadata",
    "PaginationOptions",
    "PaginationPlugin",
    "PaginationSpec",
    "QueryResult",
]
