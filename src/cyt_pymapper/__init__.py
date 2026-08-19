"""PostgreSQL-first XML mapper and implicit SQLAlchemy transaction runtime."""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from cyt_pymapper import runtime
from cyt_pymapper.base import (
    MapperBase,
    SessionFactory,
    bind_session,
    configure_session_factory,
    current_session,
    require_session,
    transactional,
    transactional_scope,
)
from cyt_pymapper.errors import PyMapperError, TooManyResultsError
from cyt_pymapper.runtime import (
    AMapper,
    amapper,
    configure_mapper_paths,
    load_all_mappers,
    load_mapper,
    render_sql,
    reset_state,
    scalar,
    validate_result_types,
)

__version__ = "0.1.0"


def configure(
    *,
    session_factory: SessionFactory,
    mapper_paths: Sequence[str | Path],
) -> None:
    """Bind the framework to one application's session factory and XML roots."""
    configure_session_factory(session_factory)
    configure_mapper_paths(tuple(mapper_paths))


__all__ = [
    "AMapper",
    "MapperBase",
    "PyMapperError",
    "TooManyResultsError",
    "__version__",
    "amapper",
    "bind_session",
    "configure",
    "current_session",
    "load_all_mappers",
    "load_mapper",
    "render_sql",
    "require_session",
    "reset_state",
    "runtime",
    "scalar",
    "transactional",
    "transactional_scope",
    "validate_result_types",
]
