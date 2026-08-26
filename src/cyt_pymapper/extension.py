"""Application bootstrap for mapper discovery, validation and pool lifecycle."""
from __future__ import annotations

import importlib
import logging
import pkgutil
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from cyt_pymapper.database import (
    PoolLike,
    close_database,
    configure_database,
    configure_pool,
    open_database,
    ping_database,
)
from cyt_pymapper.runtime import configure_mapper_paths, load_all_mappers

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MapperStartupState:
    """Observable result of one mapper application startup."""

    statement_count: int
    database_ready: bool
    database_error: Exception | None = None


def _import_mapper_package(package_ref: str | ModuleType) -> tuple[str, ...]:
    package = importlib.import_module(package_ref) if isinstance(package_ref, str) else package_ref
    imported = [package.__name__]
    package_paths = getattr(package, "__path__", None)
    if package_paths is None:
        return tuple(imported)
    prefix = f"{package.__name__}."
    module_names = sorted(
        module_info.name
        for module_info in pkgutil.walk_packages(package_paths, prefix=prefix)
    )
    for module_name in module_names:
        importlib.import_module(module_name)
    imported.extend(module_names)
    return tuple(imported)


class PyMapperExtension:
    """Configure cyt-pymapper once and own its application lifecycle.

    Construction only records configuration; it never performs network I/O.
    ``lifespan()`` imports every configured mapper package, validates all XML,
    opens the asyncpg pool and closes the owned pool on exit.
    """

    def __init__(
        self,
        *,
        mapper_paths: Sequence[str | Path],
        mapper_packages: Sequence[str | ModuleType] = (),
        database_url: str | None = None,
        pool: PoolLike | None = None,
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        command_timeout: float | None = None,
        ssl: Any = None,
        statement_cache_size: int = 100,
    ) -> None:
        if (database_url is None) == (pool is None):
            raise ValueError("PyMapperExtension requires exactly one of database_url or pool")
        self._mapper_packages = tuple(mapper_packages)
        self._imported_modules: tuple[str, ...] = ()
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

    @property
    def imported_modules(self) -> tuple[str, ...]:
        return self._imported_modules

    def load_mappers(self) -> int:
        """Import configured packages and validate every mapper/XML contract."""
        imported: list[str] = []
        for package_ref in self._mapper_packages:
            imported.extend(_import_mapper_package(package_ref))
        self._imported_modules = tuple(dict.fromkeys(imported))
        return load_all_mappers()

    async def startup(self, *, require_database: bool = True) -> MapperStartupState:
        """Load mappers and open the database pool.

        ``require_database=False`` is intended for applications whose health
        endpoint must report an unavailable database while the HTTP process stays
        alive. XML/import errors always fail startup because they are code defects.
        """
        statement_count = self.load_mappers()
        try:
            await open_database()
            database_ready = await ping_database()
            return MapperStartupState(statement_count, database_ready)
        except Exception as error:
            logger.exception("cyt-pymapper database startup failed")
            if require_database:
                raise
            return MapperStartupState(statement_count, False, error)

    async def shutdown(self) -> None:
        await close_database()

    @asynccontextmanager
    async def lifespan(
        self,
        *,
        require_database: bool = True,
    ) -> AsyncGenerator[MapperStartupState]:
        """Framework-neutral lifespan usable by FastAPI and other ASGI hosts."""
        try:
            yield await self.startup(require_database=require_database)
        finally:
            await self.shutdown()


__all__ = ["MapperStartupState", "PyMapperExtension"]
