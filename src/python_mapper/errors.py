"""Public exception types raised by python-mapper."""
from __future__ import annotations


class PyMapperError(RuntimeError):
    """Base class for python-mapper runtime errors."""


class TooManyResultsError(PyMapperError):
    """A ``single=true`` statement returned more than one database row."""


class PaginationError(PyMapperError):
    """A paginated query declaration or invocation is invalid."""


class PaginationConflictError(PaginationError):
    """Framework pagination conflicts with a manual SQL pagination clause."""


__all__ = [
    "PaginationConflictError",
    "PaginationError",
    "PyMapperError",
    "TooManyResultsError",
]
