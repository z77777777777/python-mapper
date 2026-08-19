"""Public exception types raised by cyt-pymapper."""
from __future__ import annotations


class PyMapperError(RuntimeError):
    """Base class for cyt-pymapper runtime errors."""


class TooManyResultsError(PyMapperError):
    """A ``single=true`` statement returned more than one database row."""


__all__ = ["PyMapperError", "TooManyResultsError"]
