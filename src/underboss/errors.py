"""Exception hierarchy for underboss."""

from __future__ import annotations


class UnderbossError(Exception):
    """Base class for every error raised by underboss."""


class NotStartedError(UnderbossError):
    """Raised when an operation is attempted before :meth:`Underboss.start`."""


class NotInstalledError(UnderbossError):
    """Raised when the underboss schema is absent from the target database."""


class MigrationRequiredError(UnderbossError):
    """Raised when the installed schema version differs from the expected one."""


class QueueNotFoundError(UnderbossError):
    """Raised when a referenced queue does not exist."""


class JobNotFoundError(UnderbossError):
    """Raised when a referenced job does not exist."""
