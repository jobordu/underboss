"""underboss — an async, Postgres-backed job queue for Python.

A port of pg-boss (https://github.com/timgit/pg-boss).
"""

from __future__ import annotations

from underboss.errors import (
    JobNotFoundError,
    MigrationRequiredError,
    NotInstalledError,
    NotStartedError,
    QueueNotFoundError,
    UnderbossError,
)
from underboss.schema import SCHEMA_VERSION
from underboss.types import (
    GroupOptions,
    Job,
    JobInsert,
    JobState,
    Queue,
    QueueOptions,
    QueuePolicy,
    Schedule,
    SendOptions,
    WorkHandler,
    WorkOptions,
)
from underboss.underboss import Underboss

__version__ = "0.1.0"

__all__ = [
    "SCHEMA_VERSION",
    "GroupOptions",
    "Job",
    "JobInsert",
    "JobNotFoundError",
    "JobState",
    "MigrationRequiredError",
    "NotInstalledError",
    "NotStartedError",
    "Queue",
    "QueueNotFoundError",
    "QueueOptions",
    "QueuePolicy",
    "Schedule",
    "SendOptions",
    "Underboss",
    "UnderbossError",
    "WorkHandler",
    "WorkOptions",
    "__version__",
]
