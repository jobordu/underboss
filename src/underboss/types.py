"""Public types for underboss — queue policies, job states, and option objects."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class JobState(StrEnum):
    """Lifecycle state of a job.

    The ordering is significant: the database enum is numeric under the hood and
    the SQL layer relies on comparisons such as ``state < 'active'`` (queued) or
    ``state > 'active'`` (finished). Do not reorder.
    """

    CREATED = "created"
    RETRY = "retry"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class QueuePolicy(StrEnum):
    """How a queue admits and dispatches jobs.

    - ``STANDARD``        — full feature set: deferral, priority, throttling.
    - ``SHORT``           — at most one job queued; unlimited active.
    - ``SINGLETON``       — at most one job active; unlimited queued.
    - ``STATELY``         — at most one job per state, queued and/or active.
    - ``EXCLUSIVE``       — at most one job queued or active.
    - ``KEY_STRICT_FIFO`` — strict FIFO per ``singleton_key``; a key is required.
    """

    STANDARD = "standard"
    SHORT = "short"
    SINGLETON = "singleton"
    STATELY = "stately"
    EXCLUSIVE = "exclusive"
    KEY_STRICT_FIFO = "key_strict_fifo"


@dataclass(slots=True)
class QueueOptions:
    """Retry, retention, and expiration policy for a queue.

    Every job inherits these unless overridden at send time.
    """

    policy: QueuePolicy = QueuePolicy.STANDARD
    retry_limit: int = 2
    retry_delay: int = 0
    retry_backoff: bool = False
    retry_delay_max: int | None = None
    expire_in_seconds: int = 15 * 60
    retention_seconds: int = 14 * 24 * 60 * 60
    delete_after_seconds: int = 7 * 24 * 60 * 60
    dead_letter: str | None = None
    warning_queue_size: int = 0
    heartbeat_seconds: int | None = None


@dataclass(slots=True)
class Queue:
    """A named queue and its options."""

    name: str
    options: QueueOptions = field(default_factory=QueueOptions)


@dataclass(slots=True)
class GroupOptions:
    """Assigns a job to a concurrency group."""

    id: str
    tier: str | None = None


@dataclass(slots=True)
class SendOptions:
    """Per-job options accepted by :meth:`Underboss.send`."""

    priority: int = 0
    start_after: datetime | int | str | None = None
    singleton_key: str | None = None
    singleton_seconds: int | None = None
    singleton_next_slot: bool = False
    keep_until: datetime | int | str | None = None
    dead_letter: str | None = None
    group: GroupOptions | None = None
    # Per-job overrides of the queue's policy.
    retry_limit: int | None = None
    retry_delay: int | None = None
    retry_backoff: bool | None = None
    expire_in_seconds: int | None = None


@dataclass(slots=True)
class JobInsert:
    """A single job for bulk insertion via :meth:`Underboss.insert`."""

    data: Mapping[str, Any] | None = None
    id: str | None = None
    priority: int = 0
    retry_limit: int | None = None
    retry_delay: int | None = None
    retry_backoff: bool | None = None
    start_after: datetime | int | str | None = None
    singleton_key: str | None = None
    expire_in_seconds: int | None = None
    keep_until: datetime | int | str | None = None
    dead_letter: str | None = None


@dataclass(slots=True)
class WorkOptions:
    """Options controlling a worker started by :meth:`Underboss.work`."""

    batch_size: int = 1
    poll_interval_seconds: float = 2.0
    include_metadata: bool = False
    priority: bool = True
    local_concurrency: int = 1


@dataclass(slots=True)
class Schedule:
    """A cron schedule attached to a queue."""

    name: str
    cron: str
    key: str = ""
    timezone: str | None = None
    data: Mapping[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class Job:
    """A job handed to a work handler.

    Core fields are always populated. The metadata fields (``state`` through
    ``created_on``) are populated only when the worker runs with
    ``include_metadata`` enabled.
    """

    id: str
    name: str
    data: Any
    expire_in_seconds: int
    group_id: str | None = None
    group_tier: str | None = None
    # Metadata — only populated when include_metadata=True.
    state: JobState | None = None
    priority: int | None = None
    retry_limit: int | None = None
    retry_count: int | None = None
    started_on: datetime | None = None
    created_on: datetime | None = None


#: Signature of a work handler: it receives a batch of jobs.
WorkHandler = Callable[[list[Job]], Awaitable[Any]]
