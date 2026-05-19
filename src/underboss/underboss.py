"""The :class:`Underboss` class — underboss's public entry point."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import TracebackType
from typing import Any

import asyncpg

from underboss import schema as ddl
from underboss import sql
from underboss.db import Database
from underboss.errors import MigrationRequiredError, NotStartedError
from underboss.schema import DEFAULT_SCHEMA, SCHEMA_VERSION
from underboss.types import QueueOptions, SendOptions, WorkHandler, WorkOptions
from underboss.worker import Worker

_PLANNED = "lands in a later wave on the way to 0.1.0"


def _isoformat_z(value: datetime) -> str:
    """Render a datetime as an ISO-8601 string ending in ``Z`` (UTC)."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _encode_start_after(value: datetime | int | str | None) -> str | None:
    """Encode a ``start_after`` value the way the insert query expects it.

    A datetime becomes a ``Z``-suffixed ISO string (parsed as an absolute time);
    an int or str becomes a relative interval (e.g. ``"30"`` → 30s from now).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return _isoformat_z(value)
    return str(value)


def _queue_options_payload(options: QueueOptions) -> dict[str, Any]:
    """Translate :class:`QueueOptions` into the camelCase JSON ``create_queue`` expects."""
    payload: dict[str, Any] = {
        "policy": options.policy.value,
        "retryLimit": options.retry_limit,
        "retryDelay": options.retry_delay,
        "retryBackoff": options.retry_backoff,
        "expireInSeconds": options.expire_in_seconds,
        "retentionSeconds": options.retention_seconds,
        "deleteAfterSeconds": options.delete_after_seconds,
        "warningQueueSize": options.warning_queue_size,
    }
    if options.retry_delay_max is not None:
        payload["retryDelayMax"] = options.retry_delay_max
    if options.dead_letter is not None:
        payload["deadLetter"] = options.dead_letter
    if options.heartbeat_seconds is not None:
        payload["heartbeatSeconds"] = options.heartbeat_seconds
    return payload


def _job_payload(data: Mapping[str, Any] | None, options: SendOptions) -> dict[str, Any]:
    """Translate a job's data and :class:`SendOptions` into the insert query's job spec."""
    payload: dict[str, Any] = {"priority": options.priority}
    if data is not None:
        payload["data"] = data
    start_after = _encode_start_after(options.start_after)
    if start_after is not None:
        payload["startAfter"] = start_after
    if options.singleton_key is not None:
        payload["singletonKey"] = options.singleton_key
    if options.singleton_seconds is not None:
        payload["singletonSeconds"] = options.singleton_seconds
        if options.singleton_next_slot:
            payload["singletonOffset"] = options.singleton_seconds
    if options.dead_letter is not None:
        payload["deadLetter"] = options.dead_letter
    if options.group is not None:
        payload["groupId"] = options.group.id
        if options.group.tier is not None:
            payload["groupTier"] = options.group.tier
    if options.retry_limit is not None:
        payload["retryLimit"] = options.retry_limit
    if options.retry_delay is not None:
        payload["retryDelay"] = options.retry_delay
    if options.retry_backoff is not None:
        payload["retryBackoff"] = options.retry_backoff
    if options.expire_in_seconds is not None:
        payload["expireInSeconds"] = options.expire_in_seconds
    return payload


class Underboss:
    """An async, Postgres-backed job queue.

    Create an instance with a DSN (underboss owns the connection pool) or an
    existing :class:`asyncpg.Pool`, then :meth:`start` it::

        boss = await Underboss("postgresql://localhost/mydb").start()
        ...
        await boss.stop()

    It also works as an async context manager.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: asyncpg.Pool | None = None,
        schema: str = DEFAULT_SCHEMA,
        min_pool_size: int = 2,
        max_pool_size: int = 10,
    ) -> None:
        self._schema = schema
        self._db = Database(dsn, pool=pool, min_size=min_pool_size, max_size=max_pool_size)
        self._workers: dict[str, Worker] = {}
        self._started = False

    @property
    def schema(self) -> str:
        """The Postgres schema (namespace) underboss is installed in."""
        return self._schema

    @property
    def started(self) -> bool:
        """Whether :meth:`start` has completed."""
        return self._started

    async def start(self) -> Underboss:
        """Open the connection pool and install the schema if it is absent."""
        await self._db.open()
        await self._provision()
        self._started = True
        return self

    async def stop(self) -> None:
        """Stop every running worker, then close the connection pool."""
        workers = list(self._workers.values())
        self._workers.clear()
        for worker in workers:
            await worker.stop()
        await self._db.close()
        self._started = False

    async def __aenter__(self) -> Underboss:
        return await self.start()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def _provision(self) -> None:
        """Install the schema, or verify an existing install is the right version."""
        installed = await self._db.fetchval(ddl.version_table_exists(self._schema))
        if not installed:
            await self._db.run_script(ddl.build_schema(self._schema, SCHEMA_VERSION))
            return
        version = await self._db.fetchval(ddl.get_version(self._schema))
        if version is not None and int(version) != SCHEMA_VERSION:
            raise MigrationRequiredError(
                f"database schema '{self._schema}' is at version {version}, "
                f"but this build of underboss expects {SCHEMA_VERSION}"
            )

    def _require_started(self) -> None:
        if not self._started:
            raise NotStartedError("Underboss is not started; call start() first")

    # ----------------------------------------------------------------------
    # Producer / worker API — stubbed; implemented incrementally toward 0.1.0.
    # ----------------------------------------------------------------------
    async def create_queue(self, name: str, options: QueueOptions | None = None) -> None:
        """Create a queue, or do nothing if a queue with this name already exists."""
        self._require_started()
        payload = _queue_options_payload(options or QueueOptions())
        await self._db.execute(sql.create_queue(self._schema), name, payload)

    async def send(
        self,
        name: str,
        data: Mapping[str, Any] | None = None,
        options: SendOptions | None = None,
    ) -> str | None:
        """Enqueue a job on queue ``name``.

        Returns the new job's id, or ``None`` when a queue-policy index
        suppressed it as a duplicate.
        """
        self._require_started()
        payload = _job_payload(data, options or SendOptions())
        row = await self._db.fetchrow(sql.insert_jobs(self._schema), [payload], name)
        return None if row is None else str(row["id"])

    async def work(
        self,
        name: str,
        handler: WorkHandler,
        options: WorkOptions | None = None,
    ) -> str:
        """Start a worker that polls ``name`` and dispatches jobs to ``handler``.

        Returns the worker's id. The worker runs in the background until
        :meth:`stop_worker` or :meth:`stop` is called.
        """
        self._require_started()
        worker = Worker(self._db, self._schema, name, handler, options or WorkOptions())
        worker.start()
        self._workers[worker.id] = worker
        return worker.id

    async def stop_worker(self, worker_id: str) -> None:
        """Stop a single worker by id."""
        worker = self._workers.pop(worker_id, None)
        if worker is not None:
            await worker.stop()

    async def schedule(
        self,
        name: str,
        cron: str,
        *,
        data: Mapping[str, Any] | None = None,
        key: str = "",
        timezone: str | None = None,
    ) -> None:
        """Attach a cron schedule to a queue."""
        self._require_started()
        raise NotImplementedError(f"schedule {_PLANNED}")
