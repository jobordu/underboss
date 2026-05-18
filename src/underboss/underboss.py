"""The :class:`Underboss` class — underboss's public entry point."""

from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import Any

import asyncpg

from underboss import schema as ddl
from underboss.db import Database
from underboss.errors import MigrationRequiredError, NotStartedError
from underboss.schema import DEFAULT_SCHEMA, SCHEMA_VERSION
from underboss.types import QueueOptions, SendOptions, WorkHandler, WorkOptions

_PLANNED = "lands with the manager/worker layer on the way to 0.1.0"


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
        """Close the connection pool (if underboss owns it)."""
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
        """Create a queue. Idempotent."""
        self._require_started()
        raise NotImplementedError(f"create_queue {_PLANNED}")

    async def send(
        self,
        name: str,
        data: Mapping[str, Any] | None = None,
        options: SendOptions | None = None,
    ) -> str | None:
        """Enqueue a job; returns its id, or ``None`` if dedup suppressed it."""
        self._require_started()
        raise NotImplementedError(f"send {_PLANNED}")

    async def work(
        self,
        name: str,
        handler: WorkHandler,
        options: WorkOptions | None = None,
    ) -> str:
        """Start a worker that polls ``name`` and dispatches jobs to ``handler``."""
        self._require_started()
        raise NotImplementedError(f"work {_PLANNED}")

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
