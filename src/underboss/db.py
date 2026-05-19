"""Async database access for underboss, backed by asyncpg."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import asyncpg


async def _register_json_codecs(conn: asyncpg.Connection) -> None:
    """Decode json/jsonb columns to Python objects, and encode them on the way in."""
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )


class Database:
    """A thin async wrapper around an asyncpg connection pool.

    Either pass a ``dsn`` (underboss creates and owns the pool) or an existing
    ``pool`` (the caller retains ownership and underboss will not close it).
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: asyncpg.Pool | None = None,
        min_size: int = 2,
        max_size: int = 10,
    ) -> None:
        if dsn is None and pool is None:
            raise ValueError("Database requires either a dsn or an existing pool")
        self._dsn = dsn
        self._pool = pool
        self._owns_pool = pool is None
        self._min_size = min_size
        self._max_size = max_size

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database is not open; call open() first")
        return self._pool

    async def open(self) -> None:
        """Create the connection pool, unless one was supplied or already open."""
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            init=_register_json_codecs,
        )

    async def close(self) -> None:
        """Close the pool if underboss owns it."""
        if self._pool is not None and self._owns_pool:
            await self._pool.close()
        self._pool = None

    async def execute(
        self, sql: str, *args: Any, connection: asyncpg.Connection | None = None
    ) -> str:
        target = connection if connection is not None else self.pool
        return await target.execute(sql, *args)

    async def fetch(
        self, sql: str, *args: Any, connection: asyncpg.Connection | None = None
    ) -> list[asyncpg.Record]:
        target = connection if connection is not None else self.pool
        return await target.fetch(sql, *args)

    async def fetchrow(
        self, sql: str, *args: Any, connection: asyncpg.Connection | None = None
    ) -> asyncpg.Record | None:
        target = connection if connection is not None else self.pool
        return await target.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self.pool.fetchval(sql, *args)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.pool.PoolConnectionProxy]:
        """Acquire a pooled connection wrapped in a transaction."""
        async with self.pool.acquire() as conn, conn.transaction():
            yield conn

    async def run_script(self, statements: Sequence[str]) -> None:
        """Run a sequence of statements inside a single transaction."""
        async with self.transaction() as conn:
            for statement in statements:
                await conn.execute(statement)
