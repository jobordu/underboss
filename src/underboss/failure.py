"""Failing jobs and routing exhausted ones to their dead-letter queue.

CockroachDB rejects multiple mutations of the same table in a single statement,
so the failure UPDATE and the dead-letter INSERT run as two statements inside
one transaction — atomic, but each touching ``job`` only once.
"""

from __future__ import annotations

from typing import Any

from underboss import sql
from underboss.db import Database


async def _route(conn: Any, schema: str, rows: list[Any]) -> None:
    """Copy each just-failed job that carries a dead_letter into that queue."""
    route_sql = sql.route_to_dead_letter(schema)
    for row in rows:
        if row["state"] == "failed" and row["dead_letter"] is not None:
            await conn.execute(route_sql, row["dead_letter"], row["data"], row["output"])


async def fail_by_id(
    db: Database, schema: str, name: str, ids: list[Any], output: dict[str, Any]
) -> None:
    """Fail jobs by id, routing any that exhaust their retries to their DLQ."""
    async with db.transaction() as conn:
        rows = await conn.fetch(sql.fail_jobs(schema), name, ids, output)
        await _route(conn, schema, rows)


async def fail_expired(db: Database, schema: str) -> None:
    """Fail jobs whose lease has expired, routing exhausted ones to their DLQ."""
    async with db.transaction() as conn:
        rows = await conn.fetch(sql.fail_expired_jobs(schema))
        await _route(conn, schema, rows)
