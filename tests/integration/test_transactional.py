"""Integration tests for transactional enqueue (require DATABASE_URL)."""

from __future__ import annotations

import asyncpg

from underboss import Underboss


async def _job_count(boss: Underboss, name: str) -> int:
    return await boss._db.fetchval(f"SELECT count(*) FROM {boss.schema}.job WHERE name = $1", name)


async def test_send_on_a_connection_rolls_back_with_its_transaction(boss: Underboss) -> None:
    await boss.create_queue("tx")
    async with boss._db.pool.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        job_id = await boss.send("tx", {"n": 1}, connection=conn)
        assert job_id is not None
        await transaction.rollback()
    # the job was enqueued inside the rolled-back transaction, so it is gone
    assert await _job_count(boss, "tx") == 0


async def test_send_on_a_connection_commits_with_its_transaction(boss: Underboss) -> None:
    await boss.create_queue("tx")
    async with boss._db.pool.acquire() as conn, conn.transaction():
        await boss.send("tx", {"n": 2}, connection=conn)
    assert await _job_count(boss, "tx") == 1


async def test_send_on_a_foreign_connection_without_underbosss_jsonb_codec(
    boss: Underboss, db_url: str
) -> None:
    """send(connection=...) works on a connection underboss did not create.

    A caller-owned asyncpg connection (e.g. one managed by SQLAlchemy) has no
    underboss jsonb codec registered, so send() must encode the job payload
    itself. Regression test for the transactional-enqueue path.
    """
    await boss.create_queue("foreign")
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            job_id = await boss.send("foreign", {"hello": "world"}, connection=conn)
        assert job_id is not None
    finally:
        await conn.close()
    job = await boss.get_job("foreign", job_id)
    assert job is not None
    assert job.data == {"hello": "world"}
