"""Integration tests for transactional enqueue (require DATABASE_URL)."""

from __future__ import annotations

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
