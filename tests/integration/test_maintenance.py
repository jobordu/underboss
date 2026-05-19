"""Integration tests for the maintenance sweeps (require DATABASE_URL)."""

from __future__ import annotations

import asyncio
import time

from underboss import QueueOptions, Underboss, WorkOptions


async def _job_state(boss: Underboss, job_id: str) -> str | None:
    return await boss._db.fetchval(
        f"SELECT state FROM {boss.schema}.job WHERE id = $1::uuid", job_id
    )


async def test_timeout_sweep_fails_an_expired_job(db_url: str) -> None:
    boss = await Underboss(db_url, maintenance_interval_seconds=0.5).start()
    release = asyncio.Event()
    try:
        # expire_in_seconds=1 → the worker's lease lapses 1s after it claims the
        # job; retry_limit=0 → no retry, so the timeout sweep lands it 'failed'.
        await boss.create_queue("slow", QueueOptions(expire_in_seconds=1, retry_limit=0))
        job_id = await boss.send("slow", {"n": 1})
        assert job_id is not None

        async def handler(jobs):
            # Hold the job 'active', as a stalled worker would, until released.
            await release.wait()

        await boss.work("slow", handler, WorkOptions(poll_interval_seconds=0.1))

        deadline = time.monotonic() + 10.0
        state = None
        while time.monotonic() < deadline:
            state = await _job_state(boss, job_id)
            if state == "failed":
                break
            await asyncio.sleep(0.1)
        assert state == "failed"
    finally:
        release.set()
        await boss.stop()


async def test_deletion_sweep_removes_a_completed_job(db_url: str) -> None:
    boss = await Underboss(db_url, maintenance_interval_seconds=0.5).start()
    try:
        # delete_after_seconds=1 → a completed job is purged 1s after completion.
        await boss.create_queue("brief", QueueOptions(delete_after_seconds=1))
        job_id = await boss.send("brief", {"n": 1})
        assert job_id is not None

        async def handler(jobs):
            return None

        await boss.work("brief", handler, WorkOptions(poll_interval_seconds=0.1))

        deadline = time.monotonic() + 10.0
        remaining = 1
        while time.monotonic() < deadline:
            remaining = await boss._db.fetchval(
                f"SELECT count(*) FROM {boss.schema}.job WHERE id = $1::uuid", job_id
            )
            if remaining == 0:
                break
            await asyncio.sleep(0.1)
        assert remaining == 0
    finally:
        await boss.stop()
