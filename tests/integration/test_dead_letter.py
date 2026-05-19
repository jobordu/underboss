"""Integration tests for dead-letter routing (require DATABASE_URL)."""

from __future__ import annotations

import asyncio
import time

from underboss import QueueOptions, Underboss, WorkOptions


async def test_failed_job_routes_to_its_dead_letter_queue(boss: Underboss) -> None:
    await boss.create_queue("parking")
    # retry_limit=0 → the first failure exhausts the job and routes it.
    await boss.create_queue("orders", QueueOptions(dead_letter="parking", retry_limit=0))

    async def handler(jobs):
        raise RuntimeError("boom")

    await boss.send("orders", {"order": 42})
    await boss.work("orders", handler, WorkOptions(poll_interval_seconds=0.05))

    deadline = time.monotonic() + 10.0
    parked = None
    while time.monotonic() < deadline:
        parked = await boss._db.fetchval(
            f"SELECT data FROM {boss.schema}.job WHERE name = 'parking' LIMIT 1"
        )
        if parked is not None:
            break
        await asyncio.sleep(0.1)
    assert parked == {"order": 42}


async def test_failed_job_without_dead_letter_is_not_copied(boss: Underboss) -> None:
    await boss.create_queue("plain", QueueOptions(retry_limit=0))

    async def handler(jobs):
        raise RuntimeError("boom")

    await boss.send("plain", {"n": 1})
    await boss.work("plain", handler, WorkOptions(poll_interval_seconds=0.05))

    deadline = time.monotonic() + 10.0
    failed = 0
    while time.monotonic() < deadline:
        failed = await boss._db.fetchval(
            f"SELECT count(*) FROM {boss.schema}.job WHERE name = 'plain' AND state = 'failed'"
        )
        if failed == 1:
            break
        await asyncio.sleep(0.1)
    assert failed == 1
    # No dead_letter set → nothing was copied to any other queue.
    copied = await boss._db.fetchval(
        f"SELECT count(*) FROM {boss.schema}.job WHERE name != 'plain'"
    )
    assert copied == 0
