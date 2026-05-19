"""Integration tests for the cron scheduler (require DATABASE_URL)."""

from __future__ import annotations

import asyncio
import time

from underboss import Underboss


async def _job_count(boss: Underboss, queue: str) -> int:
    return await boss._db.fetchval(f"SELECT count(*) FROM {boss.schema}.job WHERE name = $1", queue)


async def test_schedule_enqueues_a_job(db_url: str) -> None:
    boss = await Underboss(db_url, cron_interval_seconds=0.5).start()
    try:
        await boss.create_queue("reports")
        await boss.schedule("reports", "* * * * *", data={"kind": "daily"})

        deadline = time.monotonic() + 15.0
        count = 0
        while time.monotonic() < deadline:
            count = await _job_count(boss, "reports")
            if count >= 1:
                break
            await asyncio.sleep(0.2)
        assert count >= 1
    finally:
        await boss.stop()


async def test_unschedule_removes_the_schedule(db_url: str) -> None:
    boss = await Underboss(db_url, cron_interval_seconds=0.5).start()
    try:
        await boss.create_queue("reports")
        await boss.schedule("reports", "* * * * *")
        await boss.unschedule("reports")
        remaining = await boss._db.fetchval(
            f"SELECT count(*) FROM {boss.schema}.schedule WHERE name = 'reports'"
        )
        assert remaining == 0
    finally:
        await boss.stop()
