"""Integration tests for the worker (require DATABASE_URL)."""

from __future__ import annotations

import asyncio
import time

from underboss import QueueOptions, Underboss, WorkOptions


async def _job_state(boss: Underboss, job_id: str) -> str | None:
    return await boss._db.fetchval(
        f"SELECT state FROM {boss.schema}.job WHERE id = $1::uuid", job_id
    )


async def _await_state(
    boss: Underboss, job_id: str, expected: str, *, timeout: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout
    state: str | None = None
    while time.monotonic() < deadline:
        state = await _job_state(boss, job_id)
        if state == expected:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id}: state={state!r}, expected {expected!r}")


async def test_worker_runs_and_completes_a_job(boss: Underboss) -> None:
    await boss.create_queue("emails")
    seen: list[object] = []

    async def handler(jobs):
        seen.extend(job.data for job in jobs)

    job_id = await boss.send("emails", {"to": "ada@example.com"})
    assert job_id is not None
    await boss.work("emails", handler, WorkOptions(poll_interval_seconds=0.05))

    await _await_state(boss, job_id, "completed")
    assert seen == [{"to": "ada@example.com"}]


async def test_worker_retries_then_fails(boss: Underboss) -> None:
    # retry_limit=1 → one initial attempt plus one retry, then 'failed'.
    await boss.create_queue("flaky", QueueOptions(retry_limit=1))
    attempts = 0

    async def handler(jobs):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    job_id = await boss.send("flaky", {"n": 1})
    assert job_id is not None
    await boss.work("flaky", handler, WorkOptions(poll_interval_seconds=0.05))

    await _await_state(boss, job_id, "failed")
    assert attempts == 2
