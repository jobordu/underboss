"""Integration tests for the job/queue admin API (require DATABASE_URL)."""

from __future__ import annotations

import asyncio
import time

from underboss import Job, JobInsert, JobState, QueueOptions, QueuePolicy, Underboss, WorkOptions


async def _fetch_n(boss: Underboss, name: str, n: int) -> list[Job]:
    """Fetch ``n`` jobs, retrying — an immediate fetch can miss a job whose
    write intent CockroachDB has not resolved yet (see worker notify handling)."""
    claimed: list[Job] = []
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and len(claimed) < n:
        claimed += await boss.fetch(name, batch_size=n - len(claimed))
        if len(claimed) < n:
            await asyncio.sleep(0.05)
    assert len(claimed) == n, f"fetched {len(claimed)}/{n}"
    return claimed


async def test_insert_and_fetch(boss: Underboss) -> None:
    await boss.create_queue("batch")
    ids = await boss.insert("batch", [JobInsert(data={"n": 1}), JobInsert(data={"n": 2})])
    assert len(ids) == 2
    jobs = await _fetch_n(boss, "batch", 2)
    assert {job.data["n"] for job in jobs} == {1, 2}


async def test_get_job(boss: Underboss) -> None:
    await boss.create_queue("q")
    job_id = await boss.send("q", {"hello": "world"})
    assert job_id is not None
    job = await boss.get_job("q", job_id)
    assert job is not None
    assert job.id == job_id
    assert job.data == {"hello": "world"}
    assert await boss.get_job("q", "00000000-0000-0000-0000-000000000000") is None


async def test_cancel_and_resume(boss: Underboss) -> None:
    await boss.create_queue("q")
    job_id = await boss.send("q", {"n": 1})
    assert job_id is not None
    await boss.cancel("q", job_id)
    job = await boss.get_job("q", job_id)
    assert job is not None and job.state == JobState.CANCELLED
    await boss.resume("q", job_id)
    job = await boss.get_job("q", job_id)
    assert job is not None and job.state == JobState.CREATED


async def test_complete_and_delete(boss: Underboss) -> None:
    await boss.create_queue("q")
    job_id = await boss.send("q", {"n": 1})
    assert job_id is not None
    [claimed] = await _fetch_n(boss, "q", 1)
    await boss.complete("q", claimed.id)
    job = await boss.get_job("q", job_id)
    assert job is not None and job.state == JobState.COMPLETED
    await boss.delete_job("q", job_id)
    assert await boss.get_job("q", job_id) is None


async def test_retry_a_failed_job(boss: Underboss) -> None:
    await boss.create_queue("q", QueueOptions(retry_limit=0))

    async def handler(jobs):
        raise RuntimeError("boom")

    job_id = await boss.send("q", {"n": 1})
    assert job_id is not None
    await boss.work("q", handler, WorkOptions(poll_interval_seconds=0.05))

    deadline = time.monotonic() + 10.0
    job = None
    while time.monotonic() < deadline:
        job = await boss.get_job("q", job_id)
        if job is not None and job.state == JobState.FAILED:
            break
        await asyncio.sleep(0.1)
    assert job is not None and job.state == JobState.FAILED

    await boss.retry("q", job_id)
    job = await boss.get_job("q", job_id)
    assert job is not None and job.state == JobState.RETRY


async def test_queue_introspection(boss: Underboss) -> None:
    await boss.create_queue("alpha", QueueOptions(policy=QueuePolicy.EXCLUSIVE, retry_limit=7))
    await boss.create_queue("beta")
    alpha = await boss.get_queue("alpha")
    assert alpha is not None
    assert alpha.name == "alpha"
    assert alpha.options.policy == QueuePolicy.EXCLUSIVE
    assert alpha.options.retry_limit == 7
    assert await boss.get_queue("nonexistent") is None
    names = {queue.name for queue in await boss.get_queues()}
    assert {"alpha", "beta"} <= names
