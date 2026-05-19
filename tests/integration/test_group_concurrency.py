"""Integration tests for group concurrency (require DATABASE_URL)."""

from __future__ import annotations

import asyncio

from underboss import GroupOptions, SendOptions, Underboss


async def test_group_concurrency_caps_active_jobs_per_group(boss: Underboss) -> None:
    await boss.create_queue("q")
    for i in range(5):
        await boss.send("q", {"i": i}, SendOptions(group=GroupOptions(id="g1")))
    await asyncio.sleep(0.3)  # let CockroachDB resolve the write intents

    # group_concurrency=2 → at most 2 jobs from group g1 may be active at once.
    first = await boss.fetch("q", batch_size=10, group_concurrency=2)
    assert len(first) == 2

    # g1 is now at its cap — a further fetch claims nothing.
    assert await boss.fetch("q", batch_size=10, group_concurrency=2) == []

    # completing one job frees exactly one slot in the group.
    await boss.complete("q", first[0].id)
    second = await boss.fetch("q", batch_size=10, group_concurrency=2)
    assert len(second) == 1
