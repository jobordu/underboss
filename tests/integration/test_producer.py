"""Integration tests for create_queue and send (require DATABASE_URL)."""

from __future__ import annotations

from underboss import QueueOptions, QueuePolicy, SendOptions, Underboss


async def test_send_inserts_a_job(boss: Underboss) -> None:
    await boss.create_queue("emails")
    job_id = await boss.send("emails", {"to": "ada@example.com"})
    assert job_id is not None
    count = await boss._db.fetchval(
        f"SELECT count(*) FROM {boss.schema}.job WHERE name = 'emails'"
    )
    assert count == 1


async def test_create_queue_is_idempotent(boss: Underboss) -> None:
    await boss.create_queue("emails")
    await boss.create_queue("emails")  # must not raise
    count = await boss._db.fetchval(
        f"SELECT count(*) FROM {boss.schema}.queue WHERE name = 'emails'"
    )
    assert count == 1


async def test_exclusive_policy_dedups_by_singleton_key(boss: Underboss) -> None:
    await boss.create_queue("provision", QueueOptions(policy=QueuePolicy.EXCLUSIVE))
    first = await boss.send("provision", {"id": 1}, SendOptions(singleton_key="srv-1"))
    second = await boss.send("provision", {"id": 1}, SendOptions(singleton_key="srv-1"))
    assert first is not None
    assert second is None


async def test_send_data_round_trips_as_json(boss: Underboss) -> None:
    await boss.create_queue("emails")
    payload = {"to": "ada@example.com", "tags": ["x", "y"], "n": 3}
    job_id = await boss.send("emails", payload)
    stored = await boss._db.fetchval(
        f"SELECT data FROM {boss.schema}.job WHERE id = $1::uuid", job_id
    )
    assert stored == payload
