"""Unit tests for option translation into pg-boss's camelCase JSON specs."""

from __future__ import annotations

from datetime import UTC, datetime

from underboss.types import GroupOptions, QueueOptions, QueuePolicy, SendOptions
from underboss.underboss import _encode_start_after, _job_payload, _queue_options_payload


def test_encode_start_after_datetime_is_z_suffixed() -> None:
    when = datetime(2026, 5, 18, 12, 30, 0, tzinfo=UTC)
    assert _encode_start_after(when) == "2026-05-18T12:30:00Z"


def test_encode_start_after_int_becomes_relative_interval() -> None:
    assert _encode_start_after(30) == "30"


def test_encode_start_after_none() -> None:
    assert _encode_start_after(None) is None


def test_queue_options_payload_is_camelcase_and_omits_none() -> None:
    payload = _queue_options_payload(QueueOptions(policy=QueuePolicy.EXCLUSIVE, retry_limit=5))
    assert payload["policy"] == "exclusive"
    assert payload["retryLimit"] == 5
    assert "retryDelayMax" not in payload
    assert "deadLetter" not in payload


def test_job_payload_maps_group_and_omits_unset_fields() -> None:
    options = SendOptions(
        priority=3,
        singleton_key="server-1",
        group=GroupOptions(id="g1", tier="fast"),
    )
    payload = _job_payload({"x": 1}, options)
    assert payload["priority"] == 3
    assert payload["data"] == {"x": 1}
    assert payload["singletonKey"] == "server-1"
    assert payload["groupId"] == "g1"
    assert payload["groupTier"] == "fast"
    assert "startAfter" not in payload
    assert "deadLetter" not in payload


def test_job_payload_singleton_next_slot_sets_offset() -> None:
    payload = _job_payload(None, SendOptions(singleton_seconds=60, singleton_next_slot=True))
    assert payload["singletonSeconds"] == 60
    assert payload["singletonOffset"] == 60
