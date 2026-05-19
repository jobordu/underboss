"""Unit tests for scheduler helpers (no database required)."""

from __future__ import annotations

import pytest

from underboss.scheduler import _should_send, validate_cron


def test_should_send_is_true_for_an_every_minute_cron() -> None:
    # "* * * * *" always last fired less than 60 seconds ago.
    assert _should_send("* * * * *", None) is True


def test_should_send_is_false_for_a_distant_cron() -> None:
    # 03:00 on 1 January — practically never within the last minute.
    assert _should_send("0 3 1 1 *", None) is False


def test_validate_cron_accepts_a_valid_expression() -> None:
    validate_cron("*/5 * * * *")  # must not raise


def test_validate_cron_rejects_an_invalid_expression() -> None:
    with pytest.raises(ValueError, match="invalid cron"):
        validate_cron("not a cron")
