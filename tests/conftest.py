"""Shared pytest fixtures.

Integration tests need a database; they are skipped unless ``DATABASE_URL`` is
set (a PostgreSQL or CockroachDB DSN).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest

from underboss import Underboss

DATABASE_URL = os.environ.get("DATABASE_URL")


@pytest.fixture
async def boss() -> AsyncIterator[Underboss]:
    """A started Underboss backed by a freshly-provisioned schema."""
    if DATABASE_URL is None:
        pytest.skip("integration test — set DATABASE_URL to run")

    setup = await asyncpg.connect(DATABASE_URL)
    try:
        await setup.execute("DROP SCHEMA IF EXISTS underboss CASCADE")
    finally:
        await setup.close()

    instance = await Underboss(DATABASE_URL).start()
    try:
        yield instance
    finally:
        await instance.stop()
