"""Unit tests for the SQL query builders (no database required)."""

from __future__ import annotations

from underboss import sql


def test_create_queue_targets_the_schema_function() -> None:
    assert sql.create_queue("underboss") == "SELECT underboss.create_queue($1, $2::jsonb)"


def test_delete_queue_targets_the_schema_function() -> None:
    assert sql.delete_queue("pgboss") == "SELECT pgboss.delete_queue($1)"


def test_insert_jobs_uses_bind_params_and_schema() -> None:
    query = sql.insert_jobs("pgboss")
    assert "INSERT INTO pgboss.job" in query
    assert "JOIN pgboss.queue q ON q.name = $2" in query
    assert "jsonb_array_elements($1::jsonb)" in query
    assert "ON CONFLICT DO NOTHING" in query
    assert "RETURNING id" in query


def test_insert_jobs_never_interpolates_names() -> None:
    # With schema "pgboss", nothing should leak the default schema name.
    assert "underboss" not in sql.insert_jobs("pgboss")


def test_fail_expired_jobs_targets_active_jobs_past_their_lease() -> None:
    query = sql.fail_expired_jobs("underboss")
    assert "UPDATE underboss.job" in query
    assert "state = 'active'" in query
    assert "started_on + expire_seconds" in query


def test_delete_old_jobs_removes_completed_and_stale_jobs() -> None:
    query = sql.delete_old_jobs("underboss")
    assert query.strip().startswith("DELETE FROM underboss.job")
    assert "deletion_seconds" in query
    assert "keep_until" in query
