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
