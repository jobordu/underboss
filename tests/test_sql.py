"""Unit tests for the SQL query builders (no database required)."""

from __future__ import annotations

from underboss import sql


def test_create_queue_inserts_into_the_queue_table_directly() -> None:
    # Inlines the create_queue UDF body as a direct INSERT into the qualified
    # {schema}.queue TABLE rather than CALLING the UDF. A prepared statement that
    # binds a UDF (qualified OR via search_path) trips a spurious CockroachDB
    # "no USAGE on schema" on a freshly-created/recently-changed schema; qualified
    # table refs are immune, so the direct INSERT dodges it on every gateway.
    q = sql.create_queue("underboss")
    assert "INSERT INTO underboss.queue" in q
    assert "create_queue(" not in q  # no UDF call
    assert "$2::text::jsonb" in q  # options bound as JSON text, no jsonb codec needed
    assert "ON CONFLICT DO NOTHING" in q
    # schema IS interpolated (it names the table), names/options stay bind params.
    assert "INSERT INTO pgboss.queue" in sql.create_queue("pgboss")


def test_delete_queue_deletes_from_job_and_queue_tables_directly() -> None:
    q = sql.delete_queue("pgboss")
    assert "delete_queue(" not in q  # no UDF call
    assert "DELETE FROM pgboss.job WHERE name = $1" in q
    assert "DELETE FROM pgboss.queue WHERE name = $1" in q


def test_insert_jobs_uses_bind_params_and_schema() -> None:
    query = sql.insert_jobs("pgboss")
    assert "INSERT INTO pgboss.job" in query
    assert "JOIN pgboss.queue q ON q.name = $2" in query
    assert "jsonb_array_elements($1::text::jsonb)" in query
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


def test_fail_jobs_returns_dead_letter_routing_info() -> None:
    query = sql.fail_jobs("underboss")
    assert "UPDATE underboss.job" in query
    assert "RETURNING dead_letter, data, output, state" in query


def test_route_to_dead_letter_inserts_into_the_target_queue() -> None:
    query = sql.route_to_dead_letter("underboss")
    assert "INSERT INTO underboss.job" in query
    assert "FROM underboss.queue q" in query
    assert "WHERE q.name = $1" in query


def test_fetch_next_job_has_a_group_concurrency_variant() -> None:
    plain = sql.fetch_next_job("underboss")
    grouped = sql.fetch_next_job("underboss", group_concurrency=3)
    assert "active_group_counts" not in plain
    assert "active_group_counts" in grouped
    assert "ROW_NUMBER() OVER (PARTITION BY t.group_id" in grouped
    assert "(active_cnt + group_rn) <= $3" in grouped


def test_admin_sql_builders_target_the_schema() -> None:
    assert "FROM underboss.job" in sql.get_job_by_id("underboss")
    assert "state = 'cancelled'" in sql.cancel_jobs("underboss")
    assert "state = 'created'" in sql.resume_jobs("underboss")
    assert "retry_limit = retry_limit + 1" in sql.retry_jobs("underboss")
    assert sql.delete_jobs("underboss").strip().startswith("DELETE FROM underboss.job")
    assert "FROM underboss.queue WHERE name = $1" in sql.get_queue("underboss")
    assert "FROM underboss.queue ORDER BY name" in sql.get_queues("underboss")
