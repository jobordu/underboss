"""Unit tests for the schema DDL builder (no database required)."""

from __future__ import annotations

from underboss import schema


def test_schema_version_matches_pgboss_v12() -> None:
    assert schema.SCHEMA_VERSION == 30


def test_build_schema_emits_core_objects() -> None:
    sql = "\n".join(schema.build_schema("underboss", schema.SCHEMA_VERSION))
    assert "CREATE SCHEMA IF NOT EXISTS underboss" in sql
    # CREATE TYPE has no portable IF NOT EXISTS — wrapped in DO/EXCEPTION instead.
    assert "CREATE TYPE underboss.job_state AS ENUM" in sql
    assert "EXCEPTION WHEN duplicate_object THEN NULL" in sql
    assert "CREATE TABLE IF NOT EXISTS underboss.job" in sql
    assert "CREATE TABLE IF NOT EXISTS underboss.queue" in sql
    assert "CREATE TABLE IF NOT EXISTS underboss.schedule" in sql
    assert "FOR UPDATE" not in sql  # the schema build is DDL only
    assert f"VALUES ('{schema.SCHEMA_VERSION}') ON CONFLICT DO NOTHING" in sql


def test_build_schema_honours_custom_schema_name() -> None:
    sql = "\n".join(schema.build_schema("pgboss", schema.SCHEMA_VERSION))
    assert "CREATE TABLE IF NOT EXISTS pgboss.job" in sql
    assert "underboss." not in sql


def test_build_schema_can_skip_namespace_creation() -> None:
    statements = schema.build_schema("underboss", create_namespace=False)
    assert not any(s.startswith("CREATE SCHEMA") for s in statements)


def test_build_schema_statement_count_is_stable() -> None:
    # 1 namespace + 22 schema objects.
    # (Was 25; FK q_fkey, FK dlq_fkey, and the key_strict_fifo CHECK are now
    # inlined on CREATE TABLE underboss.job because CockroachDB lacks
    # `ALTER TABLE ... ADD CONSTRAINT IF NOT EXISTS`.)
    assert len(schema.build_schema("underboss")) == 23


def test_build_schema_is_fully_idempotent_on_text_level() -> None:
    """Every emitted statement is one of: a guarded CREATE (IF NOT EXISTS),
    a CREATE OR REPLACE FUNCTION, or an INSERT with ON CONFLICT DO NOTHING.
    This is the textual contract that makes `Underboss.start()` safe to call
    on a partially or fully installed schema — critical on CockroachDB where
    DDL is non-transactional (a mid-script failure leaves earlier objects in
    place, and the retry must complete the install rather than fail on
    `already exists`).
    """
    for stmt in schema.build_schema("underboss"):
        s = " ".join(stmt.split())  # collapse whitespace
        is_idempotent = (
            "IF NOT EXISTS" in s
            or "CREATE OR REPLACE" in s
            or "ON CONFLICT" in s
            # DO/EXCEPTION wrapper around CREATE TYPE — portable PG+CRDB form,
            # since standard Postgres has no `CREATE TYPE IF NOT EXISTS`.
            or ("DO $$" in s and "EXCEPTION WHEN duplicate_object" in s)
        )
        assert is_idempotent, f"non-idempotent DDL would break partial-state retries:\n{stmt}"


def test_build_schema_inlines_job_table_constraints_for_crdb() -> None:
    """The job-table FKs (q_fkey, dlq_fkey) and the key_strict_fifo CHECK
    live INLINE on CREATE TABLE rather than as separate ALTER TABLE statements
    — CockroachDB lacks `ALTER TABLE ... ADD CONSTRAINT IF NOT EXISTS`, so
    inlining is the only non-PL/pgSQL idempotency path. Constraint names are
    preserved (q_fkey, dlq_fkey, job_key_strict_fifo_singleton_key_check) so
    downstream tools see the same names."""
    sql = "\n".join(schema.build_schema("underboss"))
    # ALTER TABLE ADD CONSTRAINT no longer appears at all in build_schema
    assert "ALTER TABLE" not in sql, f"unexpected ALTER TABLE leaked back into build_schema:\n{sql}"
    # The constraint names are still present (inlined on the CREATE TABLE)
    assert "CONSTRAINT q_fkey" in sql
    assert "CONSTRAINT dlq_fkey" in sql
    assert "CONSTRAINT job_key_strict_fifo_singleton_key_check" in sql
