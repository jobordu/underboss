"""Unit tests for the schema DDL builder (no database required)."""

from __future__ import annotations

from underboss import schema


def test_schema_version_matches_pgboss_v12() -> None:
    assert schema.SCHEMA_VERSION == 30


def test_build_schema_emits_core_objects() -> None:
    sql = "\n".join(schema.build_schema("underboss", schema.SCHEMA_VERSION))
    assert "CREATE SCHEMA IF NOT EXISTS underboss" in sql
    assert "CREATE TYPE underboss.job_state AS ENUM" in sql
    assert "CREATE TABLE underboss.job" in sql
    assert "CREATE TABLE underboss.queue" in sql
    assert "CREATE TABLE underboss.schedule" in sql
    assert "FOR UPDATE" not in sql  # the schema build is DDL only
    assert f"VALUES ('{schema.SCHEMA_VERSION}')" in sql


def test_build_schema_honours_custom_schema_name() -> None:
    sql = "\n".join(schema.build_schema("pgboss", schema.SCHEMA_VERSION))
    assert "CREATE TABLE pgboss.job" in sql
    assert "underboss." not in sql


def test_build_schema_can_skip_namespace_creation() -> None:
    statements = schema.build_schema("underboss", create_namespace=False)
    assert not any(s.startswith("CREATE SCHEMA") for s in statements)


def test_build_schema_statement_count_is_stable() -> None:
    # 1 namespace + 25 schema objects (see build_schema).
    assert len(schema.build_schema("underboss")) == 26
