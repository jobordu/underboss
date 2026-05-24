"""Portable DDL for the underboss schema.

A faithful port of pg-boss v12's schema (the ``create()`` plan in pg-boss's
``plans.ts``), kept deliberately free of PL/pgSQL and Postgres extensions so the
exact same DDL runs unmodified on both PostgreSQL and CockroachDB.

The layout is wire-compatible with pg-boss schema version 30 (pg-boss 12.18.1).
underboss is a Python port of pg-boss (https://github.com/timgit/pg-boss), MIT.
"""

from __future__ import annotations

#: pg-boss schema version this layout is wire-compatible with (pg-boss 12.18.1).
SCHEMA_VERSION = 30

#: Default Postgres schema (namespace) underboss installs into.
DEFAULT_SCHEMA = "underboss"

#: Job lifecycle states. Order is significant — the enum is numeric under the
#: hood and the SQL layer relies on comparisons such as ``state < 'active'``.
JOB_STATES: tuple[str, ...] = (
    "created",
    "retry",
    "active",
    "completed",
    "cancelled",
    "failed",
)

#: Supported queue policies.
QUEUE_POLICIES: tuple[str, ...] = (
    "standard",
    "short",
    "singleton",
    "stately",
    "exclusive",
    "key_strict_fifo",
)


# --------------------------------------------------------------------------
# Introspection queries
# --------------------------------------------------------------------------
def version_table_exists(schema: str) -> str:
    """SQL resolving to the ``version`` table's regclass, or NULL if absent."""
    return f"SELECT to_regclass('{schema}.version') AS name"


def get_version(schema: str) -> str:
    """SQL returning the installed schema version."""
    return f"SELECT version FROM {schema}.version"


def set_version(schema: str, version: int) -> str:
    """SQL stamping the schema version."""
    return f"UPDATE {schema}.version SET version = {version}"


# --------------------------------------------------------------------------
# DDL fragments — one function per object, mirroring pg-boss ``plans.ts``.
# --------------------------------------------------------------------------
def _create_schema(schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema}"


def _create_enum_job_state(schema: str) -> str:
    values = ",\n      ".join(f"'{state}'" for state in JOB_STATES)
    return f"CREATE TYPE IF NOT EXISTS {schema}.job_state AS ENUM (\n      {values}\n    )"


def _create_table_version(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.version (
      version int primary key,
      cron_on timestamp with time zone,
      bam_on timestamp with time zone
    )"""


def _create_table_queue(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.queue (
      name text NOT NULL,
      policy text NOT NULL,
      retry_limit int NOT NULL,
      retry_delay int NOT NULL,
      retry_backoff bool NOT NULL,
      retry_delay_max int,
      expire_seconds int NOT NULL,
      retention_seconds int NOT NULL,
      deletion_seconds int NOT NULL,
      dead_letter text REFERENCES {schema}.queue (name) CHECK (dead_letter IS DISTINCT FROM name),
      partition bool NOT NULL,
      table_name text NOT NULL,
      deferred_count int NOT NULL default 0,
      queued_count int NOT NULL default 0,
      warning_queued int NOT NULL default 0,
      active_count int NOT NULL default 0,
      total_count int NOT NULL default 0,
      heartbeat_seconds int,
      singletons_active text[],
      monitor_on timestamp with time zone,
      maintain_on timestamp with time zone,
      created_on timestamp with time zone not null default now(),
      updated_on timestamp with time zone not null default now(),
      PRIMARY KEY (name)
    )"""


def _create_table_schedule(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.schedule (
      name text REFERENCES {schema}.queue ON DELETE CASCADE,
      key text not null DEFAULT '',
      cron text not null,
      timezone text,
      data jsonb,
      options jsonb,
      created_on timestamp with time zone not null default now(),
      updated_on timestamp with time zone not null default now(),
      PRIMARY KEY (name, key)
    )"""


def _create_table_subscription(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.subscription (
      event text not null,
      name text not null REFERENCES {schema}.queue ON DELETE CASCADE,
      created_on timestamp with time zone not null default now(),
      updated_on timestamp with time zone not null default now(),
      PRIMARY KEY (event, name)
    )"""


def _create_table_bam(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.bam (
      id uuid PRIMARY KEY default gen_random_uuid(),
      name text NOT NULL,
      version int NOT NULL,
      status text NOT NULL DEFAULT 'pending',
      queue text,
      table_name text NOT NULL,
      command text NOT NULL,
      error text,
      created_on timestamp with time zone NOT NULL DEFAULT now(),
      started_on timestamp with time zone,
      completed_on timestamp with time zone
    )"""


def _job_table_format_function(schema: str) -> str:
    return f"""CREATE OR REPLACE FUNCTION {schema}.job_table_format(command text, table_name text)
    RETURNS text AS
    $$
      SELECT format(
        replace(
          replace(command, '.job', '.%1$I'),
          'job_i', '%1$s_i'
        ),
        table_name
      );
    $$
    LANGUAGE sql"""


def _job_table_run_async_function(schema: str) -> str:
    # CockroachDB v23.2 rejects DEFAULT values on function arguments
    # (crdb #100962), so the arguments are plain. underboss never calls this
    # function — build_schema installs job_i7 directly — it exists only for
    # wire-compatibility with pg-boss schema v30; the arity is preserved.
    return f"""CREATE OR REPLACE FUNCTION {schema}.job_table_run_async(
      command_name text, version int, command text,
      tbl_name text, queue_name text)
    RETURNS VOID AS
    $$
      INSERT INTO {schema}.bam (name, version, status, queue, table_name, command)
      VALUES (command_name, version, 'pending', NULL, 'job', command)
    $$
    LANGUAGE sql"""


def _create_table_job(schema: str) -> str:
    # FKs (q_fkey, dlq_fkey) and the key_strict_fifo CHECK are inlined here
    # rather than emitted as separate ALTER TABLE statements: CockroachDB
    # has no `ALTER TABLE ... ADD CONSTRAINT IF NOT EXISTS` (crdb #29657),
    # so making the schema-install idempotent requires the constraints to
    # live on the CREATE TABLE itself (which IS guarded by IF NOT EXISTS).
    # Constraint names are preserved so existing pg-boss-shape tooling sees
    # the same names.
    return f"""CREATE TABLE IF NOT EXISTS {schema}.job (
      id uuid not null default gen_random_uuid(),
      name text not null,
      priority integer not null default(0),
      data jsonb,
      state {schema}.job_state not null default 'created',
      retry_limit integer not null default 2,
      retry_count integer not null default 0,
      retry_delay integer not null default 0,
      retry_backoff boolean not null default false,
      retry_delay_max integer,
      expire_seconds int not null default 900,
      deletion_seconds int not null default 604800,
      singleton_key text,
      singleton_on timestamp without time zone,
      group_id text,
      group_tier text,
      start_after timestamp with time zone not null default now(),
      created_on timestamp with time zone not null default now(),
      started_on timestamp with time zone,
      completed_on timestamp with time zone,
      keep_until timestamp with time zone NOT NULL default now() + interval '1209600',
      output jsonb,
      dead_letter text,
      policy text,
      heartbeat_on timestamp with time zone,
      heartbeat_seconds int,
      CONSTRAINT q_fkey FOREIGN KEY (name)
        REFERENCES {schema}.queue (name) ON DELETE RESTRICT,
      CONSTRAINT dlq_fkey FOREIGN KEY (dead_letter)
        REFERENCES {schema}.queue (name) ON DELETE RESTRICT,
      CONSTRAINT job_key_strict_fifo_singleton_key_check CHECK (
        NOT (policy = 'key_strict_fifo' AND singleton_key IS NULL)
      ),
      PRIMARY KEY (name, id)
    )"""


def _index_job_policy_short(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS job_i1 ON {schema}.job "
        f"(name, COALESCE(singleton_key, '')) "
        f"WHERE state = 'created' AND policy = 'short'"
    )


def _index_job_policy_singleton(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS job_i2 ON {schema}.job "
        f"(name, COALESCE(singleton_key, '')) "
        f"WHERE state = 'active' AND policy = 'singleton'"
    )


def _index_job_policy_stately(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS job_i3 ON {schema}.job "
        f"(name, state, COALESCE(singleton_key, '')) "
        f"WHERE state <= 'active' AND policy = 'stately'"
    )


def _index_job_policy_exclusive(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS job_i6 ON {schema}.job "
        f"(name, COALESCE(singleton_key, '')) "
        f"WHERE state <= 'active' AND policy = 'exclusive'"
    )


def _index_job_policy_key_strict_fifo(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS job_i8 ON {schema}.job (name, singleton_key) "
        f"WHERE state IN ('active', 'retry', 'failed') AND policy = 'key_strict_fifo'"
    )


def _index_job_throttle(schema: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS job_i4 ON {schema}.job "
        f"(name, singleton_on, COALESCE(singleton_key, '')) "
        f"WHERE state <> 'cancelled' AND singleton_on IS NOT NULL"
    )


def _index_job_fetch(schema: str) -> str:
    # NOTE: the PK column is intentionally absent from INCLUDE — CockroachDB
    # rejects primary-key columns in a covering INCLUDE clause.
    return (
        f"CREATE INDEX IF NOT EXISTS job_i5 ON {schema}.job (name, start_after) "
        f"INCLUDE (priority, created_on) WHERE state < 'active'"
    )


def _index_job_group_concurrency(schema: str) -> str:
    return (
        f"CREATE INDEX IF NOT EXISTS job_i7 ON {schema}.job (name, group_id) "
        f"WHERE state = 'active' AND group_id IS NOT NULL"
    )


def _create_table_warning(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.warning (
      id uuid PRIMARY KEY default gen_random_uuid(),
      type text NOT NULL,
      message text NOT NULL,
      data jsonb,
      created_on timestamp with time zone NOT NULL DEFAULT now()
    )"""


def _index_warning(schema: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS warning_i1 ON {schema}.warning (created_on DESC)"


def _create_queue_function(schema: str) -> str:
    return f"""CREATE OR REPLACE FUNCTION {schema}.create_queue(queue_name text, options jsonb)
    RETURNS VOID AS
    $$
      INSERT INTO {schema}.queue (
        name, policy, retry_limit, retry_delay, retry_backoff, retry_delay_max,
        expire_seconds, retention_seconds, deletion_seconds, warning_queued,
        dead_letter, partition, table_name, heartbeat_seconds
      )
      VALUES (
        $1,
        $2->>'policy',
        COALESCE(($2->>'retryLimit')::int, 2),
        COALESCE(($2->>'retryDelay')::int, 0),
        COALESCE(($2->>'retryBackoff')::bool, false),
        ($2->>'retryDelayMax')::int,
        COALESCE(($2->>'expireInSeconds')::int, 900),
        COALESCE(($2->>'retentionSeconds')::int, 1209600),
        COALESCE(($2->>'deleteAfterSeconds')::int, 604800),
        COALESCE(($2->>'warningQueueSize')::int, 0),
        $2->>'deadLetter',
        false,
        'job',
        ($2->>'heartbeatSeconds')::int
      )
      ON CONFLICT DO NOTHING
    $$
    LANGUAGE sql"""


def _delete_queue_function(schema: str) -> str:
    return f"""CREATE OR REPLACE FUNCTION {schema}.delete_queue(queue_name text)
    RETURNS VOID AS
    $$
      DELETE FROM {schema}.job WHERE name = queue_name;
      DELETE FROM {schema}.queue WHERE name = queue_name;
    $$
    LANGUAGE sql"""


def _insert_version(schema: str, version: int) -> str:
    # ON CONFLICT DO NOTHING so re-running build_schema() against a partially
    # or fully installed database is a no-op rather than a PK violation.
    return f"INSERT INTO {schema}.version(version) VALUES ('{version}') ON CONFLICT DO NOTHING"


def build_schema(
    schema: str = DEFAULT_SCHEMA,
    version: int = SCHEMA_VERSION,
    *,
    create_namespace: bool = True,
) -> list[str]:
    """Return the ordered DDL statements that install the underboss schema.

    Every statement is **idempotent** — re-running on a partially or fully
    installed schema is a clean no-op rather than a duplicate-object error.
    This matters most on CockroachDB, whose non-transactional DDL leaves
    partial state behind when a script fails mid-way: a retry must succeed,
    not compound the original failure.

    Idempotency mechanism per object class:
      - ``CREATE TYPE`` / ``TABLE`` / ``INDEX``        → ``IF NOT EXISTS``
      - ``CREATE FUNCTION``                            → ``CREATE OR REPLACE``
      - FOREIGN KEY + CHECK constraints on ``job``     → inlined on the
        guarded ``CREATE TABLE IF NOT EXISTS`` (CRDB has no
        ``ALTER TABLE … ADD CONSTRAINT IF NOT EXISTS``)
      - Version row INSERT                             → ``ON CONFLICT DO NOTHING``

    Run inside a single transaction where the dialect supports it (see
    :meth:`underboss.db.Database.run_script`); CockroachDB will still apply
    each DDL non-transactionally, but the idempotency above means partial
    application is recoverable.
    """
    statements: list[str] = []
    if create_namespace:
        statements.append(_create_schema(schema))
    # FKs (q_fkey, dlq_fkey) and the key_strict_fifo CHECK are no longer
    # separate ALTER TABLE statements — they're inlined on _create_table_job
    # because CockroachDB lacks `ADD CONSTRAINT IF NOT EXISTS`.
    statements += [
        _create_enum_job_state(schema),
        _create_table_version(schema),
        _create_table_queue(schema),
        _create_table_schedule(schema),
        _create_table_subscription(schema),
        _create_table_bam(schema),
        _job_table_format_function(schema),
        _job_table_run_async_function(schema),
        _create_table_job(schema),
        _index_job_policy_short(schema),
        _index_job_policy_singleton(schema),
        _index_job_policy_stately(schema),
        _index_job_policy_exclusive(schema),
        _index_job_policy_key_strict_fifo(schema),
        _index_job_throttle(schema),
        _index_job_fetch(schema),
        _index_job_group_concurrency(schema),
        _create_table_warning(schema),
        _index_warning(schema),
        _create_queue_function(schema),
        _delete_queue_function(schema),
        _insert_version(schema, version),
    ]
    return statements
