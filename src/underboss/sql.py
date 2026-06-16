"""SQL query layer for underboss — ported from pg-boss's ``plans.ts``.

Each function takes the target schema name and returns a parameterised SQL
string. Queue and job names are passed as bind parameters, never interpolated.
"""

from __future__ import annotations


def create_queue(schema: str) -> str:
    """Create a queue via the ``create_queue`` function ($1 = name, $2 = options).

    ``$2`` is bound as JSON text and cast (``::text::jsonb``), not bound as jsonb
    directly, so the call needs no per-connection jsonb codec and can run on a
    caller-supplied connection.

    The function is called UNqualified (resolved via the connection's
    ``search_path``, pinned to ``schema`` by :class:`~underboss.db.Database`),
    NOT as ``{schema}.create_queue``. CockroachDB (≥v24.3) raises a spurious
    "no USAGE on schema" when a schema-qualified UDF is resolved inside a
    prepared statement against a freshly-created schema; search_path resolution
    avoids it. ``schema`` is unused here but kept for signature parity.
    """
    return "SELECT create_queue($1, $2::text::jsonb)"


def delete_queue(schema: str) -> str:
    """Delete a queue and its jobs via the ``delete_queue`` function ($1 = name).

    Called UNqualified (via search_path) — see :func:`create_queue` for why.
    """
    return "SELECT delete_queue($1)"


def insert_jobs(schema: str) -> str:
    """Insert a batch of jobs ($1 = JSON-text array of job specs, $2 = queue name).

    Ported from pg-boss ``insertJobs``. Per-job options fall back to the queue's
    defaults via the join on ``queue``; ``ON CONFLICT DO NOTHING`` lets the
    policy indexes suppress duplicates. Returns the id of each row inserted.

    ``$1`` is bound as JSON text and cast (``::text::jsonb``), not bound as jsonb
    directly, so the call needs no per-connection jsonb codec and can run on a
    caller-supplied connection.
    """
    return f"""
    INSERT INTO {schema}.job (
      id, name, data, priority, start_after, singleton_key, singleton_on,
      group_id, group_tier, expire_seconds, deletion_seconds, keep_until,
      retry_limit, retry_delay, retry_backoff, retry_delay_max, policy,
      dead_letter, heartbeat_seconds
    )
    SELECT
      COALESCE(id, gen_random_uuid()) as id,
      $2 as name,
      data,
      COALESCE(priority, 0) as priority,
      j.start_after,
      "singletonKey",
      CASE
        WHEN "singletonSeconds" IS NOT NULL
        -- Explicit float8 casts: CockroachDB will not implicitly mix float and
        -- int operands inside floor()/division the way PostgreSQL does. The
        -- bucket count is reduced to int8 before multiplying the interval.
        THEN 'epoch'::timestamp + (
          ("singletonSeconds" * floor(
            (date_part('epoch', now()) + COALESCE("singletonOffset", 0)::float8)
            / "singletonSeconds"::float8
          )::int8) * interval '1 second'
        )
        ELSE NULL
      END as singleton_on,
      "groupId" as group_id,
      "groupTier" as group_tier,
      COALESCE("expireInSeconds", q.expire_seconds) as expire_seconds,
      COALESCE("deleteAfterSeconds", q.deletion_seconds) as deletion_seconds,
      j.start_after + (COALESCE("retentionSeconds", q.retention_seconds) * interval '1s')
        as keep_until,
      COALESCE("retryLimit", q.retry_limit) as retry_limit,
      COALESCE("retryDelay", q.retry_delay) as retry_delay,
      COALESCE("retryBackoff", q.retry_backoff, false) as retry_backoff,
      COALESCE("retryDelayMax", q.retry_delay_max) as retry_delay_max,
      q.policy,
      COALESCE("deadLetter", q.dead_letter) as dead_letter,
      COALESCE("heartbeatSeconds", q.heartbeat_seconds) as heartbeat_seconds
    FROM (
      SELECT
        (x->>'id')::uuid as id,
        (x->>'priority')::integer as priority,
        (x->>'data')::jsonb as data,
        (x->>'retryLimit')::integer as "retryLimit",
        (x->>'retryDelay')::integer as "retryDelay",
        (x->>'retryDelayMax')::integer as "retryDelayMax",
        (x->>'retryBackoff')::boolean as "retryBackoff",
        x->>'singletonKey' as "singletonKey",
        (x->>'singletonSeconds')::integer as "singletonSeconds",
        (x->>'singletonOffset')::integer as "singletonOffset",
        x->>'groupId' as "groupId",
        x->>'groupTier' as "groupTier",
        (x->>'expireInSeconds')::integer as "expireInSeconds",
        (x->>'deleteAfterSeconds')::integer as "deleteAfterSeconds",
        (x->>'retentionSeconds')::integer as "retentionSeconds",
        x->>'deadLetter' as "deadLetter",
        (x->>'heartbeatSeconds')::integer as "heartbeatSeconds",
        CASE
          WHEN right(x->>'startAfter', 1) = 'Z'
          THEN CAST(x->>'startAfter' as timestamp with time zone)
          ELSE now() + CAST(COALESCE(x->>'startAfter', '0') as interval)
        END as start_after
      FROM jsonb_array_elements($1::text::jsonb) AS x
    ) j
    JOIN {schema}.queue q ON q.name = $2
    ON CONFLICT DO NOTHING
    RETURNING id
    """


_JOB_COLUMNS_MIN = ("id", "name", "data", "expire_seconds", "group_id", "group_tier")
_JOB_COLUMNS_META = ("state", "priority", "retry_limit", "retry_count", "started_on", "created_on")


def fetch_next_job(
    schema: str, *, include_metadata: bool = False, group_concurrency: int | None = None
) -> str:
    """Claim jobs from queue $1 ($1 = queue, $2 = batch size, $3 = group limit).

    Ported from pg-boss ``fetchNextJob``. The claim is a single auto-committed
    statement: the row lock is released the instant the UPDATE commits, so
    executing a job never holds a database lock. When ``group_concurrency`` is
    set, $3 caps how many jobs per ``group_id`` may be active at once.

    The '<=' in ``start_after <= now()`` is deliberate: on CockroachDB now()
    can equal a just-inserted job's start_after, so a strict '<' would miss a
    job claimed in the same instant as send() (what notify_worker triggers).
    """
    columns = _JOB_COLUMNS_MIN + (_JOB_COLUMNS_META if include_metadata else ())
    returning = ", ".join(f"j.{column}" for column in columns)
    if group_concurrency is None:
        return f"""
    WITH next AS (
      SELECT j.id
      FROM {schema}.job j
      WHERE j.name = $1
        AND j.state < 'active'
        AND j.start_after <= now()
      ORDER BY j.priority DESC, j.created_on, j.id
      LIMIT $2
      FOR UPDATE OF j SKIP LOCKED
    )
    UPDATE {schema}.job j SET
      state = 'active',
      started_on = now(),
      heartbeat_on = now(),
      retry_count = CASE WHEN j.started_on IS NOT NULL
                         THEN j.retry_count + 1 ELSE j.retry_count END
    FROM next
    WHERE j.name = $1 AND j.id = next.id
    RETURNING {returning}
    """
    # group-concurrency path: only claim jobs whose group is below the $3 cap.
    return f"""
    WITH active_group_counts AS (
      SELECT group_id, COUNT(*)::int AS active_cnt
      FROM {schema}.job
      WHERE name = $1 AND state = 'active' AND group_id IS NOT NULL
      GROUP BY group_id
    ),
    next AS (
      SELECT j.id, j.group_id
      FROM {schema}.job j
      LEFT JOIN active_group_counts agc ON j.group_id = agc.group_id
      WHERE j.name = $1
        AND j.state < 'active'
        AND j.start_after <= now()
        AND (j.group_id IS NULL OR agc.active_cnt IS NULL OR agc.active_cnt < $3)
      ORDER BY j.priority DESC, j.created_on, j.id
      LIMIT $2
      FOR UPDATE OF j SKIP LOCKED
    ),
    group_ranking AS (
      SELECT t.id, t.group_id,
        ROW_NUMBER() OVER (PARTITION BY t.group_id ORDER BY t.id) AS group_rn,
        COALESCE(agc.active_cnt, 0) AS active_cnt
      FROM next t
      LEFT JOIN active_group_counts agc ON t.group_id = agc.group_id
    ),
    group_filtered AS (
      SELECT id FROM group_ranking
      WHERE group_id IS NULL OR (active_cnt + group_rn) <= $3
    )
    UPDATE {schema}.job j SET
      state = 'active',
      started_on = now(),
      heartbeat_on = now(),
      retry_count = CASE WHEN j.started_on IS NOT NULL
                         THEN j.retry_count + 1 ELSE j.retry_count END
    FROM group_filtered
    WHERE j.name = $1 AND j.id = group_filtered.id
    RETURNING {returning}
    """


def complete_jobs(schema: str) -> str:
    """Mark active jobs completed ($1 = queue, $2 = JSON-text id array, $3 = output JSON text).

    ``$2`` (the job ids) and ``$3`` (the output) are bound as JSON text and
    cast (``::text::jsonb``), not as a ``uuid[]`` array / ``jsonb`` directly.
    asyncpg introspects an unfamiliar array type the first time it prepares a
    statement that binds one, and on CockroachDB that introspection is
    pathologically slow — its ``pg_catalog`` emulation makes asyncpg's
    recursive ``typeinfo_tree`` query take several seconds, stalling the
    completion UPDATE. Keeping every bind parameter plain text avoids it
    (and matches the ``::text::jsonb`` convention of insert_jobs/create_queue).
    """
    return f"""
    UPDATE {schema}.job
    SET completed_on = now(), state = 'completed', output = $3::text::jsonb
    WHERE name = $1
      AND id IN (SELECT e::uuid FROM jsonb_array_elements_text($2::text::jsonb) AS e)
      AND state = 'active'
    """


def _retry_or_fail_set(schema: str, output: str) -> str:
    """The shared SET clause for failing a job — retry with backoff, or fail.

    A job with retries remaining moves back to ``retry`` with its next
    ``start_after`` computed from ``retry_delay`` / ``retry_backoff``; an
    exhausted job moves to ``failed``. ``retry_count`` is incremented at claim
    time by :func:`fetch_next_job`, not here. ``output`` is the SQL expression
    stored as the job's output. Numeric operands are cast to float8 explicitly —
    CockroachDB will not mix float and int the way PostgreSQL does.
    """
    return f"""
      state = CASE WHEN retry_count < retry_limit
                   THEN 'retry'::{schema}.job_state
                   ELSE 'failed'::{schema}.job_state END,
      start_after = CASE
        WHEN retry_count >= retry_limit THEN start_after
        WHEN NOT retry_backoff THEN now() + retry_delay * interval '1 second'
        ELSE now() + ((LEAST(
          COALESCE(retry_delay_max::float8, 'infinity'::float8),
          retry_delay::float8 * (
            power(2::float8, LEAST(16, retry_count + 1)::float8) / 2::float8
            + power(2::float8, LEAST(16, retry_count + 1)::float8) / 2::float8 * random()
          )
        ))::int8 * interval '1 second')
      END,
      completed_on = CASE WHEN retry_count < retry_limit THEN NULL ELSE now() END,
      output = {output},
      heartbeat_on = NULL
    """


def fail_jobs(schema: str) -> str:
    """Fail active jobs by id ($1 = queue, $2 = JSON-text id array, $3 = output JSON text).

    Moves each job to ``retry`` or ``failed`` and RETURNs its dead_letter / data
    / output / state, so the caller can route exhausted jobs to a dead-letter
    queue with :func:`route_to_dead_letter` — a separate statement, because
    CockroachDB rejects two mutations of one table in a single statement.

    ``$2`` / ``$3`` are bound as JSON text and cast — see :func:`complete_jobs`
    for why (asyncpg's array-type introspection is pathologically slow on CRDB).
    """
    set_clause = _retry_or_fail_set(schema, "$3::text::jsonb")
    return f"""
    UPDATE {schema}.job SET
    {set_clause}
    WHERE name = $1
      AND id IN (SELECT e::uuid FROM jsonb_array_elements_text($2::text::jsonb) AS e)
      AND state = 'active'
    RETURNING dead_letter, data, output, state
    """


def fail_expired_jobs(schema: str) -> str:
    """Fail every active job whose lease has expired (started_on + expire_seconds).

    The timeout sweep. RETURNs dead_letter / data / output / state for
    dead-letter routing, like :func:`fail_jobs`.
    """
    timed_out = "'{\"message\": \"job timed out\"}'::jsonb"
    set_clause = _retry_or_fail_set(schema, timed_out)
    return f"""
    UPDATE {schema}.job SET
    {set_clause}
    WHERE state = 'active'
      AND started_on + expire_seconds * interval '1 second' < now()
    RETURNING dead_letter, data, output, state
    """


def route_to_dead_letter(schema: str) -> str:
    """Copy a failed job into its dead-letter queue ($1 = DLQ, $2 = data, $3 = output).

    Inserts a fresh job into the dead-letter queue carrying the failed job's
    payload and failure output, with the DLQ's own retry / retention config.
    Ported from pg-boss ``failJobs`` (the dlq_jobs CTE), run as its own
    statement so it never shares a statement with the failure UPDATE.
    """
    return f"""
    INSERT INTO {schema}.job (
      name, data, output, retry_limit, retry_delay, retry_backoff,
      keep_until, deletion_seconds
    )
    SELECT
      q.name, $2::jsonb, $3::jsonb,
      q.retry_limit, q.retry_delay, q.retry_backoff,
      now() + q.retention_seconds * interval '1 second', q.deletion_seconds
    FROM {schema}.queue q
    WHERE q.name = $1
    """


def delete_old_jobs(schema: str) -> str:
    """Delete jobs past their retention window (ported from pg-boss ``deletion``).

    Removes completed jobs older than ``deletion_seconds`` and queued jobs
    (created/retry) past ``keep_until``.
    """
    return f"""
    DELETE FROM {schema}.job
    WHERE (deletion_seconds > 0 AND completed_on + deletion_seconds * interval '1 second' < now())
       OR (state < 'active' AND keep_until < now())
    """


def get_schedules(schema: str) -> str:
    """All cron schedules, ordered by queue and key."""
    return (
        f"SELECT name, key, cron, timezone, data, options "
        f"FROM {schema}.schedule ORDER BY name, key"
    )


def upsert_schedule(schema: str) -> str:
    """Create or replace a schedule ($1=name, $2=key, $3=cron, $4=tz, $5=data, $6=options)."""
    return f"""
    INSERT INTO {schema}.schedule (name, key, cron, timezone, data, options)
    VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
    ON CONFLICT (name, key) DO UPDATE SET
      cron = EXCLUDED.cron,
      timezone = EXCLUDED.timezone,
      data = EXCLUDED.data,
      options = EXCLUDED.options,
      updated_on = now()
    """


def delete_schedule(schema: str) -> str:
    """Remove a schedule ($1 = queue name, $2 = key)."""
    return f"DELETE FROM {schema}.schedule WHERE name = $1 AND COALESCE(key, '') = $2"


def try_set_cron_time(schema: str) -> str:
    """Claim the cron tick when $1 seconds have elapsed since the last one.

    Returns one row when this caller won the tick, and no rows otherwise — this
    debounces the cron clock across multiple nodes via ``version.cron_on``.
    """
    return f"""
    UPDATE {schema}.version
    SET cron_on = now()
    WHERE EXTRACT(EPOCH FROM (now() - COALESCE(cron_on, now() - interval '1 week')))::float8 > $1
    RETURNING true
    """


def get_job_by_id(schema: str) -> str:
    """A single job by id ($1 = queue, $2 = job id)."""
    columns = ", ".join(_JOB_COLUMNS_MIN + _JOB_COLUMNS_META)
    return f"SELECT {columns} FROM {schema}.job WHERE name = $1 AND id = $2::uuid"


def cancel_jobs(schema: str) -> str:
    """Cancel queued or active jobs ($1 = queue, $2 = JSON-text id array)."""
    return f"""
    UPDATE {schema}.job
    SET completed_on = now(), state = 'cancelled'
    WHERE name = $1
      AND id IN (SELECT e::uuid FROM jsonb_array_elements_text($2::text::jsonb) AS e)
      AND state < 'completed'
    """


def resume_jobs(schema: str) -> str:
    """Return cancelled jobs to the queue ($1 = queue, $2 = JSON-text id array)."""
    return f"""
    UPDATE {schema}.job
    SET completed_on = NULL, state = 'created'
    WHERE name = $1
      AND id IN (SELECT e::uuid FROM jsonb_array_elements_text($2::text::jsonb) AS e)
      AND state = 'cancelled'
    """


def retry_jobs(schema: str) -> str:
    """Re-queue failed jobs, lifting their retry limit ($1 = queue, $2 = JSON-text id array)."""
    return f"""
    UPDATE {schema}.job
    SET state = 'retry', retry_limit = retry_limit + 1
    WHERE name = $1
      AND id IN (SELECT e::uuid FROM jsonb_array_elements_text($2::text::jsonb) AS e)
      AND state = 'failed'
    """


def delete_jobs(schema: str) -> str:
    """Delete jobs by id ($1 = queue, $2 = JSON-text id array)."""
    return f"""
    DELETE FROM {schema}.job
    WHERE name = $1
      AND id IN (SELECT e::uuid FROM jsonb_array_elements_text($2::text::jsonb) AS e)
    """


_QUEUE_COLUMNS = (
    "name", "policy", "retry_limit", "retry_delay", "retry_backoff", "retry_delay_max",
    "expire_seconds", "retention_seconds", "deletion_seconds", "dead_letter",
    "warning_queued", "heartbeat_seconds",
)


def get_queue(schema: str) -> str:
    """A single queue's configuration by name ($1 = name)."""
    return f"SELECT {', '.join(_QUEUE_COLUMNS)} FROM {schema}.queue WHERE name = $1"


def get_queues(schema: str) -> str:
    """Every queue's configuration, ordered by name."""
    return f"SELECT {', '.join(_QUEUE_COLUMNS)} FROM {schema}.queue ORDER BY name"
