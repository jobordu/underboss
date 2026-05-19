"""SQL query layer for underboss — ported from pg-boss's ``plans.ts``.

Each function takes the target schema name and returns a parameterised SQL
string. Queue and job names are passed as bind parameters, never interpolated.
"""

from __future__ import annotations


def create_queue(schema: str) -> str:
    """Create a queue via the ``create_queue`` function ($1 = name, $2 = options)."""
    return f"SELECT {schema}.create_queue($1, $2::jsonb)"


def delete_queue(schema: str) -> str:
    """Delete a queue and its jobs via the ``delete_queue`` function ($1 = name)."""
    return f"SELECT {schema}.delete_queue($1)"


def insert_jobs(schema: str) -> str:
    """Insert a batch of jobs ($1 = jsonb array of job specs, $2 = queue name).

    Ported from pg-boss ``insertJobs``. Per-job options fall back to the queue's
    defaults via the join on ``queue``; ``ON CONFLICT DO NOTHING`` lets the
    policy indexes suppress duplicates. Returns the id of each row inserted.
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
      FROM jsonb_array_elements($1::jsonb) AS x
    ) j
    JOIN {schema}.queue q ON q.name = $2
    ON CONFLICT DO NOTHING
    RETURNING id
    """


_JOB_COLUMNS_MIN = ("id", "name", "data", "expire_seconds", "group_id", "group_tier")
_JOB_COLUMNS_META = ("state", "priority", "retry_limit", "retry_count", "started_on", "created_on")


def fetch_next_job(schema: str, *, include_metadata: bool = False) -> str:
    """Claim up to $2 jobs from queue $1 ($1 = queue name, $2 = batch size).

    Ported from pg-boss ``fetchNextJob`` (standard-policy path). The claim is a
    single auto-committed statement: the row lock is released the instant the
    UPDATE commits, so executing a job never holds a database lock.
    """
    columns = _JOB_COLUMNS_MIN + (_JOB_COLUMNS_META if include_metadata else ())
    returning = ", ".join(f"j.{column}" for column in columns)
    return f"""
    WITH next AS (
      SELECT j.id
      FROM {schema}.job j
      WHERE j.name = $1
        AND j.state < 'active'
        AND j.start_after < now()
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


def complete_jobs(schema: str) -> str:
    """Mark active jobs completed ($1 = queue, $2 = uuid[], $3 = output jsonb)."""
    return f"""
    UPDATE {schema}.job
    SET completed_on = now(), state = 'completed', output = $3::jsonb
    WHERE name = $1 AND id = ANY($2::uuid[]) AND state = 'active'
    """


def fail_jobs(schema: str) -> str:
    """Fail active jobs ($1 = queue, $2 = uuid[], $3 = error output jsonb).

    A job with retries remaining moves back to ``retry`` with its next
    ``start_after`` computed from ``retry_delay`` / ``retry_backoff``; an
    exhausted job moves to ``failed``. ``retry_count`` is incremented at claim
    time by :func:`fetch_next_job`, not here.

    This is a plain UPDATE rather than pg-boss's delete-and-reinsert: that
    pattern exists for table partitioning and dead-letter copying, neither of
    which the single-table schema needs (dead-letter routing lands separately).
    Numeric operands are cast to float8 explicitly — CockroachDB will not mix
    float and int implicitly the way PostgreSQL does.
    """
    return f"""
    UPDATE {schema}.job SET
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
      output = $3::jsonb,
      heartbeat_on = NULL
    WHERE name = $1 AND id = ANY($2::uuid[]) AND state = 'active'
    """
