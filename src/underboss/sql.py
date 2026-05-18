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
