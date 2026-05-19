# underboss

An async, Postgres-backed job queue for Python — a port of
[pg-boss](https://github.com/timgit/pg-boss).

> **Status: alpha.** underboss 0.1.0 is feature-complete and tested against both
> PostgreSQL and CockroachDB, but it is young and not yet published to PyPI —
> expect rough edges.

underboss runs background jobs in a SQL database you already operate. No Redis,
no broker — just a `job` table, `SELECT ... FOR UPDATE SKIP LOCKED`, and your
existing Postgres. Its schema is wire-compatible with pg-boss schema version 30.

## Why

- **One less system.** Your queue lives in the Postgres database you already
  run — there is no separate broker to deploy, monitor, secure, or back up.
- **Runs on PostgreSQL and CockroachDB.** The DDL is pure SQL — no PL/pgSQL, no
  extensions — so it runs unmodified on either. Dispatch is poll-based
  (`FOR UPDATE SKIP LOCKED`); `notify_worker()` is an in-process nudge so a
  same-process `send()` is picked up without waiting for the next poll.
- **Faithful pg-boss semantics.** Retries with exponential backoff, dead-letter
  routing, cron scheduling, job groups with per-group concurrency limits,
  singleton / throttling keys, and automatic timeout & retention sweeps.

## Install

underboss is young and not yet on PyPI. Install from source:

    pip install git+https://github.com/jobordu/underboss

Requires Python 3.11+ and PostgreSQL 13+ or CockroachDB 22.2+.

## Quickstart

```python
import asyncio
from underboss import Underboss


async def main():
    boss = await Underboss("postgresql://localhost/mydb").start()
    await boss.create_queue("email")

    job_id = await boss.send("email", {"to": "ada@example.com"})
    print(f"queued {job_id}")

    async def handler(jobs):
        for job in jobs:
            print(f"sending email: {job.data}")

    await boss.work("email", handler)
    await asyncio.sleep(5)
    await boss.stop()


asyncio.run(main())
```

## Database support

underboss runs the same way on **PostgreSQL 13+** and **CockroachDB 22.2+** — the
schema is pure SQL (no PL/pgSQL, no extensions) and dispatch relies only on
`FOR UPDATE SKIP LOCKED`. CI exercises the suite on PostgreSQL; it is also
developed and tested against CockroachDB.

## Credit

underboss is a Python port of [pg-boss](https://github.com/timgit/pg-boss) by
Tim Jones, and reuses its battle-tested schema design. pg-boss is MIT licensed;
so is underboss.

## License

MIT — see [LICENSE](LICENSE).
