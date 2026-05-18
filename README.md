# underboss

An async, Postgres-backed job queue for Python — a port of
[pg-boss](https://github.com/timgit/pg-boss).

> **Status: alpha.** The schema and core architecture are in place; the
> producer/worker API is under active development toward the `0.1.0` release.
> The API shown below is the target surface.

underboss runs background jobs in a SQL database you already operate. No Redis,
no broker — just a `job` table, `SELECT ... FOR UPDATE SKIP LOCKED`, and your
existing Postgres. Its schema is wire-compatible with pg-boss schema version 30.

## Why

- **One less system.** Your queue lives in Postgres alongside your data — so you
  can enqueue a job in the *same transaction* as the row that needs it. No
  dual-write desync between a database and a separate broker.
- **Runs on PostgreSQL and CockroachDB.** The DDL is pure SQL — no PL/pgSQL, no
  extensions — so it runs unmodified on either. On PostgreSQL, `LISTEN/NOTIFY`
  wakes workers instantly; on CockroachDB it transparently falls back to
  polling.
- **pg-boss semantics.** Six queue policies, retries with exponential backoff,
  dead-letter queues, cron scheduling, job groups, and singleton/throttling
  keys.

## Install

underboss is in early development and not yet on PyPI. Install from source:

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

| Database    | Dequeue                  | Worker wake-up  |
| ----------- | ------------------------ | --------------- |
| PostgreSQL  | `FOR UPDATE SKIP LOCKED` | `LISTEN/NOTIFY` |
| CockroachDB | `FOR UPDATE SKIP LOCKED` | polling         |

## Credit

underboss is a Python port of [pg-boss](https://github.com/timgit/pg-boss) by
Tim Jones, and reuses its battle-tested schema design. pg-boss is MIT licensed;
so is underboss.

## License

MIT — see [LICENSE](LICENSE).
