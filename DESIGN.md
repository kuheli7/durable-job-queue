# DESIGN

Durable job queue. Postgres is the only infrastructure. One API process, N worker processes.

## 1. The core idea (read this first)

A job is **one row** in a `jobs` table. The row *is* the queue. There is nothing else to lose.

* A worker **claims** a job by setting `status='running'` and a **lease** (`lease_expires_at = now() + 30s`)
  in a single committed transaction, using `SELECT ... FOR UPDATE SKIP LOCKED` so two workers never
  pick the same row.
* While the handler runs, a heartbeat thread keeps pushing the lease forward.
* If the worker dies (kill -9, power loss), the heartbeat stops, the lease expires, and *any* worker
  puts the job back to `queued`. That is the whole crash-recovery story.
* Every claim increments `attempts`. `attempts` **never goes down**. It is also the **fencing token**:
  completing / failing / heartbeating a job only works if `status='running' AND attempts = <my attempt>`.
  A zombie worker that comes back after its lease expired holds an old token, matches 0 rows, and its
  result is thrown away.

## 2. Delivery guarantee (stated precisely)

* **No job is lost.** Once `POST /jobs` returns 2xx the row is committed in Postgres. From then on the
  job is always in exactly one of: `queued` (will be claimed), `running` (has a lease that will either
  be renewed or expire and be requeued), `succeeded`, `failed` (kept for a human). There is no state
  from which it can silently disappear. (Caveat: only as durable as the Postgres disk. See LIMITATIONS.)
* **Handler execution is AT-LEAST-ONCE.** A handler can run more than once for the same job: worker
  dies after the side effect but before the commit of `succeeded`; or the lease expires on a slow
  worker and a second worker starts. Exactly-once execution is impossible across a crash, so we do not claim it.
* **Recording the outcome is exactly-once.** Fencing guarantees at most one attempt can write the
  final state of a given run.
* **Effectively-once is available to job authors** who follow the rules below.

### What a job author must do

1. Make the handler **idempotent**. Pass `ctx.idempotency_key` (= `job-<uuid>`, identical on every
   attempt) to every external system that supports one (the partner call does: `Idempotency-Key` header).
2. Do **one external call per attempt**, with a timeout. No hidden internal retry loop (the queue is the retry loop).
3. Never assume "this is the first attempt" (`ctx.attempt` tells you).
4. Return JSON-serialisable data (it is stored as the result).
5. Raise `PermanentError` for "retrying can never help" (e.g. HTTP 400). Any other exception = transient, retried.

## 3. Decisions where the brief is silent or contradicts itself

| Topic | Decision |
|---|---|
| Statuses | `queued`, `running`, `succeeded`, `failed`. `failed` is terminal ("give up, human please"). A job waiting for its next retry is `queued` with `attempts > 0`. |
| Priority | Integer, **higher runs first**, default 0, range -100..100. Ties: earlier `run_at`, then `created_at`. Strict priority: a flood of high-priority jobs can starve low ones (documented, accepted). |
| `run_at` | Optional ISO-8601. Missing = now. A timestamp without a timezone is read as UTC. A past `run_at` = run immediately. |
| Idempotent submit | Optional key in the `Idempotency-Key` header **or** `idempotency_key` body field (both given and different -> 400). Same key + same request -> **200** and the original job. New job -> **201**. Same key + *different* request -> **409**. Without a key we cannot know a retry from a new job, so no de-duplication (see open questions). |
| Request identity | "Same request" = hash of only the fields the client *sent* (name, payload, priority, run_at). A server default (run_at = now) must not make a retry look different. |
| Retry backoff | Exponential with **full jitter**: `delay = uniform(0, min(300s, 5s * 2^(attempt-1)))`. Jitter stops a partner outage from being hit by a synchronised herd. |
| Retry budget | Per job type, copied onto the row at submit (`max_attempts`, default 5). A worker crash consumes an attempt too, so a job that kills its worker (poison pill) cannot loop forever. |
| Permanent failure | `PermanentError`, unknown job type on a worker, or budget exhausted -> `failed`, with `last_error`. |
| Human retry | `POST /jobs/{id}/retry` on a `failed` job -> `queued`, and `max_attempts = attempts + <type budget>`. `attempts` is **not reset**: that keeps the fencing token monotonic (a zombie from the old run can never match the new run). |
| Time | Every timestamp comes from Postgres `now()` (`timestamptz`). Workers' and API's clocks are never compared. |
| Result | Stored as JSON on the row; `GET /jobs/{id}` shows `result` once `succeeded`, `last_error` if it has failed. |
| Lease | 30 s, heartbeat every 10 s. A handler also has a hard `timeout` after which the heartbeat stops, so a wedged handler cannot hold a job forever. |
| Shutdown | SIGTERM = finish the current job, then exit (so `docker stop` is graceful). SIGKILL is handled by the lease. |
| Auth | None. Assumed to run on a trusted internal network. |
| Stats | `GET /stats` returns counts by status, backlog, oldest-ready-job age, expired leases and a single `health: ok/degraded` with reasons. |

## 4. API

* `POST /jobs` `{name, payload?, priority?, run_at?, idempotency_key?}` -> 201 new / 200 replay / 409 same key, different request / 400 header and body keys disagree / 422 invalid
* `GET /jobs/{id}` -> job (status, attempts, result, last_error, timestamps)
* `GET /stats`
* Extras (my design, not in the fixed contract): `GET /jobs?status=failed` (the human's inbox), `POST /jobs/{id}/retry`.

## 5. Assumptions

* The partner API is slow (seconds) and fails randomly; **it may or may not honour an idempotency key**
  (we send one regardless). The system is correct either way. Only the *number of duplicate effects* differs, and the tests measure both cases.
* Jobs are seconds long, not hours. Payloads are small (< 1 MB).
* Thousands of jobs, a handful of workers. Polling every 0.5 s is fine at this scale.
* Postgres is trusted to be durable (fsync on) and is a single instance.

## 6. Open questions I would ask the client

1. Does the partner API accept an idempotency key? What does it do on a duplicate? Is a duplicate call harmful or just wasteful?
2. Partner limits: rate limit, concurrency limit, timeout guidance, and does it send `Retry-After`?
3. Which partner errors are permanent vs transient (is 4xx ever transient)?
4. Is "higher number = higher priority" what they expect? Should low priority be protected from starvation?
5. How long must `succeeded` jobs be kept? (We keep forever today, no cleanup job.)
6. Who is the "human" that retries failed jobs? Do we need authentication or an audit trail?
7. Should clients without an idempotency key still be de-duplicated (e.g. by payload hash)? That would block legitimate repeats.
8. What is an acceptable pickup latency (affects polling vs LISTEN/NOTIFY)?
9. Expected volume and largest payload?
10. Status names and `/stats` shape: do existing clients expect specific ones?

## 7. Alternatives I rejected

* **Celery/RQ/Kafka/Redis**: forbidden, and a second system to lose data in.
* **Exactly-once execution**: impossible if a process can die between "did the thing" and "wrote it down". We give at-least-once + idempotency keys instead.
* **Claim = delete the row / pop from a list**: a crash after the pop loses the job. Leases keep the row.
* **Advisory locks as the lock**: a lock vanishing with the connection is neat, but then there is no record of *who* has the job, and a stuck-but-connected worker is invisible. Leases are visible in the table and in `/stats`.
* **Plain `SELECT` then `UPDATE`** (no `SKIP LOCKED`): two workers can both read the same row; or they queue up behind each other.
* **LISTEN/NOTIFY** for instant wakeup: nicer latency, more moving parts (lost notifications still need polling). Polling is the safe baseline; can be added later.
* **Resetting `attempts` on manual retry**: simpler to explain but breaks fencing (a zombie from the old run could match the new run's token).
* **De-duplicating by payload hash**: blocks legitimate identical jobs ("send the same reminder email again").
* **A separate `job_attempts` history table**: better forensics; I kept `attempts` + `last_error` to stay small. First thing to add if asked.
* **A `workers` table / worker heartbeats**: lag (`oldest_ready_age`) and expired leases already reveal dead workers.
* **Async SQLAlchemy**: worker handlers are blocking by nature; sync code is easier to explain and debug.
