# Walkthrough: how to explain this project

## The 60-second version

"A job is one row in Postgres. A worker claims it by marking it `running` with a 30-second lease and
keeps renewing the lease while it works. If the worker is killed, the lease runs out and another worker
re-queues the job. Because a worker can die after doing the work but before writing it down, jobs can run
twice, so I promise **at-least-once execution**, record each outcome **exactly once**, and give job authors an
idempotency key so the partner can make repeats harmless. Client retries of `POST /jobs` are de-duplicated
by a unique key in the database. I proved it by running real processes, `kill -9`-ing workers in the middle of
jobs, and comparing what the queue says with what the partner API actually received."

## Guarantee -> code -> proof

| Claim | Where it is enforced | Test that proves it |
|---|---|---|
| Two workers never take the same job | `queue.claim`: `SELECT ... FOR UPDATE SKIP LOCKED`, committed before the handler runs | `test_concurrent_workers_never_claim_the_same_job` (8 threads, 60 jobs) |
| A dead worker's job is picked up again | lease: `claim` sets `lease_expires_at`; `requeue_expired` (called at the start of every `claim`) puts expired jobs back | `test_expired_lease_is_requeued_and_stale_completion_is_a_noop`; chaos test |
| A slow job is not stolen | `worker.Heartbeat` thread calls `queue.heartbeat` every 10 s | `test_heartbeat_holds_the_lease_for_a_job_longer_than_the_lease` |
| A zombie worker cannot overwrite the result | fencing: `heartbeat`, `complete`, `fail` all `WHERE status='running' AND attempts = <my token>` | the two tests above, `test_wedged_handler_loses_the_job_...` |
| Retries are sensible | `queue.fail` + `backoff_delay` (exponential, full jitter, capped); `PermanentError` skips retries | `test_transient_failure_backs_off_then_gives_up`, `test_permanent_error_fails_immediately`, `test_backoff_delay_is_jittered_and_capped` |
| A job that kills workers can't loop forever | crashes consume an attempt, so `requeue_expired` marks it `failed` at the limit | `test_poison_pill_that_keeps_killing_workers_ends_up_failed` |
| Never-succeeding jobs are kept for a human | status `failed`; `GET /jobs?status=failed`; `POST /jobs/{id}/retry` (`queue.retry_failed`) | `test_human_retry_extends_budget_...`, `test_failed_jobs_can_be_listed_and_retried`, soak |
| Submit retries never duplicate | `queue.submit`: unique index on `idempotency_key` + `INSERT ... ON CONFLICT DO NOTHING`; fingerprint gives 409 for a different request | `test_ten_concurrent_submits_with_one_key_make_exactly_one_job`, API tests, chaos |
| Operators see health | `queue.stats` -> `GET /stats` (counts, backlog, oldest ready age, expired leases, `health` + reasons) | `test_stats_reports_health` |
| No job lost, nothing double-counted, checked against the partner | `tests/harness.py::assert_guarantees` | `test_chaos.py` (dedupe on and off), `test_soak.py` |

## What the chaos/soak result means

(Numbers are from one 2000-job, 6-worker soak run; yours will differ, the properties will not.)

* With the partner honouring the idempotency key: 0 duplicate effects, even after 277 kills in the 2000-job soak
  (123 partner replays, 92 "applied but the answer was lost" cases).
* With the partner **not** honouring it: some jobs got 2-3 effects. That is the honest cost of at-least-once,
  and the test asserts it happens so you can see the difference.
* The test is not vacuous: the harness proves a worker held a `running` lease before killing it, and asserts
  that kills, victims, lost responses and replays actually occurred.

## Questions you may be asked

* **Why not exactly-once?** If a process dies between "did the thing" and "wrote that down", nobody can know.
  So: at-least-once + idempotency key.
* **Why not delete the row when claiming?** A crash after the pop would lose the job. A lease keeps the row.
* **Why is `attempts` never reset on human retry?** It is the fencing token. Resetting could let an old zombie match a new run.
* **What if two workers' clocks differ?** Every timestamp comes from Postgres `now()`.
* **What if the lease is too short?** The job is run twice (harmless if idempotent) and the first worker's late result is discarded.
* **What's the bottleneck?** One Postgres and polling. See LIMITATIONS.md.

## Live-change ideas, and where to touch

| Change | Where |
|---|---|
| Per-job `max_attempts` | `JobIn` in `app/schemas.py`, `submit` call in `app/api.py` (the row column already exists). The fingerprint in `api.py` is built from whatever fields the client sent (`model_fields_set`), so a new `JobIn` field is automatically part of the "same request?" check: a replay with a different value gives 409. Good thing to mention |
| Cancel a job | new `queue.cancel` (`UPDATE ... WHERE status IN ('queued','running')`), new route in `app/api.py`. A new `cancelled` status touches: the check constraint (new migration), `app/models.py` constants, `JobCounts`/`stats` and `StatsOut`. Cancelling a *running* job is safe: the worker's later `complete` matches 0 rows (fencing), though its side effect may already have happened |
| Change backoff (linear, fixed) | `queue.backoff_delay` only (and its test) |
| Different priority order | `ORDER BY` in `queue.claim` and the partial index in the migration |
| New job type | function + `REGISTRY` entry in `app/handlers.py` |
| Rate-limit / per-type concurrency | extra condition in `queue.claim` (count running of that name) |
| Retention of old jobs | a `DELETE ... WHERE status='succeeded' AND finished_at < now() - interval` helper, called from the worker loop |
| Shorter/longer lease | `LEASE_SECONDS`, `HEARTBEAT_SECONDS` env vars |
| Attempt history | new `job_attempts` table written in `claim` and `complete`/`fail` |

Before running the tests, stop the compose api/worker/partner (they share the database): `docker compose stop api worker partner`.

After any change: `ruff check . && ruff format --check . && mypy && pytest`.
