# Durable job queue (Postgres only)

A small job queue service: clients submit named jobs over HTTP, a pool of workers runs them,
and nothing is lost when a worker dies. Postgres is the only infrastructure (no Celery, RQ or Kafka).

* **DESIGN.md** is the design and the exact delivery guarantee. Read it first.
* **WALKTHROUGH.md** maps every guarantee to the code that enforces it and the test that proves it.
* **LIMITATIONS.md** says honestly what this does not do.

## Run it

```
docker compose up --build
```

This starts Postgres 15 (host port **5433**, to stay clear of a local Postgres on 5432), runs the
migrations once, then starts the API (http://localhost:8000), **3 workers**, and the mentor's mock
partner API (`mock_partner_api/`, http://localhost:9000, slow and randomly failing; what it really
received is at http://localhost:9000/audit). Interactive docs: http://localhost:8000/docs

## API

```
# submit (the Idempotency-Key header, or an "idempotency_key" body field, makes retries safe)
curl -i -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -H 'Idempotency-Key: order-42' \
     -d '{"name":"call_partner","payload":{"to":"someone"},"priority":5}'
#  201 = new job, 200 = same key + same request (original job returned), 409 = same key, different request

curl localhost:8000/jobs/<id>            # status, attempts, result, last_error
curl localhost:8000/stats                # counts, backlog, oldest waiting job, expired leases, health
curl localhost:8000/health               # extra: 200 if the database answers, 503 if not
curl 'localhost:8000/jobs?status=failed' # extra: the inbox of jobs a human must look at
curl -X POST localhost:8000/jobs/<id>/retry   # extra: give a failed job a fresh retry budget
```

Fields: `name` (job type), `payload` (JSON object, at most 1 MB), `priority` (-100..100, higher first, default 0),
`run_at` (ISO-8601; no timezone means UTC; omitted means now),
`max_attempts` (1..20; omitted means the job type's default, 5 for `echo`, 6 for `call_partner`).
Statuses: `queued`, `running`, `succeeded`, `failed`.
Job types live in `app/handlers.py`: `echo`, `call_partner` (calls the partner's `POST /v1/deliver`
with `Idempotency-Key: job-<id>`) and `always_fail` (a demo that never succeeds: it ends up in the
`failed` inbox after 3 attempts).

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://jobs:jobs@localhost:5433/jobs` | Postgres connection |
| `LEASE_SECONDS` | 30 | how long a claim lasts without a heartbeat |
| `HEARTBEAT_SECONDS` | 10 | how often a running job renews its lease |
| `POLL_SECONDS` | 0.5 | idle worker poll interval |
| `BACKOFF_BASE_SECONDS` / `BACKOFF_CAP_SECONDS` | 5 / 300 | retry backoff (full jitter) |
| `PARTNER_API_URL` / `PARTNER_TIMEOUT_SECONDS` | `http://localhost:9000` / 10 | partner client |
| `LAG_THRESHOLD_SECONDS` | 30 | `/stats` says `degraded` if the oldest ready job waited longer |

The mentor's mock is configured by its own `MOCK_*` variables (see `mock_partner_api/README.md`).
Its `GET /audit` and `GET /audit/jobs/{id}` are what the tests compare against, and
`POST /admin/reset` clears them (its log lives in the `partner_data` volume, so it survives restarts).
`mock_partner_api/` is her code, kept as she sent it, and is excluded from ruff in `pyproject.toml`
(a config exclusion; there are no inline suppressions anywhere).

## Develop and test

```
python -m venv .venv && .venv\Scripts\activate     # Linux/macOS: source .venv/bin/activate
pip install -e ".[dev]"
docker compose stop api worker partner               # keep ONLY the db up: see the note below
docker compose up -d db                              # tests use the database on localhost:5433
ruff check . && ruff format --check . && mypy        # clean, with no inline suppressions
pytest                                               # unit, API, worker and chaos tests (~4 min)
SOAK_JOBS=2000 SOAK_WORKERS=6 pytest -m soak         # soak: thousands of jobs, random kills (~15 min against the mentor's mock)
```

**The tests TRUNCATE the `jobs` table of the database on port 5433.** Run them with only the `db`
service up. If the compose workers are running they will claim the tests' jobs and tests will fail
at random. (The chaos/soak tests start their own API, partner and workers on free ports.)

The chaos and soak tests start the real API, partner and worker processes, `kill -9` workers that
are provably in the middle of a job, and then check the queue's claims against what the partner
actually received (`tests/harness.py`).
