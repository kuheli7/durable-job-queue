# The task (as sent by the mentor), and where each requirement is met

## The brief (verbatim)

Teams need background work to run reliably: sending emails, resizing images, calling flaky partner APIs. Build a small job queue service and worker. Postgres is the only infrastructure. No Celery, RQ or Kafka.

**It must let**
- Clients submit named jobs with a payload and an optional priority and run-at time, then check status and result.
- Many workers run at once and can die at any moment, including mid-job.
- Failed jobs retry sensibly. Jobs that never succeed are kept for a human to inspect and retry.
- A client retrying a submission must not create two jobs.
- Operators see system health at a glance.
- Your mentor will give you a mock "partner API" in week 1 that is slow and fails randomly. Ship a job type that calls it.

**Guarantees (you prove these)**
- No job is ever lost.
- State precisely what delivery guarantee you give and what job authors must do to stay correct.
- Your chaos test (kill workers mid-job) and your soak test (thousands of jobs, several workers, random kills) check your claims against what the mock partner actually received.

**Fixed contract (our tests call these):** `POST /jobs`, `GET /jobs/{id}`, `GET /stats`. Everything else is your design.

**Stack:** Python 3.11, FastAPI, SQLAlchemy 2 + Alembic, Postgres 15, pytest, ruff and mypy clean (no inline suppressions), everything runs with `docker compose up`.

**Deliverables**
- DESIGN.md by end of week 1: assumptions, open questions you would ask the client, and alternatives you rejected. It is reviewed before you build further.
- Your own tests that prove each guarantee above.
- A README and a CI workflow.
- LIMITATIONS.md: how the system can still fail.
- A live walkthrough at the end where you change something on the spot.

**Rules**
- The brief is intentionally incomplete. Where it is silent or contradicts itself, decide, write it down and apply it consistently.
- Expect one requirement change mid-month.
- You may use AI tools, but you must be able to explain and modify any line live.

## Checklist

| Requirement | Status | Where |
|---|---|---|
| Submit named jobs with payload, optional priority and run-at; check status and result | Done | `POST /jobs`, `GET /jobs/{id}` in `app/api.py` |
| Many workers, any can die mid-job | Done | leases + heartbeat + fencing in `app/queue.py`, `app/worker.py`; `tests/test_chaos.py` |
| Sensible retries; never-succeeding jobs kept for a human to inspect and retry | Done | backoff in `queue.fail`; `failed` status; `GET /jobs?status=failed`; `POST /jobs/{id}/retry` |
| Client retry never creates two jobs | Done | `queue.submit` (unique key + `ON CONFLICT DO NOTHING`) |
| Operators see health at a glance | Done | `GET /stats` (`health` ok/degraded with reasons); `GET /health` for liveness |
| Job type that calls the partner API | Done, against my stand-in | `call_partner` in `app/handlers.py`; stand-in in `partner_mock/main.py`. **Replace with the mentor's mock when it arrives.** |
| No job ever lost | Done, proven | chaos and soak tests assert nothing is stuck and every job is succeeded or kept as failed |
| Delivery guarantee stated; what job authors must do | Done | `DESIGN.md` section 2 |
| Chaos and soak tests checked against what the partner received | Done | `tests/harness.py::assert_guarantees`, `tests/test_chaos.py`, `tests/test_soak.py` |
| Fixed contract | Done | the three endpoints above |
| Python 3.11, FastAPI, SQLAlchemy 2, Alembic, Postgres 15, pytest | Done | `pyproject.toml`, `Dockerfile`, `docker-compose.yml` |
| ruff and mypy clean, no inline suppressions | Done | CI runs both; no `noqa`, `type: ignore` or `fmt:` comments in the code |
| Everything runs with `docker compose up` | Done | `docker-compose.yml` |
| DESIGN.md (assumptions, open questions, rejected alternatives) | Written; **mentor review pending** | `DESIGN.md` sections 5, 6, 7 |
| Tests proving each guarantee | Done | `tests/` |
| README, CI workflow | Done | `README.md`, `.github/workflows/ci.yml` |
| LIMITATIONS.md | Done | `LIMITATIONS.md` |
| Live walkthrough with an on-the-spot change | **To do** | prep in `WALKTHROUGH.md` |
| Mid-month requirement change | **To do** (not received yet) | will update `DESIGN.md`, tests and `LIMITATIONS.md` |
| Real partner API from the mentor | **To do** (not received yet) | adapt `call_partner`, rerun chaos and soak |
