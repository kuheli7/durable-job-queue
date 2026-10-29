"""The queue. EVERY SQL statement of the system is in this file.

Rules that make it safe (see DESIGN.md):
  * Each function is one short transaction and commits before returning. In particular `claim`
    commits BEFORE the handler runs, so no transaction is held open while a job executes.
  * `attempts` is the fencing token. It only ever grows. Every statement that changes a running
    job also says `status = 'running' AND attempts = <my attempt>`, so a worker whose lease was
    taken away matches 0 rows and its late write is discarded.
  * All times come from Postgres now(); the clocks of API/workers are never compared.
"""

import hashlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import ColumnElement, and_, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import FAILED, QUEUED, RUNNING, SUCCEEDED, Job


class IdempotencyConflict(Exception):
    """Same idempotency key was reused for a different request."""


@dataclass(frozen=True)
class Claimed:
    """What a worker needs to run a job. `attempt` is its fencing token."""

    id: uuid.UUID
    name: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int


# ---------------------------------------------------------------- submitting / reading


def fingerprint(sent: dict[str, Any]) -> str:
    """Hash of the fields the client actually sent (defaults filled in by us are excluded)."""
    canonical = json.dumps(sent, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def submit(
    session: Session,
    *,
    name: str,
    payload: dict[str, Any],
    priority: int,
    run_at: datetime | None,
    max_attempts: int,
    idempotency_key: str | None,
    request_hash: str,
) -> tuple[Job, bool]:
    """Insert a job. Returns (job, created). A repeated idempotency key returns the original job."""
    stmt = (
        insert(Job)
        .values(
            id=uuid.uuid4(),
            name=name,
            payload=payload,
            priority=priority,
            run_at=run_at if run_at is not None else func.now(),
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )
        # The unique index on idempotency_key decides races: of N concurrent inserts with the
        # same key exactly one inserts, the others do nothing (they wait for it to commit first).
        .on_conflict_do_nothing(index_elements=[Job.idempotency_key])
        .returning(Job)
    )
    job = session.execute(stmt).scalar_one_or_none()
    if job is not None:
        session.commit()
        return job, True

    existing = session.scalars(select(Job).where(Job.idempotency_key == idempotency_key)).one()
    session.commit()
    if existing.request_hash != request_hash:
        raise IdempotencyConflict(idempotency_key)
    return existing, False


def get_job(session: Session, job_id: uuid.UUID) -> Job | None:
    job = session.get(Job, job_id)
    session.commit()
    return job


def list_jobs(session: Session, status: str | None, limit: int) -> list[Job]:
    stmt = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    jobs = list(session.scalars(stmt))
    session.commit()
    return jobs


def retry_failed(session: Session, job_id: uuid.UUID, budget: int) -> Job | None:
    """Human retry of a failed job. Returns None if the job is not in the `failed` state.

    `attempts` is NOT reset (it is the fencing token); instead the budget is extended.
    """
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == FAILED)
        .values(
            status=QUEUED,
            run_at=func.now(),
            max_attempts=Job.attempts + budget,
            finished_at=None,
            locked_by=None,
            lease_expires_at=None,
        )
        .returning(Job)
        # populate_existing: the caller's session may already hold this row as `failed`; without
        # it SQLAlchemy would hand back that stale object instead of the row we just updated.
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    job = session.execute(stmt).scalar_one_or_none()
    session.commit()
    return job


# ---------------------------------------------------------------- the worker's side


def _expired_lease_ids(extra: ColumnElement[bool]) -> Any:
    """ids of running jobs whose lease ran out, locked so concurrent reapers don't collide."""
    return (
        select(Job.id)
        .where(Job.status == RUNNING, Job.lease_expires_at < func.now(), extra)
        .with_for_update(skip_locked=True)
    )


def requeue_expired(session: Session) -> int:
    """Recover jobs whose worker died. Any worker may call this; it is safe to run concurrently.

    A job with budget left goes back to `queued` immediately (a crash is not the job's fault, so
    no backoff). A job that has used all its attempts becomes `failed` (poison-pill protection).
    """
    gave_up = session.execute(
        update(Job)
        .where(Job.id.in_(_expired_lease_ids(Job.attempts >= Job.max_attempts)))
        .values(
            status=FAILED,
            last_error="worker lost on the final attempt (lease expired)",
            finished_at=func.now(),
            locked_by=None,
            lease_expires_at=None,
        )
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).all()
    requeued = session.execute(
        update(Job)
        .where(Job.id.in_(_expired_lease_ids(Job.attempts < Job.max_attempts)))
        .values(
            status=QUEUED,
            run_at=func.now(),
            last_error="worker lost (lease expired), job requeued",
            locked_by=None,
            lease_expires_at=None,
        )
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).all()
    session.commit()
    return len(gave_up) + len(requeued)


def claim(session: Session, worker_id: str, lease_seconds: float) -> Claimed | None:
    """Take the best ready job, or None. The claim is committed before this returns."""
    requeue_expired(session)

    best_ready = (
        select(Job.id)
        .where(Job.status == QUEUED, Job.run_at <= func.now())
        .order_by(Job.priority.desc(), Job.run_at, Job.created_at)
        .limit(1)
        # SKIP LOCKED: if another worker is claiming this row right now, take the next one
        .with_for_update(skip_locked=True)
    )
    job_id = session.execute(best_ready).scalar_one_or_none()
    if job_id is None:
        session.commit()
        return None

    row = session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            status=RUNNING,
            attempts=Job.attempts + 1,
            locked_by=worker_id,
            lease_expires_at=func.now() + timedelta(seconds=lease_seconds),
        )
        .returning(Job.id, Job.name, Job.payload, Job.attempts, Job.max_attempts)
        .execution_options(synchronize_session=False)
    ).one()
    session.commit()
    return Claimed(row.id, row.name, row.payload, row.attempts, row.max_attempts)


def heartbeat(session: Session, job: Claimed, lease_seconds: float) -> bool:
    """Extend the lease. False means the lease was lost (someone else owns the job now)."""
    row = session.execute(
        update(Job)
        .where(Job.id == job.id, Job.status == RUNNING, Job.attempts == job.attempt)
        .values(lease_expires_at=func.now() + timedelta(seconds=lease_seconds))
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).first()
    session.commit()
    return row is not None


def complete(session: Session, job: Claimed, result: dict[str, Any]) -> bool:
    """Record success. False = fenced out (our lease was lost): the result is discarded."""
    row = session.execute(
        update(Job)
        .where(Job.id == job.id, Job.status == RUNNING, Job.attempts == job.attempt)
        .values(
            status=SUCCEEDED,
            result=result,
            finished_at=func.now(),
            locked_by=None,
            lease_expires_at=None,
            # last_error is deliberately kept: it shows why earlier attempts failed
        )
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).first()
    session.commit()
    return row is not None


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2^(attempt-1)))."""
    ceiling = min(cap, base * 2 ** min(attempt - 1, 30))
    return random.uniform(0, ceiling)


def fail(
    session: Session,
    job: Claimed,
    error: str,
    *,
    permanent: bool,
    backoff_base: float,
    backoff_cap: float,
) -> str | None:
    """Record a failed attempt. Returns the new status, or None if fenced out.

    Out of attempts or `permanent` -> `failed` (kept for a human). Otherwise -> `queued` again,
    not to be claimed before the backoff delay has passed.
    """
    give_up = permanent or job.attempt >= job.max_attempts
    delay = timedelta(seconds=backoff_delay(job.attempt, backoff_base, backoff_cap))
    row = session.execute(
        update(Job)
        .where(Job.id == job.id, Job.status == RUNNING, Job.attempts == job.attempt)
        .values(
            status=FAILED if give_up else QUEUED,
            last_error=error[:4000],
            run_at=Job.run_at if give_up else func.now() + delay,
            finished_at=func.now() if give_up else None,
            locked_by=None,
            lease_expires_at=None,
        )
        .returning(Job.status)
        .execution_options(synchronize_session=False)
    ).first()
    session.commit()
    return None if row is None else str(row.status)


# ---------------------------------------------------------------- operators


def stats(session: Session, lag_threshold: float) -> dict[str, Any]:
    """Everything an operator needs at a glance, in one query."""
    ready = and_(Job.status == QUEUED, Job.run_at <= func.now())
    expired = and_(Job.status == RUNNING, Job.lease_expires_at < func.now())
    row = session.execute(
        select(
            func.count().filter(Job.status == QUEUED).label("queued"),
            func.count().filter(Job.status == RUNNING).label("running"),
            func.count().filter(Job.status == SUCCEEDED).label("succeeded"),
            func.count().filter(Job.status == FAILED).label("failed"),
            func.count().filter(ready).label("ready"),
            func.count().filter(Job.status == QUEUED, Job.attempts > 0).label("retrying"),
            func.count().filter(expired).label("expired_leases"),
            func.extract("epoch", func.now() - func.min(Job.run_at).filter(ready)).label("lag"),
            func.count()
            .filter(Job.status == SUCCEEDED, Job.finished_at > func.now() - timedelta(minutes=1))
            .label("succeeded_last_minute"),
            func.count()
            .filter(Job.status == FAILED, Job.finished_at > func.now() - timedelta(minutes=1))
            .label("failed_last_minute"),
        )
    ).one()
    session.commit()

    lag = None if row.lag is None else float(row.lag)
    reasons: list[str] = []
    if lag is not None and lag > lag_threshold:
        reasons.append(f"oldest ready job has waited {lag:.0f}s: workers down or overloaded")
    if row.expired_leases > 0:
        reasons.append(f"{row.expired_leases} running job(s) have an expired lease: worker died")
    return {
        "health": "degraded" if reasons else "ok",
        "reasons": reasons,
        "jobs": {
            "queued": row.queued,
            "running": row.running,
            "succeeded": row.succeeded,
            "failed": row.failed,
        },
        "ready_now": row.ready,
        "scheduled_for_later": row.queued - row.ready,
        "waiting_for_retry": row.retrying,
        "expired_leases": row.expired_leases,
        "oldest_ready_age_seconds": lag,
        "succeeded_last_minute": row.succeeded_last_minute,
        "failed_last_minute": row.failed_last_minute,
    }
