"""Queue-core tests against real Postgres. Each one pins down a rule from DESIGN.md."""

import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app import queue
from app.models import FAILED, QUEUED, RUNNING, SUCCEEDED, Job

LEASE = 30.0


def add(
    session: Session,
    *,
    name: str = "echo",
    priority: int = 0,
    run_at: datetime | None = None,
    max_attempts: int = 5,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Job:
    job, _ = queue.submit(
        session,
        name=name,
        payload=payload or {},
        priority=priority,
        run_at=run_at,
        max_attempts=max_attempts,
        idempotency_key=key,
        request_hash=queue.fingerprint({"name": name, "payload": payload or {}}),
    )
    return job


def status_of(session: Session, job: Job) -> Job:
    session.expire_all()
    fresh = session.get(Job, job.id)
    assert fresh is not None
    session.commit()  # end the read transaction: Postgres now() is frozen at transaction start
    return fresh


def make_due(session: Session) -> None:
    session.execute(text("UPDATE jobs SET run_at = now() WHERE status = 'queued'"))
    session.commit()


# ------------------------------------------------------------ ordering


def test_highest_priority_first_then_earliest_run_at(session: Session) -> None:
    now = datetime.now(UTC)
    low = add(session, priority=0, run_at=now - timedelta(seconds=30))
    older = add(session, priority=5, run_at=now - timedelta(seconds=20))
    newer = add(session, priority=5, run_at=now - timedelta(seconds=10))
    top = add(session, priority=9)

    order = [queue.claim(session, "w", LEASE) for _ in range(4)]
    assert [c.id for c in order if c] == [top.id, older.id, newer.id, low.id]
    assert queue.claim(session, "w", LEASE) is None


def test_job_scheduled_in_the_future_is_not_claimed(session: Session) -> None:
    add(session, run_at=datetime.now(UTC) + timedelta(hours=1))
    assert queue.claim(session, "w", LEASE) is None


# ------------------------------------------------------------ exclusivity


def test_concurrent_workers_never_claim_the_same_job(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    ids = {add(session).id for _ in range(60)}
    claimed: list[Any] = []
    lock = threading.Lock()

    def worker(n: int) -> None:
        with session_factory() as s:
            while (c := queue.claim(s, f"w{n}", LEASE)) is not None:
                with lock:
                    claimed.append(c.id)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claimed) == len(ids)  # nothing claimed twice...
    assert set(claimed) == ids  # ...and nothing left behind


# ------------------------------------------------------------ crash recovery + fencing


def test_expired_lease_is_requeued_and_stale_completion_is_a_noop(session: Session) -> None:
    job = add(session)
    first = queue.claim(session, "worker-A", lease_seconds=0.2)
    assert first is not None and first.attempt == 1
    time.sleep(0.5)  # worker-A "dies": no heartbeat

    second = queue.claim(session, "worker-B", LEASE)  # reaper requeues, B claims
    assert second is not None and second.id == job.id and second.attempt == 2

    # worker-A wakes up (zombie) and tries to write its result: must change nothing
    assert queue.complete(session, first, {"from": "A"}) is False
    assert queue.fail(session, first, "boom", permanent=True, backoff_base=1, backoff_cap=1) is None
    assert queue.heartbeat(session, first, LEASE) is False
    row = status_of(session, job)
    assert row.status == RUNNING and row.locked_by == "worker-B" and row.result is None

    assert queue.complete(session, second, {"from": "B"}) is True
    row = status_of(session, job)
    assert row.status == SUCCEEDED and row.result == {"from": "B"}


def test_heartbeat_keeps_a_slow_job_from_being_reclaimed(session: Session) -> None:
    add(session)
    c = queue.claim(session, "w", lease_seconds=0.4)
    assert c is not None
    assert queue.heartbeat(session, c, lease_seconds=30) is True
    time.sleep(0.6)
    assert queue.claim(session, "other", LEASE) is None


def test_poison_pill_that_keeps_killing_workers_ends_up_failed(session: Session) -> None:
    job = add(session, max_attempts=2)
    for expected_attempt in (1, 2):
        c = queue.claim(session, "w", lease_seconds=0.2)
        assert c is not None and c.attempt == expected_attempt
        time.sleep(0.5)  # worker dies every time
    assert queue.claim(session, "w", LEASE) is None  # reaper runs: budget gone
    row = status_of(session, job)
    assert row.status == FAILED and row.last_error is not None and "lease expired" in row.last_error


# ------------------------------------------------------------ retries


def test_transient_failure_backs_off_then_gives_up(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("random.uniform", lambda _a, b: b)  # no jitter: delay = ceiling
    job = add(session, max_attempts=2)

    c1 = queue.claim(session, "w", LEASE)
    assert c1 is not None
    assert (
        queue.fail(session, c1, "503", permanent=False, backoff_base=100, backoff_cap=1000)
        == QUEUED
    )
    assert queue.claim(session, "w", LEASE) is None  # still backing off (~100 s)
    row = status_of(session, job)
    assert row.status == QUEUED and row.attempts == 1 and row.last_error == "503"

    make_due(session)
    c2 = queue.claim(session, "w", LEASE)
    assert c2 is not None and c2.attempt == 2
    assert (
        queue.fail(session, c2, "503 again", permanent=False, backoff_base=1, backoff_cap=1)
        == FAILED
    )
    assert status_of(session, job).finished_at is not None


def test_permanent_error_fails_immediately(session: Session) -> None:
    job = add(session)
    c = queue.claim(session, "w", LEASE)
    assert c is not None
    assert (
        queue.fail(session, c, "bad request", permanent=True, backoff_base=1, backoff_cap=1)
        == FAILED
    )
    assert status_of(session, job).attempts == 1


def test_backoff_delay_is_jittered_and_capped() -> None:
    for attempt in (1, 2, 5, 50, 5000):
        delays = [queue.backoff_delay(attempt, base=5, cap=300) for _ in range(200)]
        assert all(0 <= d <= min(300, 5 * 2 ** (attempt - 1)) for d in delays)
    assert len({queue.backoff_delay(10, 5, 300) for _ in range(50)}) > 1  # actually random


def test_human_retry_extends_budget_and_never_reuses_a_fencing_token(session: Session) -> None:
    job = add(session, max_attempts=1)
    old = queue.claim(session, "w", LEASE)
    assert old is not None
    queue.fail(session, old, "nope", permanent=False, backoff_base=1, backoff_cap=1)
    assert status_of(session, job).status == FAILED

    assert queue.retry_failed(session, job.id, budget=3) is not None
    row = status_of(session, job)
    assert (row.status, row.attempts, row.max_attempts) == (QUEUED, 1, 4)

    new = queue.claim(session, "w", LEASE)
    assert new is not None and new.attempt == 2  # token 1 is never handed out again
    assert queue.complete(session, old, {"zombie": True}) is False
    assert (
        queue.retry_failed(session, job.id, budget=3) is None
    )  # only `failed` jobs can be retried


# ------------------------------------------------------------ idempotent submit


def test_same_key_returns_the_original_job(session: Session) -> None:
    first, created1 = queue.submit(
        session,
        name="echo",
        payload={"a": 1},
        priority=0,
        run_at=None,
        max_attempts=5,
        idempotency_key="k1",
        request_hash="h",
    )
    again, created2 = queue.submit(
        session,
        name="echo",
        payload={"a": 1},
        priority=0,
        run_at=None,
        max_attempts=5,
        idempotency_key="k1",
        request_hash="h",
    )
    assert (created1, created2) == (True, False) and first.id == again.id


def test_same_key_with_a_different_request_is_a_conflict(session: Session) -> None:
    add(session, key="k2", payload={"a": 1})
    with pytest.raises(queue.IdempotencyConflict):
        add(session, key="k2", payload={"a": 2})


def test_ten_concurrent_submits_with_one_key_make_exactly_one_job(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    created: list[bool] = []
    ids: set[Any] = set()
    lock = threading.Lock()
    barrier = threading.Barrier(10)

    def submit_it() -> None:
        with session_factory() as s:
            barrier.wait()  # all ten fire at the same moment
            job, was_created = queue.submit(
                s,
                name="echo",
                payload={},
                priority=0,
                run_at=None,
                max_attempts=5,
                idempotency_key="race",
                request_hash="h",
            )
            with lock:
                created.append(was_created)
                ids.add(job.id)

    threads = [threading.Thread(target=submit_it) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert created.count(True) == 1 and len(created) == 10
    assert len(ids) == 1
    assert session.execute(text("SELECT count(*) FROM jobs")).scalar_one() == 1


# ------------------------------------------------------------ stats


def test_stats_reports_health(session: Session) -> None:
    empty = queue.stats(session, lag_threshold=30)
    assert empty["health"] == "ok" and empty["oldest_ready_age_seconds"] is None

    add(session)
    add(session, run_at=datetime.now(UTC) + timedelta(hours=1))
    # make the first job (the one that is due) look like it has waited 2 minutes
    session.execute(text("UPDATE jobs SET run_at = now() - interval '2 min' WHERE run_at < now()"))
    session.commit()
    s = queue.stats(session, lag_threshold=30)
    assert s["jobs"]["queued"] == 2 and s["ready_now"] == 1 and s["scheduled_for_later"] == 1
    assert s["health"] == "degraded" and s["oldest_ready_age_seconds"] > 100  # nobody is working

    c = queue.claim(session, "w", lease_seconds=0.1)
    assert c is not None
    time.sleep(0.3)
    s = queue.stats(session, lag_threshold=30)
    assert s["expired_leases"] == 1 and s["health"] == "degraded"
