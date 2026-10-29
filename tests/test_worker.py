"""Worker loop tests, in-process, with custom handlers (real process kills: test_chaos)."""

import dataclasses
import threading
import time
from typing import Any

import pytest
from sqlalchemy.orm import Session, sessionmaker

from app import queue
from app.config import Settings
from app.handlers import HandlerSpec, JobContext, PermanentError
from app.models import FAILED, QUEUED, SUCCEEDED, Job
from app.worker import run_one
from tests.test_queue import add, status_of

FAST = dataclasses.replace(
    Settings.from_env(),
    lease_seconds=5,
    heartbeat_seconds=0.1,
    backoff_base_seconds=0,  # retry delay 0: retries are immediately due
    backoff_cap_seconds=0,
)


def flaky(ctx: JobContext) -> dict[str, Any]:
    if ctx.attempt < 3:
        raise RuntimeError("partner 503")
    return {"ok": True, "attempt": ctx.attempt}


def rejects(_ctx: JobContext) -> dict[str, Any]:
    raise PermanentError("400 bad request")


def junk(_ctx: JobContext) -> dict[str, Any]:
    return {"not json": object()}


REGISTRY = {
    "flaky": HandlerSpec(flaky),
    "rejects": HandlerSpec(rejects),
    "junk": HandlerSpec(junk),
}


def test_retries_a_flaky_job_until_it_succeeds(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    job = add(session, name="flaky")
    assert run_one(session_factory, "w", REGISTRY, FAST)
    first = status_of(session, job)
    assert first.status == QUEUED and first.last_error == "RuntimeError: partner 503"
    assert run_one(session_factory, "w", REGISTRY, FAST)
    assert run_one(session_factory, "w", REGISTRY, FAST)
    done = status_of(session, job)
    assert done.status == SUCCEEDED and done.attempts == 3
    assert done.result == {"ok": True, "attempt": 3}
    assert not run_one(session_factory, "w", REGISTRY, FAST)  # nothing left


def test_permanent_error_goes_straight_to_failed(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    job = add(session, name="rejects")
    run_one(session_factory, "w", REGISTRY, FAST)
    row = status_of(session, job)
    assert row.status == FAILED and row.attempts == 1 and "400 bad request" in str(row.last_error)


def test_unknown_handler_and_unstorable_result_fail_for_a_human_to_look_at(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    missing = add(session, name="was-removed-in-a-deploy")
    unstorable = add(session, name="junk")
    run_one(session_factory, "w", REGISTRY, FAST)
    run_one(session_factory, "w", REGISTRY, FAST)
    assert status_of(session, missing).status == FAILED
    assert status_of(session, unstorable).status == FAILED


def test_heartbeat_holds_the_lease_for_a_job_longer_than_the_lease(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    def slow(_ctx: JobContext) -> dict[str, Any]:
        time.sleep(1.5)
        return {"done": True}

    settings = dataclasses.replace(FAST, lease_seconds=0.5)  # job takes 3x the lease
    job = add(session, name="slow")
    t = threading.Thread(
        target=run_one, args=(session_factory, "A", {"slow": HandlerSpec(slow)}, settings)
    )
    t.start()
    time.sleep(1.0)  # well past the original lease
    assert queue.claim(session, "B", 30) is None  # not stolen
    t.join()
    row = status_of(session, job)
    assert row.status == SUCCEEDED and row.attempts == 1


def test_wedged_handler_loses_the_job_and_its_late_result_is_discarded(
    session: Session, session_factory: sessionmaker[Session]
) -> None:
    def wedged(_ctx: JobContext) -> dict[str, Any]:
        time.sleep(1.5)
        return {"from": "wedged worker A"}

    # hard timeout 0.3 s: heartbeat stops, lease (0.4 s) expires while the handler is still stuck
    registry = {"wedged": HandlerSpec(wedged, timeout_seconds=0.3)}
    settings = dataclasses.replace(FAST, lease_seconds=0.4)
    job = add(session, name="wedged")
    t = threading.Thread(target=run_one, args=(session_factory, "A", registry, settings))
    t.start()
    time.sleep(0.9)
    taken = queue.claim(session, "B", 30)  # B takes over while A is still stuck
    assert taken is not None and taken.attempt == 2
    assert queue.complete(session, taken, {"from": "B"})
    t.join()  # A finally returns and tries to write: fenced out

    row: Job = status_of(session, job)
    assert row.status == SUCCEEDED and row.result == {"from": "B"}


def test_worker_survives_a_database_blip_between_claim_and_result(
    session: Session,
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If recording the outcome fails, the job stays `running`; it is retried after the lease."""
    job = add(session, name="flaky")

    def boom(*_a: object, **_k: object) -> None:
        raise ConnectionError("db went away")

    monkeypatch.setattr(queue, "fail", boom)
    with pytest.raises(ConnectionError):
        run_one(session_factory, "w", REGISTRY, dataclasses.replace(FAST, lease_seconds=0.3))
    assert status_of(session, job).status == "running"
    time.sleep(0.5)
    again = queue.claim(session, "w2", 30)
    assert again is not None and again.id == job.id and again.attempt == 2
