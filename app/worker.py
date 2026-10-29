"""A worker process: claim a job, run its handler (a thread renews the lease), record the outcome.

Run many of them (python -m app.worker). They coordinate only through Postgres.
If one is killed at any moment, its lease expires and another worker picks the job up again.
"""

import json
import logging
import os
import random
import signal
import socket
import threading
import time
import uuid
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from app import queue
from app.config import Settings
from app.db import make_engine, make_session_factory
from app.handlers import REGISTRY, HandlerSpec, JobContext, PermanentError

log = logging.getLogger("worker")


class Heartbeat(threading.Thread):
    """Renews the lease every `interval` seconds while the handler runs (in the main thread).

    It uses its own session: sessions are not thread-safe. It stops renewing when
      * the job finished (stop() was called),
      * the lease was lost (someone else owns the job now), or
      * the handler exceeded its hard timeout, so a wedged handler can't hold a job forever.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        job: queue.Claimed,
        lease: float,
        interval: float,
        hard_timeout: float,
    ) -> None:
        super().__init__(daemon=True)
        self._session_factory = session_factory
        self._job = job
        self._lease = lease
        self._interval = interval
        self._hard_timeout = hard_timeout
        self._stopped = threading.Event()

    def stop(self) -> None:
        self._stopped.set()
        self.join()

    def run(self) -> None:
        started = time.monotonic()
        while not self._stopped.wait(self._interval):
            if time.monotonic() - started > self._hard_timeout:
                log.warning(
                    "job %s exceeded its %ss timeout; letting the lease expire",
                    self._job.id,
                    self._hard_timeout,
                )
                return
            try:
                with self._session_factory() as session:
                    if not queue.heartbeat(session, self._job, self._lease):
                        log.warning("job %s: lease lost, stop heartbeating", self._job.id)
                        return
            except Exception:
                log.exception("heartbeat failed for job %s; will retry", self._job.id)


def _run_handler(spec: HandlerSpec, job: queue.Claimed) -> tuple[dict[str, Any] | None, str, bool]:
    """Returns (result, error, permanent). result is None when the attempt failed."""
    try:
        result = spec.fn(JobContext(job.id, job.name, job.attempt, job.payload))
    except PermanentError as exc:
        return None, f"{type(exc).__name__}: {exc}", True
    except Exception as exc:  # any other error is assumed to be transient
        return None, f"{type(exc).__name__}: {exc}", False
    try:
        json.dumps(
            result
        )  # the result must be storable; retrying can't fix a handler that returns junk
    except (TypeError, ValueError) as exc:
        return None, f"result is not JSON-serialisable: {exc}", True
    return result, "", False


def run_one(
    session_factory: sessionmaker[Session],
    worker_id: str,
    registry: dict[str, HandlerSpec],
    settings: Settings,
) -> bool:
    """Claim and run one job. Returns False if there was nothing to do."""
    with session_factory() as session:
        job = queue.claim(session, worker_id, settings.lease_seconds)
    if job is None:
        return False

    log.info("claimed job %s (%s) attempt %d/%d", job.id, job.name, job.attempt, job.max_attempts)
    spec = registry.get(job.name)
    if spec is None:
        result, error, permanent = None, f"no handler registered for {job.name!r}", True
    else:
        heartbeat = Heartbeat(
            session_factory,
            job,
            settings.lease_seconds,
            settings.heartbeat_seconds,
            spec.timeout_seconds,
        )
        heartbeat.start()
        try:
            result, error, permanent = _run_handler(spec, job)
        finally:
            heartbeat.stop()

    with session_factory() as session:
        if result is not None:
            recorded = queue.complete(session, job, result)
            outcome = "succeeded"
        else:
            status = queue.fail(
                session,
                job,
                error,
                permanent=permanent,
                backoff_base=settings.backoff_base_seconds,
                backoff_cap=settings.backoff_cap_seconds,
            )
            recorded = status is not None
            outcome = f"failed ({error}) -> {status}"
    if recorded:
        log.info("job %s %s", job.id, outcome)
    else:
        log.warning("job %s: result discarded, our lease had been lost (%s)", job.id, outcome)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = Settings.from_env()
    session_factory = make_session_factory(make_engine(settings.database_url))
    worker_id = os.environ.get("WORKER_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

    stopping = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stopping.set()  # finish the job in hand, then exit (docker stop sends SIGTERM)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    log.info("worker %s started", worker_id)
    while not stopping.is_set():
        try:
            worked = run_one(session_factory, worker_id, REGISTRY, settings)
        except Exception:
            log.exception("worker loop error (database down?); retrying")
            worked = False
        if not worked:
            # idle: sleep with a little jitter so many workers don't poll in lockstep
            stopping.wait(settings.poll_seconds * (0.5 + random.random()))
    log.info("worker %s stopped", worker_id)


if __name__ == "__main__":
    main()
