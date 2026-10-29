"""Shared machinery for the chaos and soak tests.

It starts REAL processes (API, partner mock, several workers), submits jobs over HTTP, kills workers
with SIGKILL while they provably hold a job, waits for the system to drain, and then compares
what the queue says happened with what the partner actually received.
"""

import os
import random
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import Engine, text

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


def wait_until_up(url: str, proc: subprocess.Popen[bytes]) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError(f"process for {url} exited with {proc.returncode}")
        try:
            httpx.get(url, timeout=1)
            return
        except httpx.TransportError:
            time.sleep(0.2)
    raise RuntimeError(f"{url} never came up")


class Cluster:
    """The API, the partner mock and N workers, as real OS processes."""

    def __init__(self, tmp: Path, partner_env: dict[str, str], n_workers: int) -> None:
        self.tmp = tmp
        self.partner_env = partner_env
        self.n_workers = n_workers
        self.workers: dict[str, subprocess.Popen[bytes]] = {}
        self._spawned = 0
        self._logs = ExitStack()  # log files of all child processes, closed together
        self.partner_url = f"http://127.0.0.1:{free_port()}"
        self.api_url = f"http://127.0.0.1:{free_port()}"
        self._others: list[subprocess.Popen[bytes]] = []

    def _spawn(self, args: list[str], env: dict[str, str], logname: str) -> subprocess.Popen[bytes]:
        log = self._logs.enter_context((self.tmp / logname).open("wb"))
        return subprocess.Popen(
            [sys.executable, *args],
            cwd=ROOT,
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    def start(self) -> None:
        partner = self._spawn(
            [
                "-m",
                "uvicorn",
                "partner_mock.main:app",
                "--port",
                self.partner_url.rsplit(":", 1)[1],
                "--log-level",
                "warning",
            ],
            self.partner_env,
            "partner.log",
        )
        self._others.append(partner)
        api = self._spawn(
            [
                "-m",
                "uvicorn",
                "app.api:app",
                "--port",
                self.api_url.rsplit(":", 1)[1],
                "--log-level",
                "warning",
            ],
            {},
            "api.log",
        )
        self._others.append(api)
        wait_until_up(f"{self.partner_url}/admin/effects", partner)
        wait_until_up(f"{self.api_url}/stats", api)
        for _ in range(self.n_workers):
            self.spawn_worker()

    def spawn_worker(self) -> str:
        self._spawned += 1
        worker_id = f"w{self._spawned}"
        env = {
            "WORKER_ID": worker_id,
            "PARTNER_API_URL": self.partner_url,
            "PARTNER_TIMEOUT_SECONDS": "5",
            # short lease: a killed worker's job comes back fast; short backoff: tests stay quick
            "LEASE_SECONDS": "2",
            "HEARTBEAT_SECONDS": "0.5",
            "POLL_SECONDS": "0.1",
            "BACKOFF_BASE_SECONDS": "0.2",
            "BACKOFF_CAP_SECONDS": "1",
        }
        self.workers[worker_id] = self._spawn(["-m", "app.worker"], env, f"{worker_id}.log")
        return worker_id

    def kill_worker(self, worker_id: str) -> None:
        proc = self.workers.pop(worker_id)
        proc.kill()  # SIGKILL: no cleanup, no goodbye, exactly like a crash or power loss
        proc.wait()

    def stop(self) -> None:
        for proc in [*self.workers.values(), *self._others]:
            proc.kill()
            proc.wait()
        self._logs.close()


@contextmanager
def running_cluster(tmp: Path, partner_env: dict[str, str], n_workers: int) -> Iterator[Cluster]:
    cluster = Cluster(tmp, partner_env, n_workers)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()


@dataclass
class Chaos:
    """Background thread that kills workers that are in the middle of a job."""

    cluster: Cluster
    engine: Engine
    interval: tuple[float, float]
    kills: int = 0
    # job id -> attempt that was in flight when its worker was killed (proven, see _kill_one)
    victims: dict[str, int] = field(default_factory=dict)
    # A crash costs the job an attempt, so killing the same job again and again would just starve
    # it into `failed` (which is correct behaviour, but not what this test measures).
    max_kills_per_job: int = 2
    _times_killed: Counter[str] = field(default_factory=Counter)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def _loop(self) -> None:
        while not self._stop.wait(random.uniform(*self.interval)):
            self._kill_one()

    def _kill_one(self) -> None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, locked_by, attempts, max_attempts FROM jobs "
                    "WHERE status = 'running' AND lease_expires_at > now() "
                    "ORDER BY random() LIMIT 1"
                )
            ).first()
            if row is None or row.locked_by not in self.cluster.workers:
                return
            if self._times_killed[str(row.id)] >= self.max_kills_per_job:
                return
            self.cluster.kill_worker(row.locked_by)
            # Did the job still look 'running' on the same attempt AFTER the worker was dead?
            # Then the worker certainly died mid-job (a dead process cannot have finished it).
            after = conn.execute(
                text("SELECT status, attempts FROM jobs WHERE id = :i"), {"i": row.id}
            ).one()
            died_mid_job = after.status == "running" and after.attempts == row.attempts
            if died_mid_job:
                self._times_killed[str(row.id)] += 1
                # On its FINAL attempt a job is not retried, by design: the reaper marks it
                # `failed` for a human. Only jobs with budget left must be seen running again.
                if row.attempts < row.max_attempts:
                    self.victims[str(row.id)] = row.attempts
        self.kills += 1
        self.cluster.spawn_worker()  # the operator (docker) restarts it


@dataclass
class Report:
    """Everything the tests assert on. Empty lists/dicts are the good outcome."""

    distinct_keys: int = 0
    rows_in_db: int = 0
    resubmits_not_deduplicated: list[str] = field(default_factory=list)
    stuck: list[str] = field(default_factory=list)  # not terminal when we gave up waiting
    succeeded: int = 0
    failed_for_human: int = 0  # still `failed` at the very end (only the poison jobs should be)
    human_retried: int = 0
    succeeded_without_effect: list[str] = field(default_factory=list)
    effects_exceed_attempts: list[str] = field(default_factory=list)
    unknown_partner_keys: list[str] = field(default_factory=list)
    poison_wrongly_applied: list[str] = field(default_factory=list)
    poison_not_in_inbox: list[str] = field(default_factory=list)
    duplicate_effects: dict[str, int] = field(default_factory=dict)  # job id -> times applied (>1)
    kills: int = 0
    victims: int = 0
    victims_not_rerun: list[str] = field(default_factory=list)
    partner_replays: int = 0  # partner saw an idempotency key again and refused to apply twice
    partner_lost_responses: int = 0  # partner applied the effect, then answered with an error
    poison_retry_attempts_ok: bool = True


def _drain(engine: Engine, deadline: float) -> list[str]:
    """Wait until no job is queued/running. Returns ids still unfinished at the deadline."""
    while True:
        with engine.connect() as conn:
            left = [
                str(r.id)
                for r in conn.execute(
                    text("SELECT id FROM jobs WHERE status IN ('queued','running')")
                )
            ]
        if not left or time.monotonic() > deadline:
            return left
        time.sleep(0.5)


def run_scenario(
    engine: Engine,
    tmp: Path,
    *,
    partner_env: dict[str, str],
    n_jobs: int,
    n_workers: int,
    kill_interval: tuple[float, float],
    poison_every: int,
    timeout: float,
) -> Report:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE jobs"))
    report = Report()
    deadline = time.monotonic() + timeout

    with running_cluster(tmp, partner_env, n_workers) as cluster:
        chaos = Chaos(cluster, engine, kill_interval)
        chaos.start()
        api = httpx.Client(base_url=cluster.api_url, timeout=30)

        # ---- 1. submit everything; every submission is sent TWICE, concurrently (client retry)
        poison_idx = {i for i in range(n_jobs) if poison_every and i % poison_every == 0}
        future_time = (datetime.now(UTC) + timedelta(seconds=3)).isoformat()

        def post(i: int) -> tuple[int, int, str]:
            body: dict[str, Any] = {
                "name": "call_partner",
                "payload": {"n": i, "mock": "always_fail"} if i in poison_idx else {"n": i},
                "priority": (i * 7) % 11
                - 5,  # fixed per job: a retry must send the identical request
                "idempotency_key": f"scenario-{i}",
            }
            if i % 20 == 0:
                body["run_at"] = future_time
            r = api.post("/jobs", json=body)
            return i, r.status_code, str(r.json().get("id"))

        with ThreadPoolExecutor(max_workers=8) as pool:
            tasks = list(range(n_jobs)) * 2
            random.shuffle(tasks)
            results = list(pool.map(post, tasks))
        by_key: dict[int, list[tuple[int, str]]] = {}
        for i, code, job_id in results:
            by_key.setdefault(i, []).append((code, job_id))
        report.distinct_keys = len(by_key)
        for i, pair in by_key.items():
            codes = sorted(c for c, _ in pair)
            if codes != [200, 201] or len({j for _, j in pair}) != 1:
                report.resubmits_not_deduplicated.append(f"scenario-{i}: {pair}")
        ids_by_idx = {i: pair[0][1] for i, pair in by_key.items()}
        poison_ids = {ids_by_idx[i] for i in poison_idx if i in ids_by_idx}

        # ---- 2. let the workers drain the queue while workers keep getting killed; humans
        #         retry jobs that exhausted their budget by bad luck (never the poison ones)
        report.stuck = _drain(engine, deadline)
        for _ in range(5):
            if report.stuck:
                break
            failed = api.get("/jobs", params={"status": "failed", "limit": 500}).json()
            todo = [j["id"] for j in failed if j["id"] not in poison_ids]
            if not todo:
                break
            for job_id in todo:
                api.post(f"/jobs/{job_id}/retry").raise_for_status()
                report.human_retried += 1
            report.stuck = _drain(engine, deadline)

        chaos.stop()
        report.kills, report.victims = chaos.kills, len(chaos.victims)

        # ---- 3. the poison jobs must be sitting in the failed inbox; a human retry works
        inbox = {
            j["id"]: j for j in api.get("/jobs", params={"status": "failed", "limit": 500}).json()
        }
        report.poison_not_in_inbox = sorted(poison_ids - inbox.keys())
        if poison_ids and not report.stuck:
            one = sorted(poison_ids)[0]
            before = api.get(f"/jobs/{one}").json()["attempts"]
            api.post(f"/jobs/{one}/retry").raise_for_status()
            report.stuck += _drain(engine, deadline)
            after = api.get(f"/jobs/{one}").json()
            report.poison_retry_attempts_ok = (
                after["status"] == "failed" and after["attempts"] == before + 6
            )

        # ---- 4. compare the queue's claims with what the partner actually received
        effects: dict[str, int] = httpx.get(f"{cluster.partner_url}/admin/effects").json()
        received: list[dict[str, Any]] = httpx.get(f"{cluster.partner_url}/admin/received").json()
        outcomes = Counter(r["outcome"] for r in received)
        report.partner_replays = outcomes["replayed"]
        report.partner_lost_responses = outcomes["applied_but_error"]

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id, status, attempts FROM jobs")).all()
        report.rows_in_db = len(rows)
        known_keys = {f"job-{r.id}" for r in rows}
        seen_keys = set(effects) | {str(r["key"]) for r in received}
        report.unknown_partner_keys = sorted(seen_keys - known_keys)
        for r in rows:
            job_id = str(r.id)
            applied = effects.get(f"job-{job_id}", 0)
            if r.status == "succeeded":
                report.succeeded += 1
                if applied < 1:
                    report.succeeded_without_effect.append(job_id)
            elif r.status == "failed":
                report.failed_for_human += 1
            if applied > r.attempts:
                report.effects_exceed_attempts.append(job_id)
            if applied > 1:
                report.duplicate_effects[job_id] = applied
            if job_id in poison_ids and applied:
                report.poison_wrongly_applied.append(job_id)
            if job_id in chaos.victims and r.attempts <= chaos.victims[job_id]:
                report.victims_not_rerun.append(job_id)
        api.close()
    return report


def assert_guarantees(report: Report, *, n_jobs: int, n_poison: int, dedupe: bool) -> None:
    """The claims of DESIGN.md, checked against what the partner really received."""
    # -- a client retrying a submission never creates a second job
    assert report.resubmits_not_deduplicated == []
    assert report.distinct_keys == n_jobs == report.rows_in_db

    # -- no job is lost: everything reached a terminal state, nothing is stuck
    assert report.stuck == [], f"{len(report.stuck)} jobs never finished"
    assert report.succeeded + report.failed_for_human == n_jobs
    # -- jobs that never succeed are kept for a human (exactly the poison ones), and retry works
    assert report.failed_for_human == n_poison
    assert report.poison_not_in_inbox == []
    assert report.poison_retry_attempts_ok

    # -- success means the partner really got it; nothing it got is unaccounted for
    assert report.succeeded_without_effect == []
    assert report.unknown_partner_keys == []
    assert report.poison_wrongly_applied == []
    # -- delivery guarantee: an effect is repeated only by another attempt, never out of thin air
    assert report.effects_exceed_attempts == []

    # -- the test really hit the dangerous situations (a green run must not be vacuous)
    assert report.kills >= 3, "chaos did not kill enough workers"
    assert report.victims >= 1, "no worker was proven to die in the middle of a job"
    assert report.victims_not_rerun == [], "a job whose worker was killed was not attempted again"
    assert report.partner_lost_responses >= 1, "partner never applied-then-failed"

    if dedupe:
        # idempotency key + a partner that honours it = effectively exactly once
        assert report.duplicate_effects == {}
        assert report.partner_replays >= 1, "the idempotency key was never actually needed"
    else:
        # no help from the partner: duplicates DO happen. That is what 'at-least-once' means.
        assert report.duplicate_effects != {}, "expected at least one duplicate delivery"
