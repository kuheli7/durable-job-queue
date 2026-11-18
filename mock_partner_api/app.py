"""Mock partner API for the durable job queue task.

A deliberately unreliable HTTP service. Every call to POST /v1/deliver rolls a
random outcome (slow, rate limited, 500, hang, or fine) and, when the call
reaches the "do the work" step, records one side effect in an append-only log.
GET /audit reports how many times each job's side effect really ran, which is
the ground truth a queue's delivery claims are checked against.

Run it as ONE process (the counters live in this process, backed by a log file).
"""

import asyncio
import json
import os
import random
import threading
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

EFFECTS_FILE = Path(os.getenv("MOCK_EFFECTS_FILE", "effects.jsonl"))
HANG_SECONDS = float(os.getenv("MOCK_HANG_SECONDS", "30"))
RETRY_AFTER_RANGE = (1, 3)

# Probability of each failure mode. Whatever is left over is a normal call.
P_RATE_LIMIT = float(os.getenv("MOCK_P_RATE_LIMIT", "0.08"))  # 429, no effect
P_ERROR_BEFORE = float(os.getenv("MOCK_P_ERROR_BEFORE", "0.05"))  # 500, no effect
P_ERROR_AFTER = float(os.getenv("MOCK_P_ERROR_AFTER", "0.04"))  # 500, effect ran
P_HANG_AFTER = float(os.getenv("MOCK_P_HANG_AFTER", "0.05"))  # effect ran, reply very late
P_SLOW = float(os.getenv("MOCK_P_SLOW", "0.10"))  # 2-5s, then fine

_seed = os.getenv("MOCK_SEED")
rng = random.Random(int(_seed)) if _seed else random.Random()

app = FastAPI(title="Mock partner API")

_lock = threading.Lock()
effects_by_job: Counter[str] = Counter()
effect_by_key: dict[str, str] = {}
outcomes: Counter[str] = Counter()
state: dict[str, int] = {"requests": 0, "deduplicated": 0}


class DeliverRequest(BaseModel):
    job_id: str
    payload: dict[str, Any] | None = None


def _load_effects() -> None:
    if not EFFECTS_FILE.exists():
        return
    for line in EFFECTS_FILE.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        effects_by_job[row["job_id"]] += 1
        if row.get("idempotency_key"):
            effect_by_key[row["idempotency_key"]] = row["effect_id"]


def _record_effect(job_id: str, idempotency_key: str | None) -> tuple[str, bool]:
    """Run the side effect once per Idempotency-Key. Returns (effect_id, deduplicated)."""
    with _lock:
        if idempotency_key and idempotency_key in effect_by_key:
            state["deduplicated"] += 1
            return effect_by_key[idempotency_key], True
        effect_id = uuid.uuid4().hex
        row = {
            "effect_id": effect_id,
            "job_id": job_id,
            "idempotency_key": idempotency_key,
            "at": time.time(),
        }
        with EFFECTS_FILE.open("a") as handle:
            handle.write(json.dumps(row) + "\n")
            handle.flush()
        effects_by_job[job_id] += 1
        if idempotency_key:
            effect_by_key[idempotency_key] = effect_id
        return effect_id, False


def _roll() -> str:
    roll = rng.random()
    for name, probability in (
        ("rate_limited", P_RATE_LIMIT),
        ("error_before", P_ERROR_BEFORE),
        ("error_after", P_ERROR_AFTER),
        ("hang_after", P_HANG_AFTER),
        ("slow", P_SLOW),
    ):
        if roll < probability:
            return name
        roll -= probability
    return "ok"


@app.on_event("startup")
def startup() -> None:
    _load_effects()


@app.post("/v1/deliver")
async def deliver(
    body: DeliverRequest,
    idempotency_key: str | None = Header(default=None),
) -> JSONResponse:
    outcome = _roll()
    with _lock:
        state["requests"] += 1
        outcomes[outcome] += 1

    if outcome == "rate_limited":
        retry_after = rng.randint(*RETRY_AFTER_RANGE)
        return JSONResponse(
            {"error": "rate limited"},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )
    if outcome == "error_before":
        return JSONResponse({"error": "partner failure"}, status_code=500)

    latency = rng.uniform(2.0, 5.0) if outcome == "slow" else rng.uniform(0.05, 0.3)
    await asyncio.sleep(latency / 2)
    effect_id, deduplicated = _record_effect(body.job_id, idempotency_key)
    await asyncio.sleep(latency / 2)

    if outcome == "error_after":
        return JSONResponse({"error": "partner failure"}, status_code=500)
    if outcome == "hang_after":
        await asyncio.sleep(HANG_SECONDS)
    return JSONResponse(
        {"status": "ok", "effect_id": effect_id, "deduplicated": deduplicated}
    )


@app.get("/audit")
def audit() -> dict[str, Any]:
    with _lock:
        duplicated = {job: n for job, n in effects_by_job.items() if n > 1}
        return {
            "requests": state["requests"],
            "outcomes": dict(outcomes),
            "distinct_jobs": len(effects_by_job),
            "total_effects": sum(effects_by_job.values()),
            "deduplicated_by_key": state["deduplicated"],
            "duplicated_jobs": duplicated,
        }


@app.get("/audit/jobs/{job_id}")
def audit_job(job_id: str) -> dict[str, Any]:
    with _lock:
        return {"job_id": job_id, "effects": effects_by_job.get(job_id, 0)}


@app.post("/admin/reset")
def reset() -> dict[str, str]:
    with _lock:
        effects_by_job.clear()
        effect_by_key.clear()
        outcomes.clear()
        state["requests"] = 0
        state["deduplicated"] = 0
        EFFECTS_FILE.write_text("")
    return {"status": "reset"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
