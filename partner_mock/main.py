"""Stand-in for the mentor's "partner API": slow, fails randomly, RECORDS everything it receives.

The tests compare what the queue claims with this record (GET /admin/received, /admin/effects).

Behaviour (all configurable by environment variable):
  * every request takes LATENCY_MIN..LATENCY_MAX seconds
  * FAIL_RATE of requests fail BEFORE doing anything (503/500/429)
  * LOST_RESPONSE_RATE of requests are APPLIED and then answered with 500: the caller thinks it
    failed but the effect happened. This is the window at-least-once delivery is all about.
  * DEDUPE=1: a repeated Idempotency-Key is not applied twice (the repeat replays the first answer).
    DEDUPE=0: the partner has no idempotency support, so a repeated call applies the effect again.
  * a body containing {"mock": "always_fail"} always fails before doing anything (a poison job)
"""

import asyncio
import os
import random
import time
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Partner API mock")

LATENCY_MIN = float(os.environ.get("LATENCY_MIN", "0.5"))
LATENCY_MAX = float(os.environ.get("LATENCY_MAX", "3"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.25"))
LOST_RESPONSE_RATE = float(os.environ.get("LOST_RESPONSE_RATE", "0.05"))
DEDUPE = os.environ.get("DEDUPE", "1") == "1"

rng = random.Random(int(os.environ.get("SEED", "1")))

# Single event loop => these are only touched between awaits, so no locks are needed.
received: list[dict[str, Any]] = []  # one entry per request that reached us, with its outcome
effects: dict[str, int] = {}  # idempotency key -> how many times the effect was applied
answers: dict[str, dict[str, Any]] = {}  # idempotency key -> the answer we gave when applying


def _record(key: str | None, outcome: str) -> None:
    received.append({"seq": len(received) + 1, "key": key, "outcome": outcome, "at": time.time()})


@app.post("/v1/send")
async def send(
    body: dict[str, Any], idempotency_key: Annotated[str | None, Header()] = None
) -> JSONResponse:
    await asyncio.sleep(rng.uniform(LATENCY_MIN, LATENCY_MAX))

    if body.get("mock") == "always_fail":
        _record(idempotency_key, "failed_before_apply")
        raise HTTPException(500, "partner exploded")

    if DEDUPE and idempotency_key in answers:
        _record(idempotency_key, "replayed")
        return JSONResponse(answers[idempotency_key])

    if rng.random() < FAIL_RATE:
        _record(idempotency_key, "failed_before_apply")
        raise HTTPException(rng.choice([500, 503, 429]), "partner is having a bad day")

    # --- the effect happens here ---
    count = effects.get(idempotency_key or "", 0) + 1
    effects[idempotency_key or ""] = count
    answer = {"status": "accepted", "message_id": f"msg-{idempotency_key}-{count}"}
    if idempotency_key is not None:
        answers[idempotency_key] = answer

    if rng.random() < LOST_RESPONSE_RATE:
        _record(idempotency_key, "applied_but_error")
        raise HTTPException(500, "partner applied the request but crashed before answering")
    _record(idempotency_key, "applied")
    return JSONResponse(answer)


@app.get("/admin/received")
async def get_received() -> list[dict[str, Any]]:
    return received


@app.get("/admin/effects")
async def get_effects() -> dict[str, int]:
    return effects


@app.post("/admin/reset")
async def reset() -> dict[str, str]:
    received.clear()
    effects.clear()
    answers.clear()
    return {"status": "cleared"}
