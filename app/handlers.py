"""Job types. To add one: write a function and add it to REGISTRY (rules for authors: DESIGN.md).

A handler may run MORE THAN ONCE for the same job (at-least-once delivery), so it must be
idempotent: pass `ctx.idempotency_key` to anything with side effects outside this system.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from app.config import Settings


class PermanentError(Exception):
    """Raise when retrying can never help (e.g. the partner rejected the request as invalid)."""


@dataclass(frozen=True)
class JobContext:
    job_id: uuid.UUID
    name: str
    attempt: int  # 1 on the first try
    payload: dict[str, Any]

    @property
    def idempotency_key(self) -> str:
        """Identical on every attempt of this job. Hand it to external systems."""
        return f"job-{self.job_id}"


Handler = Callable[[JobContext], dict[str, Any]]


@dataclass(frozen=True)
class HandlerSpec:
    fn: Handler
    max_attempts: int = 5  # retry budget, copied onto the job row at submit time
    # after this long the heartbeat stops and the lease is allowed to expire
    timeout_seconds: float = 120


def echo(ctx: JobContext) -> dict[str, Any]:
    """Trivial job type for demos and smoke tests."""
    return {"echo": ctx.payload, "attempt": ctx.attempt}


def call_partner(ctx: JobContext) -> dict[str, Any]:
    """Call the slow, flaky partner API once, with the job's idempotency key.

    Timeouts and connection errors raise httpx exceptions: transient, so the queue retries them.
    """
    settings = Settings.from_env()
    response = httpx.post(
        f"{settings.partner_url}/v1/send",
        json=ctx.payload,
        headers={"Idempotency-Key": ctx.idempotency_key},
        timeout=settings.partner_timeout_seconds,
    )
    if response.is_success:
        body: dict[str, Any] = response.json()
        return body
    # 408/429/5xx can pass on their own; any other 4xx means our request is wrong, so don't retry.
    if response.status_code in (408, 429) or response.status_code >= 500:
        raise RuntimeError(f"partner returned {response.status_code}")
    raise PermanentError(
        f"partner rejected the request: {response.status_code} {response.text[:200]}"
    )


REGISTRY: dict[str, HandlerSpec] = {
    "echo": HandlerSpec(echo),
    "call_partner": HandlerSpec(call_partner, max_attempts=6, timeout_seconds=60),
}
