"""call_partner against canned partner replies (no network, no database)."""

import uuid
from typing import Any

import httpx
import pytest

from app.handlers import JobContext, PermanentError, call_partner


def ctx() -> JobContext:
    return JobContext(job_id=uuid.uuid4(), name="call_partner", attempt=1, payload={"to": "me"})


def reply(monkeypatch: pytest.MonkeyPatch, status: int) -> list[dict[str, Any]]:
    """Make httpx.post answer with `status`; returns the list the calls are recorded in."""
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> httpx.Response:
        calls.append({"url": url, **kwargs})
        return httpx.Response(status, json={"status": "ok"}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    return calls


def test_success_sends_job_id_payload_and_idempotency_key(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = reply(monkeypatch, 200)
    c = ctx()
    assert call_partner(c) == {"status": "ok"}
    (call,) = calls
    assert call["url"].endswith("/v1/deliver")
    assert call["json"] == {"job_id": str(c.job_id), "payload": {"to": "me"}}
    assert call["headers"] == {"Idempotency-Key": f"job-{c.job_id}"}


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_transient_partner_errors_are_retried(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    reply(monkeypatch, status)
    with pytest.raises(RuntimeError):
        call_partner(ctx())


@pytest.mark.parametrize("status", [400, 404, 422])
def test_other_client_errors_are_permanent(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    reply(monkeypatch, status)
    with pytest.raises(PermanentError):
        call_partner(ctx())
