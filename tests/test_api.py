"""HTTP contract tests (POST /jobs, GET /jobs/{id}, GET /stats + the extras)."""

import uuid
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.api import app


@pytest.fixture()
def client(session_factory: sessionmaker[Session]) -> Iterator[TestClient]:
    # session_factory (re)creates the schema and empties the table before each test
    with TestClient(app) as c:
        yield c


def test_submit_then_read_back(client: TestClient) -> None:
    r = client.post("/jobs", json={"name": "echo", "payload": {"x": 1}, "priority": 3})
    assert r.status_code == 201
    job = r.json()
    assert job["status"] == "queued" and job["priority"] == 3 and job["attempts"] == 0
    assert job["payload"] == {"x": 1} and job["result"] is None

    got = client.get(f"/jobs/{job['id']}")
    assert got.status_code == 200 and got.json()["id"] == job["id"]


def test_validation(client: TestClient) -> None:
    assert client.post("/jobs", json={"name": "nope"}).status_code == 422  # unknown job type
    assert client.post("/jobs", json={"name": "echo", "priority": 101}).status_code == 422
    assert client.post("/jobs", json={"name": "echo", "priorty": 1}).status_code == 422  # typo
    assert client.post("/jobs", json={"payload": {}}).status_code == 422  # no name
    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404
    assert client.get("/jobs/not-a-uuid").status_code == 422


def test_run_at_without_timezone_is_read_as_utc(client: TestClient) -> None:
    r = client.post("/jobs", json={"name": "echo", "run_at": "2031-01-01T10:00:00"})
    assert r.status_code == 201 and r.json()["run_at"].startswith("2031-01-01T10:00:00")


def test_idempotency_key_in_header(client: TestClient) -> None:
    body = {"name": "echo", "payload": {"a": 1}}
    first = client.post("/jobs", json=body, headers={"Idempotency-Key": "abc"})
    retry = client.post("/jobs", json=body, headers={"Idempotency-Key": "abc"})
    assert (first.status_code, retry.status_code) == (201, 200)
    assert first.json()["id"] == retry.json()["id"]
    assert client.get("/jobs").json().__len__() == 1


def test_idempotency_key_in_body(client: TestClient) -> None:
    body = {"name": "echo", "idempotency_key": "k-body"}
    a, b = client.post("/jobs", json=body), client.post("/jobs", json=body)
    assert (a.status_code, b.status_code) == (201, 200) and a.json()["id"] == b.json()["id"]


def test_reusing_a_key_for_a_different_request_is_409(client: TestClient) -> None:
    h = {"Idempotency-Key": "same"}
    assert (
        client.post("/jobs", json={"name": "echo", "payload": {"a": 1}}, headers=h).status_code
        == 201
    )
    assert (
        client.post("/jobs", json={"name": "echo", "payload": {"a": 2}}, headers=h).status_code
        == 409
    )


def test_header_and_body_key_must_agree(client: TestClient) -> None:
    r = client.post(
        "/jobs", json={"name": "echo", "idempotency_key": "one"}, headers={"Idempotency-Key": "two"}
    )
    assert r.status_code == 400


def test_failed_jobs_can_be_listed_and_retried(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    job_id = client.post("/jobs", json={"name": "echo"}).json()["id"]
    assert client.post(f"/jobs/{job_id}/retry").status_code == 409  # not failed yet

    with session_factory() as s:
        s.execute(
            text(
                "UPDATE jobs SET status='failed', attempts=5, last_error='partner down' WHERE id=:i"
            ),
            {"i": job_id},
        )
        s.commit()

    inbox = client.get("/jobs", params={"status": "failed"}).json()
    assert [j["id"] for j in inbox] == [job_id] and inbox[0]["last_error"] == "partner down"

    retried = client.post(f"/jobs/{job_id}/retry")
    assert retried.status_code == 200 and retried.json()["status"] == "queued"
    assert retried.json()["max_attempts"] == 10  # attempts (5) + echo's budget (5)
    assert client.get(f"/jobs/{job_id}").json()["status"] == "queued"  # really changed in the DB
    assert client.post(f"/jobs/{uuid.uuid4()}/retry").status_code == 404


def test_stats(client: TestClient) -> None:
    client.post("/jobs", json={"name": "echo"})
    s = client.get("/stats")
    assert s.status_code == 200
    body = s.json()
    assert body["jobs"] == {"queued": 1, "running": 0, "succeeded": 0, "failed": 0}
    assert body["health"] == "ok" and body["ready_now"] == 1
