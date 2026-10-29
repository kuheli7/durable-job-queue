import uuid
from collections.abc import Iterator
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from sqlalchemy.orm import Session

from app import queue
from app.config import Settings
from app.db import make_engine, make_session_factory
from app.handlers import REGISTRY
from app.models import Job
from app.schemas import JobIn, JobOut, StatsOut

settings = Settings.from_env()
session_factory = make_session_factory(make_engine(settings.database_url))

app = FastAPI(title="Job queue")


def get_session() -> Iterator[Session]:
    with session_factory() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


@app.post("/jobs", response_model=JobOut, status_code=201)
def submit_job(
    body: JobIn,
    response: Response,
    session: SessionDep,
    idempotency_key_header: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> Job:
    """201 = new job. 200 = this idempotency key was already used: here is the original job."""
    spec = REGISTRY.get(body.name)
    if spec is None:
        raise HTTPException(422, f"unknown job name {body.name!r}; known: {sorted(REGISTRY)}")

    if (
        idempotency_key_header is not None
        and body.idempotency_key is not None
        and idempotency_key_header != body.idempotency_key
    ):
        raise HTTPException(400, "Idempotency-Key header and idempotency_key field disagree")
    key = idempotency_key_header or body.idempotency_key

    # Identity of the request = only what the client actually sent (not defaults we fill in).
    sent = body.model_dump(mode="json", include=body.model_fields_set - {"idempotency_key"})
    try:
        job, created = queue.submit(
            session,
            name=body.name,
            payload=body.payload,
            priority=body.priority,
            run_at=body.run_at,
            max_attempts=spec.max_attempts,
            idempotency_key=key,
            request_hash=queue.fingerprint(sent),
        )
    except queue.IdempotencyConflict:
        raise HTTPException(
            409, "this idempotency key was already used with a different request"
        ) from None
    if not created:
        response.status_code = 200
    return job


@app.get("/jobs/{job_id}", response_model=JobOut)
def read_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    job = queue.get_job(session, job_id)
    if job is None:
        raise HTTPException(404, "no such job")
    return job


@app.get("/jobs", response_model=list[JobOut])
def list_jobs(
    session: SessionDep,
    status: Literal["queued", "running", "succeeded", "failed"] | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[Job]:
    """The human's inbox: GET /jobs?status=failed"""
    return queue.list_jobs(session, status, limit)


@app.post("/jobs/{job_id}/retry", response_model=JobOut)
def retry_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    """Put a `failed` job back in the queue with a fresh retry budget."""
    existing = queue.get_job(session, job_id)
    if existing is None:
        raise HTTPException(404, "no such job")
    spec = REGISTRY.get(existing.name)
    job = queue.retry_failed(session, job_id, spec.max_attempts if spec else 5)
    if job is None:
        raise HTTPException(409, f"job is {existing.status}; only failed jobs can be retried")
    return job


@app.get("/stats", response_model=StatsOut)
def read_stats(session: SessionDep) -> dict[str, object]:
    return queue.stats(session, settings.lag_threshold_seconds)
