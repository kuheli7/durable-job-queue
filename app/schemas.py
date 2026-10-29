import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobIn(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a typo like "priorty" must be an error, not ignored

    name: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = {}
    priority: int = Field(default=0, ge=-100, le=100)  # higher runs first
    run_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)

    @field_validator("run_at")
    @classmethod
    def assume_utc(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    status: str
    priority: int
    run_at: datetime
    attempts: int
    max_attempts: int
    payload: dict[str, Any]
    result: dict[str, Any] | None
    last_error: str | None
    idempotency_key: str | None
    created_at: datetime
    finished_at: datetime | None


class JobCounts(BaseModel):
    queued: int
    running: int
    succeeded: int
    failed: int


class StatsOut(BaseModel):
    health: str  # "ok" or "degraded"
    reasons: list[str]
    jobs: JobCounts
    ready_now: int
    scheduled_for_later: int
    waiting_for_retry: int
    expired_leases: int
    oldest_ready_age_seconds: float | None
    succeeded_last_minute: int
    failed_last_minute: int
