import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"  # terminal: gave up, kept for a human
STATUSES = (QUEUED, RUNNING, SUCCEEDED, FAILED)


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16), default=QUEUED)
    # A queued job may only be claimed once run_at has passed. Retries push it into the future.
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # attempts = number of claims so far. NEVER decreases: it is also the fencing token.
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True)
    request_hash: Mapped[str | None] = mapped_column(String(64))
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    locked_by: Mapped[str | None] = mapped_column(String(100))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('queued','running','succeeded','failed')", name="ck_jobs_status"
        ),
        # Claim query: best ready job first. Partial index = only queued rows, so it stays small.
        Index(
            "ix_jobs_ready",
            text("priority DESC"),
            "run_at",
            "created_at",
            postgresql_where=text("status = 'queued'"),
        ),
        # Reaper query: running jobs whose lease ran out.
        Index("ix_jobs_lease", "lease_expires_at", postgresql_where=text("status = 'running'")),
    )
