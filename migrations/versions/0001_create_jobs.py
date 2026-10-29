"""create jobs table

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column(
            "run_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(200), unique=True),
        sa.Column("request_hash", sa.String(64)),
        sa.Column("result", postgresql.JSONB()),
        sa.Column("last_error", sa.Text()),
        sa.Column("locked_by", sa.String(100)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "status IN ('queued','running','succeeded','failed')", name="ck_jobs_status"
        ),
    )
    op.create_index(
        "ix_jobs_ready",
        "jobs",
        [sa.text("priority DESC"), "run_at", "created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "ix_jobs_lease",
        "jobs",
        ["lease_expires_at"],
        postgresql_where=sa.text("status = 'running'"),
    )


def downgrade() -> None:
    op.drop_table("jobs")
