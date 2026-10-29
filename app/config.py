"""All tunables, read from environment variables (so docker compose and tests can change them)."""

import os
from dataclasses import dataclass

DEFAULT_DATABASE_URL = "postgresql+psycopg://jobs:jobs@localhost:5433/jobs"


@dataclass(frozen=True)
class Settings:
    database_url: str
    lease_seconds: float  # how long a claim is valid without a heartbeat
    heartbeat_seconds: float  # how often a running worker renews its lease
    poll_seconds: float  # how long an idle worker sleeps between claim attempts
    backoff_base_seconds: float  # first retry delay ceiling
    backoff_cap_seconds: float  # no retry delay ceiling is larger than this
    partner_url: str
    partner_timeout_seconds: float
    lag_threshold_seconds: float  # /stats says "degraded" if a ready job waits longer than this

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ
        return cls(
            database_url=env.get("DATABASE_URL", DEFAULT_DATABASE_URL),
            lease_seconds=float(env.get("LEASE_SECONDS", "30")),
            heartbeat_seconds=float(env.get("HEARTBEAT_SECONDS", "10")),
            poll_seconds=float(env.get("POLL_SECONDS", "0.5")),
            backoff_base_seconds=float(env.get("BACKOFF_BASE_SECONDS", "5")),
            backoff_cap_seconds=float(env.get("BACKOFF_CAP_SECONDS", "300")),
            partner_url=env.get("PARTNER_API_URL", "http://localhost:9000"),
            partner_timeout_seconds=float(env.get("PARTNER_TIMEOUT_SECONDS", "10")),
            lag_threshold_seconds=float(env.get("LAG_THRESHOLD_SECONDS", "30")),
        )
