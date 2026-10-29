"""SOAK TEST: thousands of jobs, several workers, random kills. Same checks as the chaos test.

Not part of the default run (it takes a few minutes):  pytest -m soak
Size is configurable:  SOAK_JOBS=500 SOAK_WORKERS=4 pytest -m soak
"""

import os
from pathlib import Path

import pytest
from sqlalchemy import Engine

from tests.harness import assert_guarantees, run_scenario


@pytest.mark.soak
def test_soak(engine: Engine, tmp_path: Path) -> None:
    n_jobs = int(os.environ.get("SOAK_JOBS", "2000"))
    poison_every = 100
    report = run_scenario(
        engine,
        tmp_path,
        partner_env={
            "LATENCY_MIN": "0.02",
            "LATENCY_MAX": "0.1",
            "FAIL_RATE": "0.2",
            "LOST_RESPONSE_RATE": "0.05",
            "DEDUPE": "1",
        },
        n_jobs=n_jobs,
        n_workers=int(os.environ.get("SOAK_WORKERS", "6")),
        kill_interval=(0.5, 1.5),
        poison_every=poison_every,
        timeout=900,
    )
    print(report)
    assert_guarantees(
        report, n_jobs=n_jobs, n_poison=len(range(0, n_jobs, poison_every)), dedupe=True
    )
