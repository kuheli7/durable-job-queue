"""CHAOS TEST: real processes, workers are SIGKILLed while they hold a job.

Run twice: against a partner that honours the Idempotency-Key and against one that does not.
"""

from pathlib import Path

import pytest
from sqlalchemy import Engine

from tests.harness import assert_guarantees, run_scenario


@pytest.mark.parametrize(
    "dedupe", [True, False], ids=["partner-dedupes", "partner-does-not-dedupe"]
)
def test_killing_workers_mid_job_loses_nothing(
    engine: Engine, tmp_path: Path, dedupe: bool
) -> None:
    n_jobs, poison_every = 30, 10
    report = run_scenario(
        engine,
        tmp_path,
        partner_env={
            "LATENCY_MIN": "0.4",
            "LATENCY_MAX": "0.9",
            "FAIL_RATE": "0.15",
            "LOST_RESPONSE_RATE": "0.3",
            "DEDUPE": "1" if dedupe else "0",
        },
        n_jobs=n_jobs,
        n_workers=4,
        kill_interval=(0.6, 1.2),
        poison_every=poison_every,
        timeout=180,
    )
    print(report)
    assert_guarantees(
        report, n_jobs=n_jobs, n_poison=len(range(0, n_jobs, poison_every)), dedupe=dedupe
    )
