"""CHAOS TEST: real processes, workers are SIGKILLed while they hold a job.

The partner is the mentor's mock API. Its failure rates are turned up (through its own settings)
so that a short run reliably hits the dangerous case: the partner did the work, but the caller
never saw a success.
"""

from pathlib import Path

from sqlalchemy import Engine

from tests.harness import assert_guarantees, run_scenario


def test_killing_workers_mid_job_loses_nothing(engine: Engine, tmp_path: Path) -> None:
    n_jobs, poison_every = 30, 10
    report = run_scenario(
        engine,
        tmp_path,
        partner_env={
            "MOCK_SEED": "7",
            "MOCK_P_ERROR_BEFORE": "0.10",
            "MOCK_P_ERROR_AFTER": "0.25",
            "MOCK_P_HANG_AFTER": "0.05",
            "MOCK_P_SLOW": "0.10",
        },
        n_jobs=n_jobs,
        n_workers=4,
        kill_interval=(0.6, 1.2),
        poison_every=poison_every,
        timeout=240,
    )
    print(report)
    assert_guarantees(report, n_jobs=n_jobs, n_poison=len(range(0, n_jobs, poison_every)))
