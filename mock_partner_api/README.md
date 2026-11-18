# Mock partner API

An unreliable fake partner service for the job queue task. Your queue calls it from a job type. It fails in realistic ways, and it keeps an honest log of what it actually did so your chaos and soak tests can compare your delivery claims with reality.

## Run it

With Docker (add it as a service in your own `docker-compose.yml`):

```yaml
partner:
  build: ./mock_partner_api
  ports: ["9000:9000"]
  volumes: ["partner_data:/data"]
```

Without Docker:

```bash
pip install -r requirements.txt
uvicorn app:app --port 9000
```

Run exactly one instance.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/deliver` | The partner call. Body: `{"job_id": "...", "payload": {...}}` |
| `GET /audit` | Totals, outcome counts, and every job whose side effect ran more than once |
| `GET /audit/jobs/{job_id}` | How many times one job's side effect ran |
| `POST /admin/reset` | Clear all counters and the log (use between test runs) |
| `GET /health` | Liveness |

The partner supports an optional `Idempotency-Key` header.

## What `/v1/deliver` does

Each call rolls one outcome. Roughly:

| Outcome | Share | What the caller sees | Did the side effect run? |
|---|---|---|---|
| Normal | ~68% | `200` in 50-300 ms | yes |
| Slow | ~10% | `200` after 2-5 s | yes |
| Rate limited | ~8% | `429` with `Retry-After` | no |
| Error before work | ~5% | `500` | no |
| Error after work | ~4% | `500` | **yes** |
| Hang after work | ~5% | `200` after 30 s | **yes** |

A failed or slow reply does not tell you whether the work happened.

## Settings

Environment variables: `MOCK_SEED` (repeatable rolls), `MOCK_P_RATE_LIMIT`, `MOCK_P_ERROR_BEFORE`, `MOCK_P_ERROR_AFTER`, `MOCK_P_HANG_AFTER`, `MOCK_P_SLOW` (probabilities, 0 to 1), `MOCK_HANG_SECONDS`, `MOCK_EFFECTS_FILE`.
