# Limitations (what this does not do, on purpose or not yet)

1. **Postgres is a single point of failure and of durability.** If its disk is lost, jobs are lost.
   Replication/backups are out of scope. "No job lost" means: no job is lost by *this system's*
   crashes, worker crashes or retries.
2. **Duplicate side effects are possible if the partner does not honour the idempotency key.**
   Handlers run at-least-once. The chaos test runs with the partner's de-duplication off and shows
   2-3 effects for some jobs; with it on there are none. Nothing in a queue can fix that.
3. **Crashes use up attempts.** A job whose worker keeps dying (or that keeps exceeding the lease)
   goes to `failed` after `max_attempts`, even if the handler never reported an error. Good for
   poison jobs; under sustained crashes, innocent jobs can land in the human inbox too (never lost).
4. **A wedged handler thread cannot be killed.** After the handler's timeout we stop renewing the
   lease so another worker takes the job, but the stuck thread keeps running until it returns.
   (Its late result is discarded by fencing.) Truly forcing a stop needs one process per job.
5. **Strict priority can starve** low-priority jobs under a constant flood of high ones.
6. **Polling.** Pickup latency is up to ~0.5-1 s and idle workers query Postgres constantly.
   LISTEN/NOTIFY would improve it; polling stays as the safe fallback.
7. **`/stats` scans the table.** Fine for thousands to low millions of rows; at larger sizes it needs
   counters or a retention policy.
8. **No retention.** Succeeded jobs are kept forever. There is no cleanup job.
9. **Thin history.** Only the latest error is kept (`last_error`), not one row per attempt.
10. **No authentication, rate limiting or per-type concurrency limits.** Assumes a trusted network.
    Many workers can hit a rate-limited partner at once.
11. **No cancel** for queued jobs, and `max_attempts` is a per-type setting, not per submission.
12. **Unverified assumptions about the brief:** status names, field names, the `/stats` shape and the
    real partner's contract (my partner is a stand-in I wrote). See DESIGN.md section 6.
13. **`now()` is frozen per transaction.** Fine here because every claim/update runs in its own short
    transaction, but a long-lived transaction would see stale time.
