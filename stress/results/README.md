# Stress results

`pnpm stress <scenario>` writes three files per run, named by the UTC start time:

- `<stamp>-<scenario>-k6.json` — the k6 summary export: throughput, latency
  percentiles, failed-request rate and the custom counters.
- `<stamp>-<scenario>-verify.txt` — `stress/verify.ts` output: the SPEC §5.3
  invariant, live-order and duplicate-user checks, and the DLQ depth.
- `<stamp>-<scenario>-meta.json` — the machine that produced the run (CPU,
  memory, platform), because absolute numbers depend on the host.
- `<stamp>-soak-samples.json` — soak only: per-replica memory and stream lag,
  scraped through nginx every 15 s (`X-Upstream` identifies the replica).

Correctness checks (oversell = 0, one live order per user, invariant holds,
DLQs empty) never depend on the hardware. Throughput and p99 thresholds are the
SPEC §9.1 targets; on a smaller machine run the same scenario with
`MIN_RPS=... MAX_P99_MS=...` and record the lower number alongside the results.

The spike is paced, so its throughput is the offered `RATE`, not the stack's
capacity. Raise it to look for the limit: `RATE=10000 MIN_RPS=9000 pnpm stress spike`.
After sell-out most requests are answered from the sold-out flag without touching
Redis, so this number says little about reserve throughput on its own.

`pnpm stress` recreates nginx with a higher per-IP limit for the run, because all of
k6 arrives from one address, warms it up, and restores the default limit afterwards.

The soak fails if p99 late in the run exceeds twice the early p99 (with a 5 ms floor),
or if a replica's used-heap low point grows more than 25%. RSS is printed but not
judged: V8 grows it in steps and rarely returns it, so it cannot separate a larger
heap from a leak. `DURATION=12m pnpm stress soak` runs longer; the sale window closes
after 15 minutes. `/metrics` also exposes `reservation_stream_lag_entries`,
`reservation_stream_pending_entries` and `dlq_depth`, refreshed at most every 5 s.
