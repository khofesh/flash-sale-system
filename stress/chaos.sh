#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
. ./.envrc

scenario="${1:-spike}"
kill_after="${CHAOS_KILL_AFTER_S:-0.5}"
compose=(docker compose -f infra/docker-compose.yml)

api_id="$("${compose[@]}" ps -q api | head -n1)"
orchestrator_id="$("${compose[@]}" ps -q orchestrator)"
if [[ -z "$api_id" || -z "$orchestrator_id" ]]; then
  echo "chaos: the stack is not running (run pnpm stack:up first)" >&2
  exit 1
fi

echo "chaos: running $scenario"
pnpm stress -- "$scenario" &
stress_pid=$!

# Wait for the k6 container so the kill lands mid-run, not before load starts.
for _ in $(seq 1 300); do
  if docker ps -q --filter ancestor=grafana/k6:0.57.0 | grep -q .; then
    break
  fi
  sleep 0.1
done

echo "chaos: killing one api replica and the orchestrator after ${kill_after}s of load"
sleep "$kill_after"
docker kill "$api_id" "$orchestrator_id" >/dev/null
sleep 1

# Restarting models a crashed replica/orchestrator coming back: the pending
# stream entries are reclaimed by XAUTOCLAIM and SQS redelivers unacked messages.
echo "chaos: restarting the killed containers"
docker start "$api_id" "$orchestrator_id" >/dev/null

status=0
wait "$stress_pid" || status=$?

if [[ $status -ne 0 ]]; then
  # The spike's exact-count thresholds cannot hold while requests are killed
  # mid-flight. For chaos the pass criterion is the invariant in verify.ts.
  echo "chaos: k6 exited $status (expected under chaos; verification decides)"
fi

echo "chaos: re-running verification"
pnpm exec tsx stress/verify.ts
echo "chaos: PASS — every reservation was eventually persisted with no oversell"
