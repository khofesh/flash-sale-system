#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
. ./.envrc

scenario="${1:-spike}"
kill_after="${CHAOS_KILL_AFTER_S:-0.5}"
compose=(docker compose -f infra/docker-compose.yml)
k6_image=grafana/k6:0.57.0

k6_running() {
  docker ps -q --filter "ancestor=$k6_image" | grep -q .
}

api_id="$("${compose[@]}" ps -q api | head -n1)"
# A read loop, not mapfile: macOS ships bash 3.2.
orchestrator_ids=()
while IFS= read -r id; do
  [[ -n "$id" ]] && orchestrator_ids+=("$id")
done < <("${compose[@]}" ps -q orchestrator)
if [[ -z "$api_id" || ${#orchestrator_ids[@]} -eq 0 ]]; then
  echo "chaos: the stack is not running (run pnpm stack:up first)" >&2
  exit 1
fi

echo "chaos: running $scenario"
pnpm stress -- "$scenario" &
stress_pid=$!

# Wait for the k6 container so the kill lands mid-run, not before load starts.
for _ in $(seq 1 300); do
  if k6_running; then
    break
  fi
  sleep 0.1
done

echo "chaos: killing one api replica and every orchestrator after ${kill_after}s of load"
sleep "$kill_after"
if ! k6_running; then
  echo "chaos: FAIL — k6 was not running at the kill, so the run proves nothing" >&2
  wait "$stress_pid" || true
  exit 1
fi
docker kill "$api_id" "${orchestrator_ids[@]}" >/dev/null
sleep 1

# Restarting models a crashed replica/orchestrator coming back: the pending
# stream entries are reclaimed by XAUTOCLAIM and SQS redelivers unacked messages.
# The restart policy may already have started them, which makes this a no-op.
echo "chaos: restarting the killed containers"
docker start "$api_id" "${orchestrator_ids[@]}" >/dev/null

status=0
wait "$stress_pid" || status=$?

if [[ $status -ne 0 ]]; then
  # The spike's exact-count thresholds cannot hold while requests are killed
  # mid-flight. For chaos the pass criterion is the verification below, which
  # also rejects a run in which no request got through.
  echo "chaos: stress exited $status (expected under chaos; verification decides)"
fi

echo "chaos: re-running verification"
pnpm exec tsx stress/verify.ts --require-orders
echo "chaos: PASS — every reservation was persisted and finished its saga, with no oversell"
