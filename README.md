# Flash Sale System

Spec: [`doc/SPEC.md`](doc/SPEC.md) · Plan: [`doc/PLAN.md`](doc/PLAN.md)

## Prerequisites

- Node 24, pnpm
- Rootless **Podman** 5 (Fedora default) with its API socket enabled:
  `systemctl --user enable --now podman.socket`
- The `docker` CLI with the Compose v2+ plugin. It talks to Podman via `DOCKER_HOST`.
  `podman-compose` is not supported (no `up --wait`).

The `test:int`, `stack:*` and `stress` scripts source `.envrc`, which points `DOCKER_HOST` at the
rootless Podman socket unless `DOCKER_HOST` is already set. Run `source .envrc` (or `direnv allow`)
to use plain `docker` / `docker compose` commands in your shell.

## Run

```bash
pnpm i && pnpm build && pnpm test && pnpm test:int
pnpm stack:up        # http://localhost:8080, Floci console at http://localhost:4566/_floci/ui
pnpm e2e
pnpm stress -- spike
pnpm stack:down
```

The compose stack defaults the mock payment and shipping failure rates to `0`, so `e2e` and the
stress scenarios are deterministic. To exercise the compensating saga locally, recreate the stack
with a failure rate: `pnpm stack:down && PAYMENT_FAILURE_RATE=1 pnpm stack:up` (then a purchase ends
`CANCELLED`, its unit returns to stock, and the user can buy again).
