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
pnpm stress -- smoke
pnpm stack:down
```
