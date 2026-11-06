# AGENTS.md

Guidance for coding agents working in this repository.

## Project

A flash sale system: one product, limited stock, a fixed time window, one item per user, and heavy concurrent load. Purchases are decided atomically in Redis. Payment and shipping run as an orchestrated saga over SQS.

- **Spec (source of truth):** [`doc/SPEC.md`](doc/SPEC.md). This has been accepted. Don't change behaviour without updating the spec.
- **Plan:** [`doc/PLAN.md`](doc/PLAN.md). Work proceeds phase by phase (0 → 6).

## Status

**PLAN Phases 0, 1, 2, 3 and 4 are complete.** The walking skeleton runs end-to-end via `docker compose`: web → nginx → API → Redis stream → orchestrator → Postgres → SQS (Floci) → payment → shipping → back. Phase 1 adds the `sales` table, env seeding, the admin reset endpoint, `GET /api/sale` (250 ms cache), and the web status banner with a server-clock countdown. Phase 2 adds the atomic `reserve.lua`/`release.lua` scripts, `POST /api/purchases` with `Idempotency-Key`, `GET /api/purchases/:userId`, the Redis → Postgres reservation consumer, the `orders`/`saga_events`/`processed_messages` schema, the buy UI, the concurrency integration tests, and the spike/same-user stress scenarios with `stress/verify.ts`. Phase 3 adds the orchestrated payment/shipping saga: `core/orderStateMachine.ts`, conditional order transitions, SQS `payment.requested`/`shipping.requested`/result messages, deterministic payment/shipping mocks with configurable failure rates, the payment-timeout sweeper, compensation through `release.lua`, `GET /admin/reconcile`, saga integration tests and the Floci broker contract test. Phase 4 adds `XAUTOCLAIM` reclaim and graceful shutdown, fail-closed 503 + `Retry-After` when Redis is down, Postgres retry with exponential backoff and jitter plus Redis status fallback, the `pnpm rebuild-redis` job, and `stress/chaos.sh`.

Next is **PLAN Phase 5** (throughput hardening). Don't start a later phase until the current phase's "Verify" block passes.

## Stack

- TypeScript and Node 24 LTS, in pnpm workspaces.
- API: Fastify. It is stateless and runs as N replicas behind nginx, which does round-robin and `limit_req`.
- Web: React with Vite.
- Redis 7 with AOF everysec. Redis decides purchases, and its stream (`XADD`) is the outbox.
- Postgres 16 is the system of record for sales, orders, `saga_events` and `processed_messages`.
- SQS via **Floci** (`floci/floci:2.4.0`, port 4566). We don't use LocalStack. ElasticMQ is the fallback.
- Containers: rootless **Podman**, driven through the `docker` CLI and Compose via `DOCKER_HOST` (see `.envrc`). Under SELinux, never use `:z` on socket mounts; use `security_opt: label=disable`.
- Tests: Vitest for unit tests, Testcontainers for integration tests, and k6 for stress tests (run through the `grafana/k6` image).

## Layout (target, per SPEC §11)

```
apps/{api,orchestrator,payment,shipping,web}
packages/{core,infra,config}
infra/{docker-compose.yml,nginx.conf,init-queues.ts,migrations/}
stress/          # k6 scripts + verify.ts
scripts/         # e2e.ts, chaos.sh, rebuild-redis
doc/             # SPEC.md, PLAN.md
```

- `packages/core` holds pure domain logic, such as the state machine and envelopes. It does no I/O.
- `packages/infra` holds the Redis, Postgres and MessageBus adapters.

## Commands (root `package.json`, once scaffolded)

| Command                             | Purpose                                           |
| ----------------------------------- | ------------------------------------------------- |
| `pnpm i`                            | Install                                           |
| `pnpm build` / `pnpm lint`          | Build / lint all workspaces                       |
| `pnpm test`                         | Unit tests, with no I/O                           |
| `pnpm test:int`                     | Integration tests (Testcontainers)                |
| `pnpm stack:up` / `pnpm stack:down` | `docker compose up -d --build --wait` / `down -v` |
| `pnpm e2e`                          | End-to-end script against the running stack       |
| `pnpm stress`                       | k6 run, followed by `stress/verify.ts`            |

Don't name the scripts `up` or `down`. pnpm already has commands with those names (aliases for `update` and `downgrade`), so the scripts would never run.

## Invariants (never break these)

- **No oversell.** `sold + available == initial_stock` must hold at all times. `stress/verify.ts` checks it after every stress run.
- **One active order per user per sale.** Two layers enforce this: `SISMEMBER` on the Redis buyers set, and the partial unique index on `orders(sale_id, user_id) WHERE status NOT IN ('CANCELLED','REFUNDED')`.
- **The reserve is a single atomic Lua script.** `reserve.lua` checks the window against Redis `TIME`, checks the buyers set and the stock, then runs DECR + SADD + XADD. Don't split it into multiple round trips.
- **Compensation is idempotent.** `release.lua` does HDEL + SREM + INCR only for the reservation in `sale:{id}:holders`. Running it twice, or for an older order after a re-purchase, must be a no-op. `orders.released_at` makes a pending release durable; the sweeper retries it.
- **Saga transitions are conditional UPDATEs**, written as `WHERE status = <expected>`. A transition that does not apply replans the event against the current status. Consumers dedupe messages through `processed_messages`, and result messages have derived ids (`<orderId>:<type>`).
- **Fail closed.** If Redis is unavailable, the purchase endpoint returns 503. It must never fall back to an unsafe path.
- Purchase responses: 202 RESERVED, 409 ALREADY_PURCHASED, 410 SOLD_OUT, 403 NOT_STARTED/ENDED, 400, 429, 503.

## Conventions

- Code depends on the `MessageBus` interface (`SqsBus`, `InMemoryBus`), never on the SQS SDK directly.
- Every queue has a DLQ with `maxReceiveCount` 5. Queues are created by the one-shot `queue-init` compose service, which is idempotent.
- Pin image tags and dependency versions exactly.
- If scope has to be cut, cut from the end of the PLAN. **Never cut the Phase 2 concurrency tests or `verify.ts`.**
- Only commit when the user asks. Don't commit on `main` without being told to.

## Context Retrieval Policy

Always retrieve the smallest amount of information necessary.
Escalate only when necessary. Stop escalating as soon as sufficient information has been obtained.

Preferred order:

1. Need code structure? `graph.json`.
2. Need symbols? `ast-grep`.
3. Need repository metadata? `rtk`.
4. Need implementation? Source files.
5. Use repository-wide search as last resort

Avoid reading entire directories or the whole repository unless explicitly requested.

## RTK (Rust Token Killer) - Token-Optimized CLI

`rtk` is a CLI proxy that filters and compresses command outputs, saving 60-90% tokens.

**Always** prefix commands with `rtk`. If RTK has a dedicated filter, it uses it. If not, it passes through unchanged. This means RTK is always safe to use.

Instead of:

```
ls -la .
git status
git log -10
docker ps
```

Use:

```
rtk ls -la .
rtk git status
rtk git log -10
rtk docker ps
```

### RTK commands example

```bash
rtk ls <path>
rtk read <file>
rtk find <pattern>

rtk err <cmd>           # Filter errors only from any command
rtk log <file>          # Deduplicated logs with counts
rtk json <file>         # JSON structure without values

rtk curl <url>          # Compact HTTP responses

rtk docker ps           # Compact container list
rtk docker images       # Compact image list
rtk docker logs <c>     # Deduplicated logs
```

## Graphify - Codebase context & knowledge graph protocol

A pre-computed AST knowledge graph (`graph.json`) is available at: `graphify-out/graph.json`
Use `graph.json` before searching or reading multiple source files.

Workflow:

1. Read `graph.json`.
2. Identify the relevant symbols, files, and dependency paths.
3. Read only the source files required for the task.
4. Avoid scanning unrelated files.

Use the graph for:

- dependency tracing
- call-path discovery
- identifying high-centrality modules
- impact analysis before refactoring
- locating symbol definitions

Never read an entire directory simply to locate a symbol. Use `graph.json` to locate the symbol first, then read only the relevant files.

The graph is generated from AST extraction and represents extracted structural relationships only. Treat graph edges as authoritative for structural relationships.
Do not infer dependencies that are absent from the graph.

## ast-grep

Prefer `ast-grep` over `grep` whenever searching source code.

Use `ast-grep` for:

- locating function definitions
- locating class definitions
- locating method implementations
- finding imports
- matching AST patterns
- performing structural search

Use `grep` only for:

- Markdown
- JSON
- YAML
- log files
- generated text
- plain-text configuration

Avoid using `grep` to search programming language source code when `ast-grep` can answer the query.

## Response Style

- Be extremely concise. No pleasantries, no filler.
- When asked to write code, return code only unless explanation is explicitly requested.
- No sycophantic preambles ("Sure!", "Great question!", "Absolutely!").
- No "Here's a function that..." preambles.
- Don't restate the question before answering.
- No "Note:", "Tip:", or "Remember:" appendices unless asked.
- No usage examples unless asked.
- No unsolicited suggestions or improvements beyond what was asked.
- Use short variable names where meaning is clear from context.
