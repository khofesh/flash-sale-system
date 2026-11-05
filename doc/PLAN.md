# Flash Sale System — Implementation Plan

Status: v0.1 · Implements [`doc/SPEC.md`](./SPEC.md) (§ references below point there)

The plan has two parts:

- **Phase 0, the walking skeleton.** A "hello world" request goes through **every** component in the final architecture: web → nginx → API → Redis stream → orchestrator → Postgres → SQS (Floci) → payment → shipping → back. If Phase 0 works, all wiring, networking, configuration, tests and stress tooling are proven before any business logic exists.
- **Phases 1–6** replace the hello-world logic with the real feature, one vertical slice at a time. Each phase ends with something you can run and check.

Every phase has a **Verify** block. A phase is done only when every step in it passes.

---

## Conventions (apply to all phases)

- **Runtime:** Node.js 24 LTS, pnpm workspaces, TypeScript `strict`, ESM.
- **Versions:** exact versions are pinned in `package.json` and the lockfile. Docker images are pinned to exact tags (`redis:7.4.x`, `postgres:16.x`, `floci/floci:2.4.0`, `nginx:1.27.x`, `grafana/k6:<x.y.z>`).
- **Root scripts** (stable from Phase 0 on):

  | Command | Does |
  |---|---|
  | `pnpm i` | Install all workspaces |
  | `pnpm build` | Type-check and build all packages |
  | `pnpm lint` | ESLint + Prettier check |
  | `pnpm test` | Unit tests (no Docker) |
  | `pnpm test:int` | Integration tests (Testcontainers, needs Docker) |
  | `pnpm stack:up` / `pnpm stack:down` | `docker compose up -d --build --wait` / `down -v` |
  | `pnpm e2e` | Scripted end-to-end check against the running stack |
  | `pnpm stress` | k6 + post-run verification |
  | `pnpm dev` | API + web in watch mode against compose infrastructure |

- **Commits:** one commit per checklist group, so each phase can be reviewed on its own.

---

## Phase 0: Walking skeleton ("hello world" end-to-end)

**Goal:** `docker compose up`, open the web page, type a name, click **Say hello**, and watch the message go through API → Redis → orchestrator → Postgres → payment → shipping, with a timeline showing every hop.

### 0.1 Repo scaffolding
- [x] `pnpm-workspace.yaml`, root `package.json` with the scripts above, `tsconfig.base.json`, ESLint/Prettier config, `.editorconfig`, `.nvmrc` (`24`), `.gitignore`, `.env.example`.
- [x] Workspaces, each with `package.json`, `tsconfig.json` and `src/index.ts`:
  - `packages/config`: zod env parsing (`loadConfig()`), shared by all apps.
  - `packages/core`: empty domain module plus a `hello()` pure function (gives the unit test something to test).
  - `packages/infra`: Redis client factory, pg Pool factory, `MessageBus` interface with `SqsBus` and `InMemoryBus`, structured logger (pino).
  - `apps/api`, `apps/orchestrator`, `apps/payment`, `apps/shipping`, `apps/web`.
- [x] One multi-stage `Dockerfile` with a build arg `APP=<name>`, so every Node service uses the same image recipe.

### 0.2 Infrastructure (`infra/`)
- [x] `docker-compose.yml` with all final services and health checks: `redis` (AOF on), `postgres`, `floci`, `queue-init` (one-shot), `api` (×2 for now), `orchestrator`, `payment`, `shipping`, `nginx`, `web`.
- [x] `infra/init-queues.ts`: creates `payment-requests`, `shipping-requests` and `order-events`, each with a `-dlq` and `RedrivePolicy { maxReceiveCount: 5 }`. Safe to re-run.
- [x] `infra/migrations/0000_hello.sql`: `hello_messages (id uuid pk, name text, hops jsonb, status text, created_at, updated_at)`. A tiny migration runner (`node-pg-migrate` or a ~30-line custom script) runs at orchestrator start.
- [x] `infra/nginx.conf`: `/api/*` → API upstream (round-robin), `/` → web. Adds an `X-Upstream` response header so you can see load balancing working.

### 0.3 Hello flow
- [x] **API** (Fastify): `GET /healthz`, `GET /readyz` (pings Redis + PG), `POST /api/hello {name}` → `XADD hello:stream * id name` → `202 {id}`. `GET /api/hello/:id` reads PG, or returns `{status:"QUEUED"}` if not persisted yet.
- [x] **Orchestrator:** `XREADGROUP` on `hello:stream` (group `orchestrator`) → insert row (`ON CONFLICT DO NOTHING`), add hop `orchestrator` → publish `hello.payment` to `payment-requests` → `XACK`. Consumes `order-events`: on `hello.paid`, add hop and publish `hello.ship`. On `hello.shipped`, add hop and set `status=DONE`.
- [x] **Payment:** consume `payment-requests` → sleep 100 ms → publish `hello.paid` to `order-events`.
- [x] **Shipping:** consume `shipping-requests` → publish `hello.shipped` to `order-events`.
- [x] Every hop records `{service, hostname, at}`, so the timeline shows which replica handled each step.

### 0.4 Web
- [x] Vite + React + TS. One page: name input, **Say hello** button, then polls `GET /api/hello/:id` every 500 ms and renders the hop timeline until `DONE`.
- [x] Served by nginx in compose. `pnpm dev` uses the Vite proxy to `localhost:8080`.

### 0.5 Test and stress tooling
- [x] Vitest workspace config. One unit test (`core/hello`). One integration test using Testcontainers (Redis + Postgres + Floci) that runs the orchestrator, payment and shipping **in-process** and checks a hello reaches `DONE`.
- [x] `scripts/e2e.ts`: POST hello via nginx, poll until `DONE` (timeout 15 s), assert the hops are `[api, orchestrator, payment, orchestrator, shipping, orchestrator]`.
- [x] `stress/smoke.js` (k6, run via the `grafana/k6` Docker image so nothing needs installing locally): 50 VUs × 20 s of `POST /api/hello`. Thresholds: `http_req_failed < 1%`, `p(95) < 200ms`.
- [ ] GitHub Actions (optional): `lint → test → test:int`.

### ✅ Verify Phase 0
```bash
pnpm i && pnpm build && pnpm lint && pnpm test && pnpm test:int
pnpm stack:up                                 # every service healthy
curl -s localhost:8080/readyz             # {"redis":"ok","postgres":"ok"}
pnpm e2e                                  # PASS, prints the hop timeline
open http://localhost:8080                # manual: type name → timeline reaches DONE
open http://localhost:4566/_floci/ui      # queues + DLQs exist and are empty
pnpm stress -- smoke                      # thresholds green
podman-compose -f ./infra/docker-compose.yml restart orchestrator && pnpm e2e   # survives a restart
pnpm stack:down
```

---

## Phase 1: Sale configuration and status (SPEC §4.1, §6.1, §6.4)

- [ ] Migration `0001_sales.sql` (`sales` table, SPEC §5.3).
- [ ] `core/sale.ts`: `deriveSaleStatus(meta, stock, now)`, pure.
- [ ] `infra/saleRepo`: seed from env (`SALE_*`) on start if missing, mirror to Redis `sale:{id}:meta` and `:stock`.
- [ ] `POST /admin/sale` (guarded by `ADMIN_TOKEN`): create or reset a sale (PG first, then Redis; clears buyers and stream).
- [ ] `GET /api/sale` with a 250 ms in-process cache and `serverTime`.
- [ ] Web: status banner with countdown (corrected for clock offset) and remaining stock.
- [ ] Tests: status at the 4 window boundaries (unit), and `/api/sale` via `fastify.inject` (integration).

**Verify:** reset a sale starting 30 s from now with `curl`. The UI counts down UPCOMING → ACTIVE. Reset with `endsAt` in the past → ENDED.

---

## Phase 2: Atomic reservation, the core of the brief (SPEC §5.2, §6.2, §6.3)

- [ ] `infra/redis/reserve.lua` and `release.lua`, loaded with `SCRIPT LOAD` and called with `EVALSHA` (reload on `NOSCRIPT`).
- [ ] `core/userId.ts`: normalise and validate.
- [ ] `POST /api/purchases`: map script results to HTTP (SPEC §6.2 table). `Idempotency-Key` support (Redis `SET NX EX 600`).
- [ ] `GET /api/purchases/:userId`: PG first, then a Redis buyers-set fallback (returns `RESERVED`).
- [ ] Orchestrator: replace the hello consumer with a `sale:{id}:reservations` consumer → insert `orders` row as `RESERVED` (migration `0002_orders.sql` with the partial unique index, `saga_events`, `processed_messages`). Payment is still a stub that marks the order `COMPLETED` directly, so Phase 2 alone satisfies the brief (SPEC §14 milestone 1).
- [ ] Web: `userId` input, **Buy Now** (disabled unless ACTIVE or while a request is in flight), feedback messages per code, poll the purchase status.
- [ ] **Concurrency integration tests** (SPEC §12.2): 1,000 parallel reserves for stock 100 → exactly 100 reserved. 500 parallel calls for one user → exactly 1. Release twice → stock +1 only once.
- [ ] `stress/spike.js` + `stress/verify.ts` (SPEC §12.3 "Spike" and "Same-user abuse").

**Verify:** `pnpm test:int` green. `pnpm stress -- spike` → exactly `totalStock` × 202, the rest 410, zero 5xx, and `verify.ts` passes (no oversell, one order per user). Manual: buy twice with the same id in the UI → "already purchased".

---

## Phase 3: Payment and shipping saga (SPEC §4.2, §7)

- [ ] `core/orderStateMachine.ts`: transitions table, `nextAction(order, event)` as a pure function, plus compensation rules.
- [ ] `infra/orderRepo.transition(id, from, to)` (conditional UPDATE) + `saga_events` append + `processed_messages` dedup, all in one transaction.
- [ ] Orchestrator: `RESERVED → PAYMENT_PENDING` (publish `payment.requested`), handle `payment.succeeded/failed`, `PAID → SHIPPING_PENDING`, `shipping.succeeded/failed`, refund path. Compensation calls `release.lua`.
- [ ] Payment mock: `PAYMENT_FAILURE_RATE`, `PAYMENT_LATENCY_MS`, idempotent on `orderId`, supports `payment.refund_requested`.
- [ ] Shipping mock: `SHIPPING_FAILURE_RATE`, idempotent.
- [ ] Payment-timeout sweeper (every 5 s, `FOR UPDATE SKIP LOCKED`): `PAYMENT_PENDING` older than `PAYMENT_TIMEOUT_MS` → CANCELLED + release. A late success after cancel → refund.
- [ ] Web: progress line Reserved → Paid → Shipped / Cancelled (with reason). On CANCELLED while the sale is still ACTIVE, show "try again".
- [ ] Remove the hello flow (keep `0000_hello.sql` history, drop the table in a new migration).
- [ ] Tests: state machine (unit, every transition). Saga end-to-end with failure rates 0 and 1. Duplicate message delivery is a no-op. Broker contract test (SPEC §12.2).

**Verify:** with `PAYMENT_FAILURE_RATE=1`, a purchase ends CANCELLED, stock is back to N, and the same user can buy again. With `0`, it reaches COMPLETED. The Floci UI shows the DLQs empty. `/admin/reconcile` reports no drift.

---

## Phase 4: Resilience (SPEC §9.3)

- [ ] Orchestrator: `XAUTOCLAIM` of pending entries idle for more than 30 s, and graceful shutdown (stop reading, finish in-flight work, exit).
- [ ] API: fail closed with 503 + `Retry-After` when Redis is down. Graceful shutdown via Fastify `close()`.
- [ ] PG outage: orchestrator retries with exponential backoff + jitter, status endpoint falls back to Redis.
- [ ] Redis rebuild job (`pnpm rebuild-redis`): recompute stock and buyers from PG live orders + unconsumed stream entries.
- [ ] `GET /admin/reconcile`: check the invariant from SPEC §5.3.
- [ ] Tests: orchestrator crash mid-message → exactly-once persistence. Redis down → 503, no 500s.

**Verify:** `stress/chaos.sh` runs the spike while `docker kill`-ing one API replica and the orchestrator. `verify.ts` passes, and every reservation is eventually persisted.

---

## Phase 5: Throughput hardening (SPEC §9.1, §9.2)

- [ ] nginx `limit_req` per IP (`RATE_LIMIT_RPS`) → 429 JSON. API replicas ×4, orchestrator ×2, payment ×2.
- [ ] Per-replica sold-out short-circuit flag (re-checked every 1 s).
- [ ] `Cache-Control` on `GET /api/sale`. Tune Redis and pg pool sizes.
- [ ] `/metrics` (prom-client): request rate and latency histogram, reserve outcomes, stream lag, DLQ depth.
- [ ] `stress/soak.js` (2k req/s × 5 min).
- [ ] Record results in `stress/results/` (k6 summary JSON + verify output).

**Verify:** spike meets the SPEC §9.1 targets (≥ 5k req/s, p99 < 50 ms, 0 oversell). Soak keeps latency flat with no memory growth.

---

## Phase 6: Deliverables (SPEC §13)

- [ ] `README.md`: overview, architecture diagram (Mermaid + exported PNG in `doc/`), design choices and trade-offs (Redis-first decision, stream as outbox, orchestrated saga, Floci vs LocalStack, AOF durability window), how to run (compose, dev, tests, stress), stress results summary with how to read them.
- [ ] Clean-clone run-through: `git clone → pnpm i → pnpm stack:up → pnpm e2e → pnpm test:int → pnpm stress`.
- [ ] Final pass: lint, dead code, consistent error codes, `.env.example` complete.

**Verify:** someone else (or a fresh VM) follows only the README and gets the stack running plus green tests and stress.

---

## Phase dependencies and scope cuts

```
P0 ──► P1 ──► P2 ──► P3 ──► P4 ──► P5 ──► P6
                 │
                 └─► (brief fully satisfied here; P6 README can be written early)
```

If time is short, cut from the end: P5 (keep the spike test from P2), then P4's rebuild job and chaos script. **Never cut:** P2 concurrency tests and `verify.ts`, because they prove correctness.

## Risks

| Risk | Mitigation |
|---|---|
| Floci SQS behaviour differs from real SQS | Pinned tag. The broker contract test runs in `test:int`. Swap to ElasticMQ via `SQS_ENDPOINT`. |
| Testcontainers slow or flaky on CI | Reuse containers per test file. Fall back to `docker compose` services in CI. |
| k6 numbers depend on the laptop | Report hardware with results. Correctness thresholds (oversell = 0) never depend on hardware. |
| Clock drift between replicas | All window checks use Redis `TIME` inside Lua (SPEC §4.1). |
