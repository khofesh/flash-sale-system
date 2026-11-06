# Graph Report - flash-sale-system  (2026-11-06)

## Corpus Check
- 88 files · ~32,384 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 765 nodes · 1205 edges · 44 communities (39 shown, 5 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.89)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Docs, Plan and Compose Stack
- Reconcile and Core Imports
- Saga State Machine and Core
- Root Workspace Tooling
- MessageBus Adapters
- API Routes and Purchases
- Web Package Manifest
- Migrations and Sale Tests
- Service Entrypoints
- Infra Package Manifest
- Orchestrator Stream Consumer
- Config Package Manifest
- Order Repository
- k6 Stress Scenarios
- Saga Integration Tests
- Payment Package Manifest
- Shipping Package Manifest
- Web App UI
- Base TypeScript Config
- Migration Runner and Stress
- API Package Manifest
- Orchestrator Package Manifest
- Core Package Manifest
- Payment and Shipping Workers
- Web TypeScript Options
- Stress Verification
- Payment and Web Tsconfig
- Orchestrator Saga Handlers
- API Entrypoint
- Core Tsconfig
- Broker Contract Test
- API Tsconfig
- Orchestrator Tsconfig
- Shipping Tsconfig
- Config Tsconfig
- Infra Tsconfig
- Vitest Config
- API Dependencies
- Prettier Config
- E2E Script
- Orchestrator Dependencies

## God Nodes (most connected - your core abstractions)
1. `compilerOptions` - 16 edges
2. `vitest` - 13 edges
3. `saleRedisKeys()` - 13 edges
4. `compilerOptions` - 12 edges
5. `scripts` - 11 edges
6. `QueueName` - 11 edges
7. `RedisReservationStore` - 10 edges
8. `System Invariants` - 10 edges
9. `Implementation Plan` - 10 edges
10. `buildApp()` - 9 edges

## Surprising Connections (you probably didn't know these)
- `Rootless Podman via DOCKER_HOST` --conceptually_related_to--> `Implementation Plan`  [AMBIGUOUS]
  README.md → doc/PLAN.md
- `Deterministic Failure Rate Defaults` --conceptually_related_to--> `Technical Specification`  [AMBIGUOUS]
  README.md → doc/SPEC.md
- `pnpm Workspace Config` --implements--> `Technical Specification`  [INFERRED]
  pnpm-workspace.yaml → doc/SPEC.md
- `Flash Sale Compose Stack` --conceptually_related_to--> `Technical Specification`  [AMBIGUOUS]
  infra/docker-compose.yml → doc/SPEC.md
- `startWorkers()` --calls--> `startOrchestrator()`  [EXTRACTED]
  tests/saga.int.test.ts → apps/orchestrator/src/worker.ts

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **No-Oversell and One-Per-User Guarantees** — doc_spec_reserve_lua, doc_spec_holders_hash, doc_spec_partial_unique_index, doc_spec_stock_invariant, doc_spec_stress_verify [INFERRED 0.85]
- **Saga Compensation Flow** — doc_spec_order_state_machine, doc_spec_conditional_transitions, doc_spec_payment_timeout_sweeper, doc_spec_release_lua, doc_spec_durable_release [EXTRACTED 1.00]
- **Swappable SQS Broker** — doc_spec_message_bus, doc_spec_floci_decision, doc_spec_elasticmq, doc_spec_broker_contract_test [EXTRACTED 1.00]

## Communities (44 total, 5 thin omitted)

### Community 0 - "Docs, Plan and Compose Stack"
Cohesion: 0.06
Nodes (42): AGENTS.md Agent Guidance, Web SPA Root Mount (index.html), Implementation Plan, Phase 2: Atomic Reservation, Phase 3: Payment and Shipping Saga, Phase 4: Resilience, Phase 5: Throughput Hardening, Broker Contract Test (+34 more)

### Community 1 - "Reconcile and Core Imports"
Cohesion: 0.06
Nodes (35): countUnpersisted(), fieldValue(), readOrders(), readRedisState(), ReconcileReport, reconcileSale(), RedisState, redisUnchanged() (+27 more)

### Community 2 - "Saga State Machine and Core"
Cohesion: 0.05
Nodes (40): deterministicFailure(), mockReference(), isSagaEnvelope(), makeEnvelope(), makeResultEnvelope(), move(), nextAction(), ORDER_TRANSITIONS (+32 more)

### Community 3 - "Root Workspace Tooling"
Cohesion: 0.05
Nodes (43): devDependencies, @aws-sdk/client-sqs, eslint, @eslint/js, @fss/config, @fss/core, @fss/infra, globals (+35 more)

### Community 4 - "MessageBus Adapters"
Cohesion: 0.10
Nodes (20): InMemoryBus, SqsBus, SqsBusOptions, BusMessage, MessageBus, MessageHandler, QUEUE_NAMES, QueueName (+12 more)

### Community 5 - "API Routes and Purchases"
Cohesion: 0.09
Nodes (15): AdminSaleBody, buildApp(), BuildAppDeps, isPgDataError(), SaleView, sha256(), tokenMatches(), CachedResponse (+7 more)

### Community 6 - "Web Package Manifest"
Cohesion: 0.08
Nodes (23): dependencies, react, react-dom, devDependencies, @types/react, @types/react-dom, typescript, vite (+15 more)

### Community 7 - "Migrations and Sale Tests"
Cohesion: 0.12
Nodes (10): config, logger, pool, @testcontainers/postgresql, @testcontainers/redis, getPurchase(), images, waitForStatus() (+2 more)

### Community 8 - "Service Entrypoints"
Cohesion: 0.12
Nodes (14): bus, config, logger, pool, redis, bus, config, logger (+6 more)

### Community 9 - "Infra Package Manifest"
Cohesion: 0.08
Nodes (22): @aws-sdk/client-sqs, @types/pg, dependencies, @aws-sdk/client-sqs, @fss/core, ioredis, pg, pino (+14 more)

### Community 10 - "Orchestrator Stream Consumer"
Cohesion: 0.09
Nodes (7): fieldsToObject(), InvalidEntryError, isUuid(), OrchestratorDeps, ReservationFields, RESERVATIONS_GROUP, StreamEntry

### Community 11 - "Config Package Manifest"
Cohesion: 0.09
Nodes (18): dependencies, zod, devDependencies, @types/node, typescript, exports, main, name (+10 more)

### Community 12 - "Order Repository"
Cohesion: 0.11
Nodes (14): appendSagaEvent(), getOrderById(), getOrderByUser(), ORDER_EVENTS_CONSUMER, OrderRecord, OrderRef, OrderRow, OrderTransition (+6 more)

### Community 13 - "k6 Stress Scenarios"
Cohesion: 0.09
Nodes (18): already, expectedReserved, iterations, options, reserved, serverErrors, unexpected, users (+10 more)

### Community 14 - "Saga Integration Tests"
Cohesion: 0.13
Nodes (11): startPayment(), startShipping(), get(), getPurchase(), images, startWorkers(), stock(), waitForStatus() (+3 more)

### Community 15 - "Payment Package Manifest"
Cohesion: 0.12
Nodes (16): dependencies, @fss/config, @fss/core, @fss/infra, devDependencies, @types/node, typescript, main (+8 more)

### Community 16 - "Shipping Package Manifest"
Cohesion: 0.12
Nodes (16): dependencies, @fss/config, @fss/core, @fss/infra, devDependencies, @types/node, typescript, main (+8 more)

### Community 17 - "Web App UI"
Cohesion: 0.15
Nodes (16): App(), buy(), Feedback, feedbackFor(), finalStatuses, formatCountdown(), orderLabels, OrderStatus (+8 more)

### Community 18 - "Base TypeScript Config"
Cohesion: 0.12
Nodes (16): compilerOptions, declaration, declarationMap, esModuleInterop, forceConsistentCasingInFileNames, lib, module, moduleResolution (+8 more)

### Community 19 - "Migration Runner and Stress"
Cohesion: 0.14
Nodes (7): args, config, name, result, ScenarioName, scenarios, scriptPath

### Community 20 - "API Package Manifest"
Cohesion: 0.14
Nodes (13): devDependencies, @types/node, typescript, main, name, private, scripts, build (+5 more)

### Community 21 - "Orchestrator Package Manifest"
Cohesion: 0.15
Nodes (12): devDependencies, @types/node, typescript, main, name, private, scripts, build (+4 more)

### Community 22 - "Core Package Manifest"
Cohesion: 0.15
Nodes (12): typescript, devDependencies, typescript, exports, main, name, private, scripts (+4 more)

### Community 24 - "Web TypeScript Options"
Cohesion: 0.17
Nodes (12): compilerOptions, declaration, declarationMap, jsx, lib, module, moduleResolution, noEmit (+4 more)

### Community 25 - "Stress Verification"
Cohesion: 0.25
Nodes (5): assert(), isDrained(), readReport(), readTotalStock(), verifyStack()

### Community 26 - "Payment and Web Tsconfig"
Cohesion: 0.20
Nodes (8): ../../tsconfig.base.json, compilerOptions, outDir, rootDir, extends, include, extends, include

### Community 27 - "Orchestrator Saga Handlers"
Cohesion: 0.27
Nodes (10): applyEvent(), applySteps(), consumeReservations(), ensureGroup(), handleOrderEvent(), handleReservation(), publish(), releaseOrder() (+2 more)

### Community 28 - "API Entrypoint"
Cohesion: 0.25
Nodes (6): app, config, logger, pool, redis, reservations

### Community 29 - "Core Tsconfig"
Cohesion: 0.29
Nodes (6): compilerOptions, outDir, rootDir, exclude, extends, include

### Community 30 - "Broker Contract Test"
Cohesion: 0.38
Nodes (4): testcontainers, createQueue(), createWithDlq(), name()

### Community 31 - "API Tsconfig"
Cohesion: 0.33
Nodes (5): compilerOptions, outDir, rootDir, extends, include

### Community 32 - "Orchestrator Tsconfig"
Cohesion: 0.33
Nodes (5): compilerOptions, outDir, rootDir, extends, include

### Community 33 - "Shipping Tsconfig"
Cohesion: 0.33
Nodes (5): compilerOptions, outDir, rootDir, extends, include

### Community 34 - "Config Tsconfig"
Cohesion: 0.33
Nodes (5): compilerOptions, outDir, rootDir, extends, include

### Community 35 - "Infra Tsconfig"
Cohesion: 0.33
Nodes (5): compilerOptions, outDir, rootDir, extends, include

### Community 37 - "API Dependencies"
Cohesion: 0.40
Nodes (5): dependencies, fastify, @fss/config, @fss/core, @fss/infra

### Community 38 - "Prettier Config"
Cohesion: 0.40
Nodes (4): printWidth, semi, singleQuote, trailingComma

### Community 39 - "E2E Script"
Cohesion: 0.50
Nodes (4): main(), postPurchase(), PurchaseView, SaleView

### Community 40 - "Orchestrator Dependencies"
Cohesion: 0.50
Nodes (4): dependencies, @fss/config, @fss/core, @fss/infra

## Ambiguous Edges - Review These
- `Rootless Podman via DOCKER_HOST` → `Implementation Plan`  [AMBIGUOUS]
  README.md · relation: conceptually_related_to
- `Deterministic Failure Rate Defaults` → `Technical Specification`  [AMBIGUOUS]
  README.md · relation: conceptually_related_to
- `Phase 4: Resilience` → `GET /admin/reconcile`  [AMBIGUOUS]
  doc/PLAN.md · relation: implements
- `Technical Specification` → `Flash Sale Compose Stack`  [AMBIGUOUS]
  infra/docker-compose.yml · relation: conceptually_related_to

## Knowledge Gaps
- **11 isolated node(s):** `@types/react`, `@types/react-dom`, `eslint`, `pino-pretty`, `prettier` (+6 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 410 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **5 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `typescript` connect `Core Package Manifest` to `Root Workspace Tooling`, `Web Package Manifest`, `Infra Package Manifest`, `Config Package Manifest`, `Payment Package Manifest`, `Shipping Package Manifest`, `API Package Manifest`, `Orchestrator Package Manifest`?**
  _High betweenness centrality (0.154) - this node is a cross-community bridge._
- **What connects `@types/react`, `@types/react-dom`, `eslint` to the rest of the system?**
  _11 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Docs, Plan and Compose Stack` be split into smaller, more focused modules?**
  _Cohesion score 0.061367621274108705 - nodes in this community are weakly interconnected._
- **Why does `vitest` connect `Vitest Config` to `Saga State Machine and Core`, `Root Workspace Tooling`, `Migrations and Sale Tests`, `Saga Integration Tests`, `Broker Contract Test`?**
  _High betweenness centrality (0.109) - this node is a cross-community bridge._
- **Should `Reconcile and Core Imports` be split into smaller, more focused modules?**
  _Cohesion score 0.05989110707803993 - nodes in this community are weakly interconnected._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `Rootless Podman via DOCKER_HOST` and `Implementation Plan`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Deterministic Failure Rate Defaults` and `Technical Specification`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._