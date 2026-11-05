# Graph Report - flash-sale-system  (2026-11-05)

## Corpus Check
- Corpus is ~6,316 words - fits in a single context window. You may not need a graph.

## Summary
- 84 nodes · 137 edges · 8 communities
- Extraction: 76% EXTRACTED · 24% INFERRED · 0% AMBIGUOUS · INFERRED: 33 edges (avg confidence: 0.87)
- Token cost: 70,287 input · 0 output

## Community Hubs (Navigation)
- API Surface and Invariants
- Saga State and Persistence
- Plan and Walking Skeleton
- SQS Queues and Services
- Spec Decisions and Targets
- Redis Reservation Hot Path
- Duplicate Prevention and Compensation
- MessageBus Abstraction

## God Nodes (most connected - your core abstractions)
1. `API (Fastify, stateless)` - 11 edges
2. `Flash Sale System Implementation Plan` - 10 edges
3. `Flash Sale System Technical Specification` - 10 edges
4. `Order Orchestrator worker` - 10 edges
5. `Floci SQS emulator` - 9 edges
6. `Postgres 16 (durable source of truth)` - 8 edges
7. `reserve.lua atomic reserve script` - 7 edges
8. `Phase 2: Atomic reservation` - 7 edges
9. `Redis 7 (AOF everysec)` - 6 edges
10. `Order state machine` - 6 edges

## Surprising Connections (you probably didn't know these)
- `No-oversell invariant (sold + available == initial_stock)` --semantically_similar_to--> `Stock invariant (redis stock + live orders + unpersisted stream = total_stock)`  [INFERRED] [semantically similar]
  AGENTS.md → doc/SPEC.md
- `Flash Sale System README` --conceptually_related_to--> `Flash Sale System Technical Specification`  [INFERRED]
  README.md → doc/SPEC.md
- `Cut scope from end of PLAN; never cut Phase 2 concurrency tests or verify.ts` --references--> `stress/verify.ts`  [INFERRED]
  AGENTS.md → doc/SPEC.md
- `Phase 6: Deliverables` --references--> `Flash Sale System README`  [INFERRED]
  doc/PLAN.md → README.md
- `sale:{id}:buyers set` --implements--> `One active order per user per sale`  [INFERRED]
  doc/SPEC.md → AGENTS.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Orchestrated payment/shipping saga flow over SQS** — doc_spec_order_orchestrator, doc_spec_payment_requests_queue, doc_spec_payment_service, doc_spec_order_events_queue, doc_spec_shipping_requests_queue, doc_spec_shipping_service [EXTRACTED 1.00]
- **Atomic reservation hot path** — doc_spec_post_api_purchases, doc_spec_reserve_lua, doc_spec_buyers_set, doc_spec_reservations_stream, doc_spec_redis [EXTRACTED 1.00]
- **Idempotency and duplicate-prevention layers** — doc_spec_processed_messages_table, doc_spec_optimistic_concurrency_transitions, doc_spec_orders_one_live_per_user, doc_spec_buyers_set, doc_spec_idempotency_key [INFERRED 0.85]

## Communities (8 total, 0 thin omitted)

### Community 0 - "API Surface and Invariants"
Cohesion: 0.14
Nodes (12): GET /admin/reconcile, API (Fastify, stateless), GET /api/purchases/:userId, GET /api/sale, Idempotency-Key header, k6 stress scenarios (Spike, Same-user abuse, Soak, Chaos), GET /metrics, nginx load balancer + rate limit (+4 more)

### Community 1 - "Saga State and Persistence"
Cohesion: 0.18
Nodes (11): infra/orderRepo.transition, core/orderStateMachine.ts nextAction, Phase 3: Payment and shipping saga, Saga message Envelope<T>, Order state machine, orders table, Payment timeout sweeper, Postgres 16 (durable source of truth) (+3 more)

### Community 2 - "Plan and Walking Skeleton"
Cohesion: 0.22
Nodes (10): graphify knowledge graph (graphify-out/), core/sale.ts deriveSaleStatus, Hello flow (hello:stream through every hop), Flash Sale System Implementation Plan, infra/init-queues.ts (queue-init), Phase 0: Walking skeleton (hello world end-to-end), Phase 1: Sale configuration and status, Phase 2: Atomic reservation (+2 more)

### Community 3 - "SQS Queues and Services"
Cohesion: 0.29
Nodes (11): Broker contract test (Floci and ElasticMQ), SQS Dead Letter Queues (maxReceiveCount 5), ElasticMQ (fallback SQS emulator), Floci SQS emulator, LocalStack, SQS order-events queue, Order Orchestrator worker, SQS payment-requests queue (+3 more)

### Community 4 - "Spec Decisions and Targets"
Cohesion: 0.20
Nodes (6): Phase 5: Throughput hardening, Phase 6: Deliverables, Delivery milestones (Core, Saga, Hardening), Flash Sale System Technical Specification, Throughput targets (>=5k req/s, p99<50ms), Flash Sale System README

### Community 5 - "Redis Reservation Hot Path"
Cohesion: 0.31
Nodes (7): Phase 4: Resilience, Redis 7 (AOF everysec), Redis rebuild job, Redis sale keys (meta, stock, buyers, reservations), sale:{id}:reservations stream, reserve.lua atomic reserve script, XAUTOCLAIM pending-entry recovery

### Community 6 - "Duplicate Prevention and Compensation"
Cohesion: 0.33
Nodes (4): sale:{id}:buyers set, Compensation (release reservation to stock), orders_one_live_per_user partial unique index, release.lua compensation script

### Community 7 - "MessageBus Abstraction"
Cohesion: 0.50
Nodes (3): InMemoryBus adapter, MessageBus interface (publish, subscribe), SqsBus adapter

## Knowledge Gaps
- **15 isolated node(s):** `graphify knowledge graph (graphify-out/)`, `Web (React + Vite SPA)`, `LocalStack`, `sales table`, `orders table` (+10 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 20 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Order Orchestrator worker` connect `SQS Queues and Services` to `Saga State and Persistence`, `Spec Decisions and Targets`, `Redis Reservation Hot Path`, `Duplicate Prevention and Compensation`?**
  _High betweenness centrality (0.224) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `Floci SQS emulator` (e.g. with `SQS Dead Letter Queues (maxReceiveCount 5)` and `ElasticMQ (fallback SQS emulator)`) actually correct?**
  _`Floci SQS emulator` has 6 INFERRED edges - model-reasoned connections that need verification._
- **What connects `graphify knowledge graph (graphify-out/)`, `Web (React + Vite SPA)`, `LocalStack` to the rest of the system?**
  _15 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `API Surface and Invariants` be split into smaller, more focused modules?**
  _Cohesion score 0.14166666666666666 - nodes in this community are weakly interconnected._
- **Why does `API (Fastify, stateless)` connect `API Surface and Invariants` to `Saga State and Persistence`, `Redis Reservation Hot Path`?**
  _High betweenness centrality (0.211) - this node is a cross-community bridge._
- **Why does `Flash Sale System Implementation Plan` connect `Plan and Walking Skeleton` to `Saga State and Persistence`, `SQS Queues and Services`, `Spec Decisions and Targets`, `Redis Reservation Hot Path`?**
  _High betweenness centrality (0.199) - this node is a cross-community bridge._