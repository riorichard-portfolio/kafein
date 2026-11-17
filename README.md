# kafein

> **Eliminate lock contention, tame burst traffic, and prevent goroutine leaks in Go — without external dependencies.**

`kafein` is a deterministic concurrency toolkit providing three specialized, channel-orchestrated primitives (`syncq`, `asyncq`, and `trigger`) engineered for high-throughput backend systems where memory bounds, backpressure, and graceful lifecycle management are non-negotiable.

---

## Architecture Decision Records (ADR)

### ADR-001 (`kafein/syncq`): Bounded Work-Coalescing Queue with Atomic Oneshot Replies

* **Context:**
  Under bursty concurrent writes (e.g., hot-row ledger mutations), executing a separate I/O or database transaction per request exhausts connection pools and causes severe lock thrashing. Standard asynchronous queues cannot be used when the caller (such as an HTTP/gRPC handler) must synchronously confirm commit success before responding. Furthermore, naive batching often suffers from idle CPU polling, unbounded memory growth, or hanging callers when a worker panics.
* **Decision:**
  Implement a channel-orchestrated work-coalescing queue (`syncq`) governed by strict boundaries:
  1. **Fail-Fast Backpressure & Enqueue Deadline:** Producers submit tasks into a fixed-capacity channel bounded by an explicit enqueue deadline—rejecting immediately on saturation rather than blocking forever.
  2. **Lazy Linger & Flusher Timeout:** The worker arms its linger timer *only* upon receiving the first item of an empty batch, flushing when either `maxBatchSize` is reached or `lingerDuration` expires, with every flush wrapped in a strict execution timeout.
  3. **Atomic Single-Reply Oneshot Channel:** Each task carries a dedicated buffered-1 oneshot channel protected by atomic/sync guarantees, ensuring producers receive their outcome **at most once** without stalling the worker loop.
  4. **Panic Isolation & Atomic Draining:** Worker-level `recover()` converts flusher panics into structured errors returned to waiting callers; `Close()` atomically rejects new producers while draining all buffered tasks before termination.
* **Consequences & Trade-offs:**
  * *(+) Positive:* Collapses $N$ concurrent requests into $1$ batch execution while preserving synchronous request-response semantics; zero background CPU wakeups when idle; zero dropped acknowledged tasks during shutdown.
  * *(-) Trade-off:* Introduces up to `lingerDuration` of tail latency during low-traffic periods in exchange for order-of-magnitude throughput gains under contention; shifts overload handling to callers via immediate backpressure rejections.

---

### ADR-002 (`kafein/asyncq`): Non-Blocking Fire-and-Forget Coalescing Pipeline

* **Context:**
  High-frequency telemetry, audit logging, and event ingestion pipelines do not require callers to wait for downstream I/O confirmation. Using `syncq` for these workloads wastes memory and CPU cycles allocating per-task oneshot channels and parking producer goroutines—creating unnecessary tail-latency coupling between upstream request paths and downstream storage latency.
* **Decision:**
  Implement `asyncq` using the exact same channel-orchestrated coalescing engine, backpressure bounds, lazy linger timer, flusher timeout, panic recovery, and graceful draining lifecycle as `syncq`—but **strip out oneshot reply channels and caller blocking entirely**:
  1. **Zero-Reply Ingestion:** Producers perform a non-blocking, fail-fast enqueue into a bounded buffer. Once accepted, control returns immediately to the caller.
  2. **Decoupled Error Handling:** Because no producer is waiting on a return channel, flusher errors and recovered panics are routed to an asynchronous error handler hook rather than per-task replies.
* **Consequences & Trade-offs:**
  * *(+) Positive:* Near-zero producer latency overhead and significantly lower heap allocations (`allocs/op`) compared to `syncq`; completely decouples upstream API latency from downstream sink slowdowns.
  * *(-) Trade-off:* Delivery guarantee shifts from synchronous per-request confirmation to best-effort in-memory buffering (at-least-once flush attempt until process crash); callers cannot know whether their specific item succeeded at enqueue time.

---

### ADR-003 (`kafein/trigger`): Trailing-Edge Debounced Signal Orchestrator

* **Context:**
  Periodic state reconciliation, cache invalidation, and asynchronous buffer compaction often suffer from two extremes: either triggering immediately on every event (causing redundant work storms) or running a fixed-interval ticker even when the system is completely idle. In many event-driven workflows, the system only needs to guarantee that a listener executes once after a quiet window ($t_n$, the timestamp of the *last* fired trigger) has elapsed.
* **Decision:**
  Implement `trigger` as a lightweight, single-slot (`chan struct{}` of buffer size 1) signal orchestrator:
  1. **Zero-Allocation Coalesced Signaling:** Multiple concurrent `Fire()` calls coalesce into a single buffered `struct{}` token without blocking callers or allocating memory.
  2. **Trailing-Edge Timer ($t_n$):** Each incoming signal resets the debounce window so the listener executes strictly after $t_n + \text{delay}$, ensuring bursty events trigger only a single consolidated run.
  3. **Resilient Execution & Planned Cron Fallback:** Listener execution is wrapped with panic recovery and error routing, with an architectural extension point (`TODO`) for a configurable fallback cron schedule to guarantee periodic execution even in the absence of events.
* **Consequences & Trade-offs:**
  * *(+) Positive:* Virtually zero memory footprint (`1`-element `struct{}` channel); eliminates redundant execution storms under rapid event bursts; guarantees execution settles after the final event $t_n$.
  * *(-) Trade-off:* Under continuous, uninterrupted trigger storms where intervals between fires are always $< \text{delay}$, execution is deferred until activity subsides (mitigated by the planned max-wait / fallback cron mechanism).

---

## Proof of Work (PoW)

---
* **Repository Reference:** [kafein-proof-of-work](https://github.com/riorichard-portfolio/kafein-proof-of-work)
* **Test Environment:** Local machine, Intel Core i5-1334U, PostgreSQL Windows Installed, Windows PowerShell / k6 load testing tool.
---

# PoW-001: High-Concurrency Hot-Row Ledger Updates via Work-Coalescing (`syncq`)

* **k6 Script Reference :**  syncq-pay.js in kafein-proof-of-work Repo

## 1. Context & Problem Statement
In high-throughput transactional systems (e.g., payment gateways, financial ledgers, digital wallets), concurrent updates targeting a single "hot-row" account create severe database bottlenecks. 

Under heavy load without concurrency shielding, individual database transactions contend for row-level locks (`RowShareLock` / `RowExclusiveLock`), causing:
* **Severe Lock Contention:** Threads queue up sequentially inside the database engine, degrading throughput.
* **Connection Pool Exhaustion:** HTTP handlers block indefinitely while waiting for database connection locks and transaction slots.
* **Tail Latency Explosion (P95/P99):** Response times spike from milliseconds to several seconds.
* **Lost Updates / Failures:** Connection timeouts and serialization errors result in HTTP request failures (non-zero error rates).

---

## 2. Architectural Solution: `kafein/syncq`
To eliminate hot-row locking overhead, we implemented **`kafein/syncq`**, a memory-backed work-coalescing and batch-processing engine written in Go. 

Instead of allowing hundreds of concurrent goroutines to execute independent database transactions against the same row, `syncq` intercepts incoming requests, buffers them into an in-memory queue, merges them into bounded batches based on configurable thresholds (`MaxBatchSize`, `LingerDuration`), and flushes them to PostgreSQL via a single bulk `VALUES` update query within a unified transaction.

---

## 3. Empirical Benchmark Results (Test-Bed Validation)

Both architectures were subjected to identical stress-testing conditions using `k6` under a ramp-up workload scaling up to **200 concurrent Virtual Users (VUs)** for **50 seconds** against a single hot-row account.

### Comparative Performance Matrix

| Metric | Baseline (Direct GORM Transactions) | With `kafein/syncq` (Work-Coalescing) | Performance Delta / Improvement |
| :--- | :--- | :--- | :--- |
| **Throughput (RPS)** | `915.32 req/s` | **`5,940.76 req/s`** | **~6.5x Higher Throughput (+549%)** |
| **Total Requests Handled** | 45,984 requests | **300,253 requests** | Massive capacity uplift |
| **P95 Latency** | `287.04 ms` | **`23.54 ms`** | **~12x Lower Latency (Sub-25ms P95)** |
| **Max Latency** | `3.28 seconds` | **`424.94 ms`** | Eliminated severe tail-latency blocking |
| **Error Rate** | `0.81%` (375 failures/timeouts) | **`0.00%`** (0 errors) | **Zero-Failure Stability** |

---

## 4. Automated Financial Reconciliation & Data Integrity Audit

A critical challenge in asynchronous batching is ensuring that request optimizations never compromise ACID properties or cause ledger discrepancies. 

To validate data correctness, an automated reconciliation hook (`setup` and `teardown` audit cycles in `k6`) compared the aggregate sum of individual transaction logs (`payment_gorms`) against the physical account balance delta (`account_gorms.balance`) across hundreds of thousands of concurrent requests.

### Audit Log Output (`syncq` execution):
```text
INFO[0000] --- [SETUP] Initial Balance: 456070, Initial Total Transactions: 45607 ---
INFO[0050] ========================================
INFO[0050] --- [RECONCILIATION AUDIT REPORT] ---
INFO[0050] Execution Mode          : SYNCQ
INFO[0050] Total Recorded Tx Delta : 300251 transactions
INFO[0050] Expected Amount Delta   : 3002510
INFO[0050] Actual Balance Delta    : 3002510
INFO[0050] ========================================
INFO[0050] ✅ [SUCCESS] Data 100% Accurate! Zero Lost Updates detected.
```


# PoW-002: Aggressive Load Shedding & Bounded Memory Backpressure under Extreme Admission Pressure (`QueueSize: 5`, `BatchSize: 5`)

* **k6 Script Reference :**  syncq-pay-backpreassure.js in kafein-proof-of-work Repo

## 1. Context & Architectural Challenge
In high-concurrency systems, traffic surges (such as flash sales or DDoS-like traffic spikes) often exceed backend processing capacity. 

Without proper admission control, unbounded request queues lead to **Out-Of-Memory (OOM) crashes**, connection pool exhaustion, and catastrophic cascading failures across downstream dependencies. While high throughput is vital, **graceful degradation and bounded safety** are critical architectural invariants for resilient system design.

---

## 2. Architectural Solution: Bounded Buffer & Load Shedding
To enforce strict resource guarantees, `kafein/syncq` utilizes fixed-capacity memory structures (`QueueSize: 5`, `WorkerBatchSize: 5`). 

When incoming request volume drastically outpaces worker flush velocity, the internal queue hits capacity instantly. Rather than blocking indefinitely or allocating unbounded heap memory, `syncq` triggers **immediate load shedding (fail-fast rejection)**. Excess requests are safely shed at the admission gate, preserving process stability and guaranteeing deterministic memory footprints under extreme pressure.

---

## 3. Empirical Stress Test Results (Extreme Saturation)

The application was subjected to an aggressive ramp-up scaling up to **500 concurrent Virtual Users (VUs)** over **50 seconds** against an intentionally constrained queue capacity (`QueueSize = 5`).

### Quantitative Stress-Test Metrics

| Metric | Measured Value | Architectural Interpretation |
| :--- | :--- | :--- |
| **Total Requests Flooded** | `657,342 requests` | Massive load surge hitting a tightly bound queue |
| **Throughput (RPS)** | `13,145.79 req/s` | High-speed request ingestion and rejection loop |
| **Queue Rejection Rate** | `96.25% (632,706 dropped)` | Aggressive load shedding protecting backend resources |
| **Application Error Rate** | `0.00%` | Zero internal panics, unhandled exceptions, or OOM crashes |
| **P95 Latency (Overall)** | `48.36 ms` | Request turnaround remains fast even during massive shedding |

---

## 4. Key Engineering Takeaways & Portfolio Impact

1. **Deterministic Memory Bounds:** Despite handling over 657,000 requests under heavy VUs, heap allocation remained entirely flat because the queue capacity is statically bounded to 5 items.
2. **Defensive Load Shedding (`Fail-Fast`):** The system successfully rejected 96.25% of traffic that exceeded capacity, preventing database connection lockouts and protecting system availability.
3. **Graceful Recovery:** The application remained fully operational throughout the stress test and immediately resumed normal throughput during the cool-down phase without requiring service restarts.