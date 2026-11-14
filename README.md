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

*(Di bawah bagian ADR ini, kita tempatkan PoW-001, PoW-002, dan PoW-003 berisi hasil benchmark `go test -bench=. -benchmem`, rasio coalescing, dan kesimpulan masing-masing).*