# DESIGN_DECISIONS.md — FrankenQueue Observable Semantics

Frozen before implementation begins (Phase 1). These are the only behaviors callers, tests, and later phases may rely on. Each entry: **Behavior / Rationale / Tradeoff.**

## 1. FIFO/LIFO
**Behavior:** Ordering key is the monotonic sequence number assigned at enqueue: FIFO = sequence ASC, LIFO = sequence DESC.
**Rationale:** Sequence is a stable, already-required total order — no second mechanism needed.
**Tradeoff:** Sequence must never be reused across restarts (see #5).

## 2. Priority
**Behavior:** Primary key priority DESC; secondary key is sequence (ASC for priority-FIFO, DESC for priority-LIFO).
**Rationale:** Reuses the same tie-breaker as #1 instead of introducing a new one.
**Tradeoff:** Priority is a plain bounded int with no fairness/aging mechanism — sustained high-priority load can starve low-priority messages; accepted, out of scope.

## 3. Delay
**Behavior:** Each message carries `available_at`; excluded from the Ready structure while `available_at > now`, becomes eligible under normal ordering once `available_at <= now`.
**Rationale:** Makes delay orthogonal to ordering mode instead of a fifth queue type, matching the composability requirement.
**Tradeoff:** Requires an injectable clock for deterministic tests, and a delayed-heap check on every receive.

## 4. Tie-breaking
**Behavior:** Never use wall-clock timestamps to break ties; always fall back to sequence number.
**Rationale:** Timestamps aren't strictly unique under concurrent enqueue; sequence is guaranteed unique.
**Tradeoff:** None material.

## 5. Sequence semantics
**Behavior:** Strictly monotonic `uint64`, assigned atomically under the per-queue lock at enqueue time, never reused post-restart — recovered as `max(seen)+1` during WAL replay.
**Rationale:** Required for #1/#2/#4 to survive a restart.
**Tradeoff:** Recovery cost scales with WAL size; no compaction in this scope (flagged in the "more time" answer).

## 6. Enqueue durability
**Behavior:** Success is returned only after the ENQUEUE record is appended and `File.Sync()`'d.
**Rationale:** "Successful enqueue is durable" must be unconditionally true.
**Tradeoff:** Throughput bounded by fsync latency; documented as a known limitation, not solved via group commit in this scope.

## 7. ACK durability
**Behavior:** Success is returned only after the ACK record is appended and synced; message removed from in-flight state only after that sync completes. A duplicate ACK for a receipt handle no longer present in in-flight state is rejected via the same stale-handle path as #14 — no special-cased idempotency branch.
**Rationale:** Symmetric with #6 — a restart must never resurrect a completed message. Reusing the stale-handle path avoids a second code path for the same outcome.
**Tradeoff:** Same fsync-latency cost on the ACK path.

## 8. At-least-once delivery
**Behavior:** The system guarantees at-least-once delivery, explicitly not exactly-once; consumers are responsible for idempotency for external side effects.
**Rationale:** Exactly-once for arbitrary side effects is impossible without consumer cooperation — stated honestly rather than oversold.
**Tradeoff:** Consumers must tolerate duplicate processing.

## 9. Leases
**Behavior:** Ephemeral / in-memory only; RECEIVE is never written to the WAL.
**Rationale:** Keeps the WAL and locking simple, avoids a class of lease-persistence races, still satisfies at-least-once.
**Tradeoff:** Every restart redelivers everything currently in-flight-but-unacked, even work that was seconds from completion.

## 10. Restart behavior for in-flight messages
**Behavior:** On restart, any durably-enqueued, non-durably-ACKed message becomes immediately eligible (Ready or Delayed per `available_at`) with a fresh receipt handle; all pre-restart receipt handles are permanently invalid.
**Rationale:** Direct, simplest consequence of #9.
**Tradeoff:** Must be documented loudly for consumers — this is the behavior most likely to surprise a reviewer.

## 11. Corrupted-tail policy
**Behavior:** On WAL replay, corruption (truncated header/body, bad checksum) located at the physical end of the file is treated as an expected crash artifact: keep every valid record before it, discard the damaged tail, boot normally. If a checksum failure is instead followed by further apparently-valid records, that pattern is not explainable by an ordinary crash — recovery fails loudly with an explicit error instead of silently truncating.
**Rationale:** Distinguishes the common case (power loss mid-write) from real corruption, which likely indicates a bug.
**Tradeoff:** Slightly more recovery-logic complexity than "always truncate at first error."

## 12. Payload limit
**Behavior:** Fixed max payload size (1 MiB), enforced both at HTTP ingestion (reject oversized bodies before touching the queue) and when parsing WAL record length (never allocate based on an untrusted length beyond this bound).
**Rationale:** Prevents unbounded allocation from a corrupted or malicious length field.
**Tradeoff:** Legitimate large payloads are rejected — acceptable for a queue; large blobs belong in object storage referenced by pointer in a real system.

## 13. Queue-name rules
**Behavior:** Non-empty, ≤64 characters, charset `[A-Za-z0-9_-]`, validated at creation time.
**Rationale:** Names become URL path segments and durable state; a restrictive charset avoids encoding edge cases entirely.
**Tradeoff:** Less flexible naming — acceptable for an internal system.

## 14. Receipt-handle validity
**Behavior:** Format `messageID:deliveryAttemptSeq`, where `deliveryAttemptSeq` is an in-memory counter incremented on every (re)delivery. ACK is honored only if the presented attempt-seq matches the message's current attempt-seq, checked under the same per-queue lock as lease-expiry evaluation — expiry is lazy/logical, evaluated only under that lock, never an eager out-of-band mutation racing ACK. A late-but-still-current ACK wins over expiry as long as no newer `receive()` has already superseded it.
**Rationale:** Resolves the ACK-vs-lease-expiry race identified in the Phase 0 review; directly satisfies the invariant that a stale receipt handle cannot ACK a newer delivery.
**Tradeoff:** Requires the expiry check and ACK check to be genuinely serialized through one lock — must be covered by a dedicated concurrent test in a later phase.

## 15. Duplicate queue creation
**Behavior:** `CreateQueue` for a name that already exists returns an error (`ErrQueueExists`); it is not an idempotent no-op, and does not check whether the requested config matches the existing one.
**Rationale:** Flagged as ambiguous in the Phase 0 review and left open. Resolved here in favor of the safer default — silently accepting a call that might carry a *different* config than the one already durable would let a caller believe it changed a queue's semantics when it did not.
**Tradeoff:** A caller that intends "create if missing" must first check `GET /queues` or tolerate the error; slightly less convenient than idempotent creation.
