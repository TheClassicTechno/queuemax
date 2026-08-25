# PERFORMANCE.md — Phase 13 Performance/Complexity Review

Reasoned from the actual code, not measured (no load-testing harness is
in scope for a one-day assessment). No code changes came out of this
phase — see "Implement only if needed" at the bottom.

## Complexity of each operation

| Operation | Cost | Notes |
|---|---|---|
| Enqueue | O(log n) heap push + 1 `fsync` | n = size of the Ready or Delayed heap it lands in. |
| Receive | O(ledger size) + O(log n) | `requeueExpiredLocked` scans **every** ledger entry on **every** call, not just the expired ones — see "first likely bottleneck" below. The heap pop itself is O(log n). |
| Delayed promotion | Amortized O(log n) per message | Each message is popped out of the Delayed heap exactly once over its lifetime, regardless of how many `promoteEligibleLocked` calls it takes to get there. |
| ACK lookup | O(1) + occasional O(log n) | Handle parse and ledger lookup are O(1). `removeReadyByIDLocked` (O(log n), via the heap's index map) only runs on the rare late-Ack-after-expiry path. Plus 1 `fsync`. |
| Startup recovery | O(total WAL records), two full passes | Grows linearly with WAL size forever — there's no compaction or checkpoint (`DESIGN_DECISIONS.md` #5 accepted this tradeoff explicitly). |
| fsync cost | 1 per ENQUEUE, 1 per ACK, 0 per RECEIVE | RECEIVE is ephemeral (`DESIGN_DECISIONS.md` #9), so it never touches the WAL. |
| Lock contention | Per-queue `q.mu` isolates in-memory ops between queues; `WAL.Append`'s internal mutex does **not** — it's manager-wide by design (CLAUDE.md's concurrency model) | Every queue's durable writes funnel through one lock held across a real disk `fsync`. |
| Memory growth | Unbounded | Ready/Delayed/ledger all live in memory for the process lifetime; nothing enforces max queue depth or evicts anything (no backpressure — explicitly deferred, see README's future work). |
| WAL growth | Unbounded, append-only forever | ACKed messages' ENQUEUE+ACK records are never removed; file size (and thus recovery time) only grows. |

## First likely bottleneck

**Per-operation `fsync` serialized through one manager-wide WAL lock.**
Every `Enqueue` and every `Ack`, across every queue in the process,
funnels through the same `w.mu` held for the duration of a real disk
`fsync` (~1–10ms on typical SSD, more on network-backed or spinning
storage). That's a hard throughput ceiling on the order of
100s–1000s of ops/sec for the *entire process*, regardless of queue
count, CPU, or goroutine concurrency — and it's the first thing that
would show up under any real write load, well before heap costs or
lock contention on individual queues matter.

A secondary, workload-dependent bottleneck: `requeueExpiredLocked`'s
O(ledger size) scan on every `Receive` call. This only matters once
the number of currently-tracked (in-flight or previously-delivered)
messages per queue gets large — at the scale this demo runs at it's
invisible, but it's the first thing that would need to change under a
workload with many thousands of concurrently in-flight messages per
queue.

WAL growth / replay-time-at-startup is a real but longer-horizon
concern — it doesn't affect steady-state throughput, only how long a
restart takes after the WAL has accumulated a very large history.

## Discussion

- **Per-message fsync** — current design. Simple, and makes "success
  means durable" unconditionally true and trivial to test
  deterministically (`DESIGN_DECISIONS.md`'s explicit tradeoff, agreed
  to in the Phase 0 review). Directly responsible for the bottleneck
  above.
- **Group commit** — batch several pending `Append`s into one `fsync`,
  trading a small added latency (waiting to batch) for much higher
  sustained throughput. Would require restructuring `WAL.Append`'s
  locking from "hold the lock across write+sync" into a batching/commit
  queue — an architectural change, not a tweak.
- **WAL segmentation** — rotate the single ever-growing file into
  segments, so old segments become eligible for deletion after
  compaction. Architectural change.
- **Compaction** — periodically rewrite the WAL to drop
  ACKed-and-superseded records, shrinking both file size and future
  replay time. Explicitly deferred already (`DESIGN_DECISIONS.md` #5).
  Architectural change.
- **Replay startup** — could be bounded by a periodic snapshot/checkpoint
  of queue state, replaying only records since the last checkpoint
  instead of the whole file. Architectural change.
- **Heap cost** — O(log n) via `container/heap` is already close to
  optimal for this structure; not a bottleneck at any scale this
  assessment runs at. No change warranted.
- **Per-queue isolation** — in-memory state is correctly isolated per
  queue (separate `q.mu` each), but WAL durability is not: one busy
  queue's fsync traffic adds latency to every other queue's Enqueue/Ack,
  since they share one WAL file and one lock. A per-queue WAL would
  restore isolation at the cost of real complexity (multiple file
  handles, no longer a single simple append-only log) — not warranted
  at this scale.

## Separated by risk

1. **Low-risk, semantics-preserving** (safe to do without touching
   observable behavior, but not needed at this scale — not implemented
   here): replace `requeueExpiredLocked`'s full-ledger scan with a
   secondary min-heap of ledger entries ordered by `leaseUntil`, so only
   actually-expired entries at the root are touched. Same lazy/logical
   expiry semantics (`DESIGN_DECISIONS.md` #14), just cheaper.
2. **Architectural changes** (would touch durability/locking model, real
   design work, out of scope for one day): group commit, WAL
   segmentation, compaction, checkpointed replay, per-queue WAL
   isolation.
3. **Premature optimizations to avoid**: sharding the WAL across
   multiple files before there's a measured need, hand-rolling heap
   internals instead of `container/heap`, adding a caching layer, or
   building an LSM-tree-style storage engine for a one-day assessment
   that explicitly scopes persistence to "implement directly with
   files."

## Implement only if needed

Nothing here blocks correctness or demo reliability at the scale this
assessment runs at (`make demo` completes in ~6 seconds against a
handful of messages). No code changes were made in this phase; items
1–2 above are candidates for README's "what I'd build next" (Phase 15).
