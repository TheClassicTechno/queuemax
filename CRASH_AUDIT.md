# CRASH_AUDIT.md — Phase 11 Crash/Fault-Injection Audit

Assumes process death (not disk/OS failure — see `DESIGN_DECISIONS.md`'s
scope) at every meaningful step of CREATE_QUEUE, ENQUEUE, RECEIVE, ACK,
lease expiry, and delayed promotion. `storage.WAL.Append` (`internal/storage/wal.go`)
returns `nil` only after `Write` (with a short-write check) and `Sync`
both succeed — every row below is checked against that literal contract,
not an assumed one.

| Operation | Crash Point | Durable State | Memory Lost | Recovery State | Safe? | Why |
|---|---|---|---|---|---|---|
| CREATE_QUEUE | Before `WAL.Append` returns (mid-write or torn tail) | None (or a truncated tail, discarded by `Replay`) | In-memory registry entry never existed | Queue does not exist | Safe | Replay never observes the record; a client retry creates the queue cleanly. |
| CREATE_QUEUE | After `WAL.Append` returns, before `m.queues[name]` is assigned / before the HTTP response is sent | CREATE_QUEUE record | Registry entry (moot — process is dead) | Queue exists, empty | Safe | Replay reconstructs the registry from the durable record. The client, having gotten no response, may retry `CreateQueue` and gets `ErrQueueExists` (409) — which *is* its confirmation the original attempt succeeded (`DESIGN_DECISIONS.md` #15). |
| ENQUEUE | Before `EnqueueDurable`'s `appendFn` (`WAL.Append`) returns | None (or a truncated tail) | `q.nextSeq++` (discarded) | Message absent; next real ENQUEUE reuses that sequence value | Safe | The sequence number was never durable, never returned to a caller, and never observed by any other party — reusing it is indistinguishable from it never having been allocated. Not a violation of invariant #9, which protects sequence numbers that appear in durable state. |
| ENQUEUE | After `WAL.Append` returns, before `heap.Push` / before the HTTP response | ENQUEUE record | Ready/Delayed placement (moot) | Message present, placed correctly per `available_at` vs. the *restart-time* clock | Safe | Replay's pass 2 places it exactly as `EnqueueDurable` would have. A client retry after a timeout produces a harmless duplicate *message* (no idempotency key on Enqueue by design — documented at-least-once-producer limitation, not a crash-safety bug). |
| RECEIVE | Any point | Never touches the WAL | Entire ledger/lease state | Message reverts to Ready, as if never received | Safe by construction | Ephemeral leases are the explicit design choice (`DESIGN_DECISIONS.md` #9) — a crash here is indistinguishable from an ordinary lease expiry. |
| ACK | Before `AckDurable`'s `appendFn` (`WAL.Append`) returns | None (or a truncated tail) | Ledger entry (moot) | Message still in-flight or Ready per its lease state; will be redelivered | Safe | At-least-once holds only if the consumer treats its side effect as uncommitted until Ack durably succeeds — documented consumer contract. |
| ACK | After `WAL.Append` returns, before `completeAckLocked` / before the HTTP response | ACK record | Ledger removal (moot) | Message durably excluded from replay (pass 2's `acked` filter) | Safe | The message never resurfaces. A client retry of the same Ack gets `ErrStaleReceiptHandle` (409) since the ledger rebuilds empty on restart — ambiguous to the client, but never double-processed and never lost. |
| Lease expiry (`requeueExpiredLocked`) | Any point | No I/O — pure in-memory, evaluated lazily under `q.mu` | n/a; re-derived from `leaseUntil` timestamps on the next `Receive`/`Ack` | Consistent | Safe | No I/O inside the critical section, so there is no partial-effect crash point to land on. |
| Delayed promotion (`promoteEligibleLocked`) | Any point | No I/O — pure in-memory | n/a; re-derived from each message's durable `available_at` vs. the restart-time clock | Consistent | Safe | Promotion is a pure function of already-durable state, recomputed fresh on every restart. |

## Findings

Every crash point above is safe by the existing design, not by luck.
Two gaps in test coverage (Phase 11/12) turned up real work:

- **Double replay** — calling `Replay` a second time with no new writes
  in between must reproduce identical state, not duplicate or drop
  anything. Was untested; see `TestDoubleRestartProducesStableState` in
  `internal/queue/crash_audit_test.go`.
- **Unknown op (Phase 12, a real bug, fixed)** — a record with correct
  magic, version, length, and a checksum computed over its own
  (unrecognized) op byte was silently dropped by `Replay` and by
  `Manager`'s replay switch, instead of being rejected. This can't be
  explained by an ordinary crash (a crash truncates bytes; it doesn't
  produce internally-consistent records for op values nothing ever
  wrote), so `Replay` now rejects any unrecognized op unconditionally —
  no tail-truncation leniency, unlike length/checksum errors. See
  `OpType.valid` in `internal/storage/record.go` and
  `TestReplayUnknownOpRejectedMidLog` in `internal/storage/wal_test.go`.

The one accepted, non-bug limitation this audit surfaces explicitly:
ENQUEUE has no idempotency key, so a producer that times out waiting
for a response and retries can create a duplicate durable message.
This is the same "at-least-once, not exactly-once" tradeoff already
documented for consumers (`DESIGN_DECISIONS.md` #8), just on the
producer side — out of scope to fix for this assessment, worth a line
in README's "known limitations."
