# FrankenQueue

A durable, composable HTTP job queue in Go — FIFO/LIFO × priority ×
delay, at-least-once delivery, and crash-safe restart, all built directly
on the standard library and a hand-rolled append-only WAL. No external
broker or database.

## Why this design

Ordering (FIFO/LIFO), priority, and delay aren't four separate queue
implementations here — they're independent knobs on one `Config`
(`ordering`, `priority_enabled`, `visibility_timeout_ms`), composed into
a single ordering function (`internal/queue/heap.go`). Durability follows
one architecture end to end:

```
HTTP API -> Queue Manager -> Queue -> Append-only WAL -> Disk
```

In-memory Ready/Delayed/in-flight state is *derived* — never the
source of truth. On restart: WAL -> validate -> replay valid records ->
rebuild state -> restore next sequence -> serve traffic. Disk is
authoritative; memory is never allowed to get durably ahead of it.

## Quick start

```bash
go build ./...
go run ./cmd/server        # starts the HTTP API on :8080, WAL at ./frankenqueue.wal
```

## `make demo`

```bash
make demo
```

Starts an in-process HTTP server against a fresh temp WAL and drives the
**real HTTP API** through all 8 required properties — queue creation,
FIFO, LIFO, priority order, delayed delivery, ACK, lease-expiry replay,
and restart durability — printing expected-vs-observed for each and
exiting non-zero on any mismatch. Self-cleaning (temp dir removed on
exit) and deterministic (no wall-clock races). "Restart" reopens the
same WAL file mid-process (`WAL.Close` → `storage.Open` →
`queue.NewManager`) rather than spawning a second OS process — it
exercises the real replay path, just without a process boundary.

## Queue configuration

```json
{
  "name": "jobs",
  "ordering": "fifo",
  "priority_enabled": true,
  "visibility_timeout_ms": 30000
}
```

Every message carries: a stable ID, payload, priority, monotonic
sequence number, `enqueued_at`, and `available_at`.

## Ordering semantics

| Mode | Primary key | Secondary key (tie-break) |
|---|---|---|
| FIFO | — | sequence ASC |
| LIFO | — | sequence DESC |
| Priority FIFO | priority DESC | sequence ASC |
| Priority LIFO | priority DESC | sequence DESC |

Timestamps are never used as a tie-breaker — only the monotonic sequence
number, which is unique and stable across restarts
(`DESIGN_DECISIONS.md` #1, #4).

## Delay semantics

Delay is an eligibility rule, not a fifth queue type. Each message
carries `available_at`; it is excluded from the Ready structure while
`available_at > now`, and becomes eligible under normal ordering once
`available_at <= now`. A delayed high-priority message never blocks an
immediate lower-priority one — they live in separate heaps (Ready,
Delayed), and Delayed is only ever consulted to promote eligible
messages into Ready, never to gate a Ready pop (`DESIGN_DECISIONS.md`
#3, `internal/queue/queue.go`'s `promoteEligibleLocked`).

## Architecture

```
      HTTP (net/http, stdlib only)
              |
        internal/httpapi          <- decode/encode + status mapping only
              |
     internal/queue.Manager       <- registry, WAL append serialization
              |
      internal/queue.Queue        <- per-queue Ready/Delayed heaps, lease ledger
              |
      internal/storage.WAL        <- framed append-only log, fsync-before-return
              |
             disk
```

Package boundaries: `internal/httpapi` never implements ordering or
durability logic — only validation/translation (CLAUDE.md). `internal/
queue` never does its own file I/O — it calls into `internal/storage`
through an injected `appendFn`. `internal/storage` knows nothing about
queues, messages, or ordering — only bytes, framing, and checksums.

## Durability contract

- **Enqueue**: success means the ENQUEUE record is appended and
  `fsync`ed *before* the message is placed into memory or the HTTP
  response is sent (`Queue.EnqueueDurable`).
- **ACK**: success means the ACK record is appended and `fsync`ed
  *before* the message is removed from in-memory state
  (`Queue.AckDurable`). A successful ACK stays consumed after restart.
- **Receive/lease**: never written to the WAL — deliberately ephemeral
  (see Replay/lease model below).

Memory is never allowed to be durably ahead of disk: every state
transition that must survive a restart goes WAL-append-then-fsync
*before* the in-memory mutation, for both ENQUEUE and ACK.

## WAL / recovery

On-disk record framing (`internal/storage/record.go`):

```
magic (4B "FQW1") | version (1B) | op (1B) | length (4B BE) | payload (N B) | checksum (4B BE, CRC32 IEEE)
```

Payload length is bounded (`MaxPayloadBytes`, 1 MiB) before any
allocation happens, so a corrupted or malicious length field can never
trigger unbounded memory allocation.

Recovery policy (`WAL.Replay`), by corruption type:

| Corruption | At the physical tail | Mid-log (more data follows) |
|---|---|---|
| Clean EOF | Replay stops, no error | — |
| Truncated header | Silently truncate, no error | (can't occur mid-log by definition) |
| Truncated body | Silently truncate, no error | (can't occur mid-log by definition) |
| Checksum mismatch | Silently truncate, no error | Hard error, wraps `ErrCorrupt` |
| Unreasonable length | Silently truncate, no error | Hard error, wraps `ErrCorrupt` |
| Bad magic | Hard error, wraps `ErrCorrupt` (no tail leniency — see below) | Hard error |
| Unsupported version | Hard error, wraps `ErrCorrupt` (no tail leniency) | Hard error |
| Unknown op | Hard error, wraps `ErrCorrupt` (no tail leniency) | Hard error |

This is a **valid-prefix + damaged-tail** policy: a torn tail (the
ordinary signature of a crash mid-write) is truncated silently, but
corruption that can't be explained by an ordinary crash is never
silently skipped. Bad magic, an unsupported version, and an unknown op
get no tail leniency at all, even at the physical end of the file — a
crash truncates bytes, it doesn't produce internally-consistent
records with impossible metadata, so any of those three is treated as
real corruption regardless of position (`CRASH_AUDIT.md`'s Phase 12
finding — an earlier version of this code silently dropped unknown-op
records; fixed and regression-tested, see `OpType.valid` and
`TestReplayUnknownOpRejectedMidLog`).

`queue.NewManager` replays the WAL in two passes: pass 1 rebuilds the
queue registry and the set of durably-ACKed message IDs; pass 2
materializes every non-ACKed ENQUEUE into its queue's Ready/Delayed
structure and recomputes each queue's next sequence number as
`max(seen) + 1`, one record at a time, never buffering more than one
record's payload in memory.

## Concurrency

- `Manager.mu` (`RWMutex`): protects the queue registry map. Held only
  as a brief read-lock for lookups (`RLock`/`RUnlock` before any disk
  I/O) or a write-lock during `CreateQueue`; never held during a WAL
  `fsync`.
- `Queue.mu` (`Mutex`, per queue): protects that queue's Ready heap,
  Delayed heap, lease ledger, and sequence counter together. One lock
  per queue — an unrelated queue is never blocked by this one.
- `WAL`'s internal mutex: serializes every `Append` (and thus every
  `fsync`) across the *entire* process, deliberately manager-wide
  rather than per-queue (CLAUDE.md's concurrency model;
  `PERFORMANCE.md`'s "first likely bottleneck").

Lock ordering is `Manager.mu` (brief) → `Queue.mu` (held across the WAL
append) — never the reverse, so there's no inversion risk between
queue lookup and a durable write. `Queue.mu` is held across `appendFn`
(the WAL write) deliberately: it serializes sequence allocation with
the durable record describing it, and keeps a concurrent Dequeue from
ever observing a message whose WAL record isn't synced yet.

Lease expiry is **lazy and logical**, never an eager background
mutation: `requeueExpiredLocked` only runs under `Queue.mu`, invoked
from `Receive`/`Ack` themselves — so ACK and expiry are structurally
serialized through the same lock and can never race outside it
(`DESIGN_DECISIONS.md` #14; `TestConcurrentAckVsTimeout` runs this
under `-race`). No timers, no goroutine leaks from a reaper — there is
no reaper.

## Receive / ACK / replay model

At-least-once delivery: READY → IN_FLIGHT → ACKED, IN_FLIGHT → READY on
lease expiry. Every delivery gets a fresh receipt handle
(`messageID:queueEpoch:attemptSeq`) distinct from the stable message
ID; a stale handle (superseded by redelivery, already ACKed, or from
before a restart) is rejected (`ErrStaleReceiptHandle`), never silently
accepted.

**Leases are ephemeral, not durable** — only ENQUEUE and ACK go through
the WAL (`DESIGN_DECISIONS.md` #9). This means a process restart
immediately makes every currently in-flight, unACKed message eligible
again, even one a consumer was about to ACK. **Consumers must be
idempotent**, keyed on the stable message ID — this is at-least-once
delivery, and external side effects are explicitly **not**
exactly-once: a consumer can perform its effect, crash before ACKing,
and the message gets redelivered. See `ANSWERS.md` for the full
writeup, and `TestRestartWhileInFlightMakesMessageEligibleAgain` /
`TestAckSurvivesRestart` for the behavior under test.

## HTTP API

| Method & path | Purpose | Success | Failure |
|---|---|---|---|
| `GET /health` | Liveness check | 200 | — |
| `POST /queues` | Create a queue | 201 | 400 invalid name/ordering, 409 already exists |
| `GET /queues` | List queues, sorted by name | 200 | — |
| `POST /queues/{name}/messages` | Enqueue (base64 `payload`, `priority`, `delay_ms`) | 201 | 400 malformed/bad base64/negative delay, 404 unknown queue, 413 oversized payload |
| `POST /queues/{name}/messages/receive` | Lease the next eligible message | 200 with delivery, 204 if nothing eligible | 404 unknown queue |
| `POST /queues/{name}/messages/{receiptHandle}/ack` | Acknowledge a delivery | 200 | 404 unknown queue, 409 stale/invalid handle |
| `GET /queues/{name}/stats` | Ready/Delayed/InFlight counts | 200 | 404 unknown queue |

Message payloads travel as base64 over JSON (arbitrary bytes, not
assumed UTF-8). Request bodies are capped with `http.MaxBytesReader`
before decoding, ahead of the queue layer's own `MaxPayloadBytes`
check, so an oversized body can't force unbounded allocation. Handlers
only decode, call `*queue.Manager`, and translate its typed errors to
status codes — no ordering or durability logic lives in
`internal/httpapi`.

## Tests

63 tests across three packages (`go test ./...`), including:

- Ordering: FIFO, LIFO, priority FIFO, priority LIFO, equal-priority
  ties, near-identical delayed timestamps
- Delay: no early delivery, delayed-high-priority doesn't block
  immediate-low-priority, multiple delayed items becoming ready
  incrementally
- Persistence: messages and exact order survive restart, delay survives
  restart, sequence not reused after restart or after ACK+restart
- Replay: receive→ACK, no-ACK→lease-expiry→redelivery with a stable ID
  and a new receipt handle, stale handle rejected after redelivery,
  restart with an in-flight message makes it eligible again, ACK
  survives restart, duplicate ACK rejected, stale ACK can't consume a
  redelivery
- WAL corruption: truncated header/body (tail), checksum mismatch
  (tail and mid-log), unreasonable length (tail and mid-log), bad
  magic, unsupported version, unknown op (mid-log), valid-prefix +
  damaged-tail via real truncation, random-byte garbage, double replay
  producing stable state
- HTTP: every endpoint's success and validation-error paths
- Concurrency: concurrent enqueue (unique sequences), concurrent
  receive (each message delivered exactly once), concurrent ACK vs.
  lease-timeout race, a bounded concurrent producers/consumers stress
  test

```bash
go test ./...
go test -race ./...
```

Tests use an injectable `Clock` (`ManualClock`) wherever delay/timeout
behavior is under test, so there are no flaky-sleep races against wall
time.

## Crash behavior

See `CRASH_AUDIT.md` for the full crash-point-by-crash-point table
(every step of CREATE_QUEUE, ENQUEUE, RECEIVE, ACK, lease expiry, and
delayed promotion). Summary: every crash point is safe by construction
— WAL-before-memory for durable operations, and structurally ephemeral
(no I/O at all) for RECEIVE and lease/delay bookkeeping. The one real
bug this audit process found (an unknown WAL op silently ignored
instead of rejected) is fixed and regression-tested; see the WAL/
recovery section above.

## Known limitations

- **fsync-per-operation**: every ENQUEUE and ACK blocks on a real disk
  `fsync`, serialized through one manager-wide WAL lock — this is the
  first throughput bottleneck under real write load
  (`PERFORMANCE.md`).
- **Unbounded WAL and memory growth**: no compaction, no segmentation,
  no queue-depth limits — the WAL only grows, and nothing bounds how
  much a queue can hold.
- **No DLQ / max-attempts**: a permanently-failing message is
  redelivered forever.
- **Ephemeral leases**: every restart redelivers every currently
  in-flight message, even one a consumer was about to ACK — an
  accepted tradeoff for a simpler WAL and lock model
  (`DESIGN_DECISIONS.md` #9), but it means consumers must be
  idempotent.
- **No producer idempotency key**: a producer that times out and
  retries an ENQUEUE can create a duplicate durable message
  (`CRASH_AUDIT.md`'s findings).
- **No graceful shutdown, no NACK, no long-polling, no batch APIs, no
  observability beyond point-in-time stats.**
- **Out of scope entirely**: disk bit-rot beyond a torn tail,
  multi-process or multi-node coordination, network partitions,
  malicious clients beyond basic input validation.

## Additional assessment questions

Full answers in `ANSWERS.md`: replay/at-least-once model, the Pub/Sub
refactor (topic log, independent per-subscription cursors, retention,
optional consumer groups), prioritized future work, and why this project
exists alongside (not instead of) SQS/RabbitMQ/Pulsar.

## Future work

See `PERFORMANCE.md` for the full performance/complexity review and
`ANSWERS.md` #3 for the prioritized "more time" list: group commit,
WAL segmentation + compaction, DLQ/max-attempts, backpressure,
observability, graceful shutdown + batch APIs.
