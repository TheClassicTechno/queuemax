# CTO_AUDIT.md — Phase 17 CTO-Style Final Audit

Written as an adversarial, diagnostic review of this repo as it stands
after Phase 16, not a self-congratulatory summary. Every finding below
was checked against actual code in this session, not assumed.

## Reject-level issues

None found. Nothing here would make me reject this submission outright
— core durability, ordering, and replay guarantees are implemented,
tested (including `-race`), and match their documentation.

## High-value fixes

1. **The real HTTP server has no `ReadTimeout`/`WriteTimeout`.**
   `cmd/server/main.go` calls `http.ListenAndServe(":8080", handler)`
   directly — a raw `net/http` server with unbounded per-connection
   timeouts. A client that opens a connection and sends nothing (or
   sends slowly) can hold a handler goroutine indefinitely; at scale
   this is a real resource-exhaustion vector (a slowloris-style issue),
   not a theoretical one. `cmd/demo/main.go`'s in-process test server
   has the same gap, lower stakes there since it's local-only. **Not
   fixed in this audit** — it's a real behavior change (timeout values
   are a judgment call, and getting them wrong breaks legitimate slow
   clients or long-polling) that deserves its own review, not a
   drive-by edit during an audit pass.
2. **No idempotency key on ENQUEUE.** Already documented
   (`CRASH_AUDIT.md`, `ANSWERS.md` #3) — a producer that times out and
   retries can create a duplicate durable message. Correctly scoped as
   "more time" work, not silently missing from the record.
3. **Priority has no enforced numeric range.** See "Missing proof"
   below — not a bug, but worth fixing the documentation/expectation
   mismatch cheaply.

## Overengineering removed during this audit

- **`GET /queues` sorted its response twice.** `Manager.ListQueues`
  (`internal/queue/manager.go`) already returns queues sorted by name;
  `handler.listQueues` (`internal/httpapi/handlers.go`) redundantly
  re-sorted the already-sorted slice. Harmless (both sorts use the same
  key, so the result was always correct) but pure waste — removed the
  second sort and its now-unused `sort` import in this commit. No
  behavior change; `TestListQueues` (already asserting sorted order)
  still passes.

No other overengineering found. The composable-heap design
(`internal/queue/heap.go`), the two-pass WAL replay, and the epoch-in-
receipt-handle scheme are all doing real work, not speculative
abstraction — each is directly load-bearing for a specific tested
invariant.

## Missing proof

- **"Priority is a plain bounded int" (`DESIGN_DECISIONS.md` #2) has no
  corresponding runtime check.** Neither `Manager.CreateQueue` nor
  `Manager.Enqueue` enforces any minimum or maximum priority value —
  "bounded" in that entry means "a fixed-width Go `int`," not "clamped
  to an application-level range." This is consistent, not a bug, but
  the word choice invites a reviewer to expect a range check that
  doesn't exist. No test proves a negative or extreme (`math.MaxInt`)
  priority is accepted or rejected either way, because no behavior is
  actually specified for that case beyond "it's just an int."
- **No test proves `Manager.CreateQueue`'s config-mismatch behavior**
  beyond "a duplicate name is rejected" (`DESIGN_DECISIONS.md` #15,
  `TestCreateQueueDuplicateRejected`) — there's no test specifically
  confirming that a duplicate-name request with a *different* ordering/
  priority/timeout is rejected the same way as an identical one (the
  code path is identical either way since the name check happens before
  any config comparison, so this is very likely fine, but it isn't
  explicitly proven by a named test).
- **No load or throughput measurement exists for the `PERFORMANCE.md`
  claims.** The "first likely bottleneck" analysis (per-op fsync
  through one manager-wide lock) is reasoned from code inspection, not
  benchmarked — correctly caveated in that document as such, not
  presented as measured fact.

## 20 hard interview questions

1. Walk me through what happens, byte by byte, when `Append` writes a
   record — where exactly does `fsync` happen relative to the in-memory
   state update, and why does the order matter?
2. Why not just use SQLite for persistence here? What would you lose,
   and what would you gain?
3. Explain your WAL's checksum — what does it cover, and what would a
   checksum that only covered the payload (not version+op+length) fail
   to catch?
4. Walk me through `WAL.Append`'s short-write handling. What's the one
   line that would let a torn write through undetected if it were
   missing?
5. Describe exactly what happens if the process crashes between the
   `Write` and the `Sync` call inside `Append`.
6. Why does `Replay` treat a checksum mismatch at the physical tail
   differently from one in the middle of the log? What crash scenario
   justifies that distinction?
7. What is `hasMoreData` for, and why can't `Replay` just check
   `len(remaining bytes)` up front instead?
8. Sequence numbers: walk me through exactly how `nextSeq` is recovered
   after a restart, and why `max(seen) + 1` rather than "the last
   ENQUEUE's sequence + 1."
9. Why is it safe for a sequence number to be "wasted" (never appear in
   any durable record) if a crash happens between allocating it and
   appending its ENQUEUE record?
10. How does delay composition with priority actually work — walk me
    through why a delayed high-priority message can never block an
    immediate low-priority one, in terms of which heap holds what.
11. What is at-least-once delivery, precisely — and what concrete
    scenario in this codebase demonstrates it (not "redelivery
    happens," but the actual sequence of calls)?
12. Why is exactly-once delivery to an external side effect
    impossible in general, not just hard? Where's the fundamental
    problem?
13. Walk me through the receipt handle format and explain what each of
    its three components independently defends against.
14. Why was the epoch component added to the receipt handle after
    initial implementation? What specific test caught the gap it
    closes?
15. Describe the race between a late ACK and a lease-expiry check for
    the same message. Which one wins, and what specific lock makes that
    outcome deterministic rather than a coin flip?
16. What would break if `requeueExpiredLocked` ran on a separate timer
    goroutine instead of being invoked lazily from `Receive`/`Ack`?
17. If a consumer receives a message, restarts the whole server before
    ACKing, and gets it redelivered — is that a bug? Defend your
    answer using this project's actual documented durability contract.
18. What's the first thing that would start to slow down under
    sustained write load in this system, and why — be specific about
    which lock and which syscall.
19. What would you have to change — not add, *change* — to turn this
    single-consumer-per-message queue into a Pub/Sub system where every
    subscriber sees every message?
20. If you were given one more day, what's the single change you'd
    make first, and why that one over the other candidates in
    `ANSWERS.md` #3?
