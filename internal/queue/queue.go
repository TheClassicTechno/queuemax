package queue

import (
	"container/heap"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ordering defines how a queue's Ready messages are ordered.
type Ordering string

const (
	OrderingFIFO Ordering = "fifo"
	OrderingLIFO Ordering = "lifo"
)

// Config is the composable configuration for a single queue.
type Config struct {
	Name                string
	Ordering            Ordering
	PriorityEnabled     bool
	VisibilityTimeoutMS int64
}

// Queue holds the in-memory derived state for one named queue: a Ready
// heap ordered per its Config, a Delayed heap holding messages not yet
// eligible, and a ledger of ephemeral in-flight leases (DESIGN_DECISIONS.md
// #9 — never persisted).
type Queue struct {
	mu      sync.Mutex
	config  Config
	clock   Clock
	ready   readyHeap
	delayed delayedHeap
	nextSeq uint64
	ledger  map[string]*ledgerEntry
	// epoch is a random value generated fresh on every construction
	// (i.e. every process start — a Queue is never reused across a
	// restart). It's folded into every receipt handle this Queue mints
	// so a handle from before a restart can never collide with one
	// minted after, even though the ephemeral, unpersisted attemptSeq
	// counter restarts from zero both times (DESIGN_DECISIONS.md #9/#10).
	epoch uint64
}

// ledgerEntry tracks the current delivery attempt for one message ID.
// Entries are created on first delivery and deleted only on a successful
// Ack; they are never persisted, so a restart wipes them, which is exactly
// what makes every pre-restart receipt handle reject itself for free
// (DESIGN_DECISIONS.md #10).
type ledgerEntry struct {
	msg        Message
	attemptSeq uint64
	leased     bool
	leaseUntil time.Time
}

// ErrStaleReceiptHandle is returned by Ack for a handle that does not match
// a message's current delivery attempt — either it was never delivered,
// was already Acked, or has been superseded by a newer delivery
// (DESIGN_DECISIONS.md #14).
var ErrStaleReceiptHandle = errors.New("queue: stale or invalid receipt handle")

// NewQueue constructs a Queue for the given configuration. A nil clock
// defaults to the real wall clock.
func NewQueue(config Config, clock Clock) *Queue {
	if clock == nil {
		clock = realClock{}
	}
	q := &Queue{
		config: config,
		clock:  clock,
		ledger: make(map[string]*ledgerEntry),
		epoch:  rand.Uint64(),
	}
	q.ready.index = make(map[string]int)
	q.ready.less = lessFuncFor(config)
	heap.Init(&q.ready)
	heap.Init(&q.delayed)
	return q
}

// Enqueue inserts a fully-formed message, routing it to Ready or Delayed
// by comparing AvailableAt to the current time (DESIGN_DECISIONS.md #3).
// ID/sequence allocation is the caller's responsibility (the Manager, from
// Phase 5 onward) — this method only places the message.
func (q *Queue) Enqueue(msg Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if msg.AvailableAt.After(q.clock.Now()) {
		heap.Push(&q.delayed, msg)
		return
	}
	heap.Push(&q.ready, msg)
}

// Dequeue promotes any now-eligible delayed messages into Ready, then pops
// and returns the next message per the queue's ordering rules. ok is false
// if nothing is currently eligible — this is the "no early delivery"
// guarantee, and it means a delayed message can never block on a ready one
// or vice versa.
func (q *Queue) Dequeue() (Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.promoteEligibleLocked()

	if q.ready.Len() == 0 {
		return Message{}, false
	}
	return heap.Pop(&q.ready).(Message), true
}

func (q *Queue) promoteEligibleLocked() {
	now := q.clock.Now()
	for {
		next, ok := q.delayed.Peek()
		if !ok || next.AvailableAt.After(now) {
			return
		}
		heap.Push(&q.ready, heap.Pop(&q.delayed).(Message))
	}
}

// ReadyLen reports how many messages are currently Ready, after promoting
// any delayed messages that have become eligible.
func (q *Queue) ReadyLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteEligibleLocked()
	return q.ready.Len()
}

// DelayedLen reports how many messages are still ineligible, after
// promoting any delayed messages that have become eligible.
func (q *Queue) DelayedLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteEligibleLocked()
	return q.delayed.Len()
}

// InFlightLen reports how many messages are currently leased to a
// consumer and not yet Acked or lazily requeued by lease expiry.
func (q *Queue) InFlightLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, entry := range q.ledger {
		if entry.leased {
			n++
		}
	}
	return n
}

// Config returns a copy of this queue's configuration.
func (q *Queue) Config() Config {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.config
}

// EnqueueDurable allocates a stable ID and monotonic sequence for a new
// message and invokes appendFn — wired by the Manager to a WAL
// append+fsync — before placing the message into Ready or Delayed. The
// per-queue lock is held across appendFn deliberately: it serializes
// sequence allocation with the durable write it describes, and it keeps a
// concurrent Dequeue from ever observing a message whose WAL record isn't
// synced yet (crash-safety invariant #1). A failed appendFn leaves the
// sequence counter advanced (an accepted, harmless gap) but the message is
// never placed into memory.
func (q *Queue) EnqueueDurable(payload []byte, priority int, delay time.Duration, appendFn func(Message) error) (Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()
	seq := q.nextSeq
	q.nextSeq++

	msg := Message{
		ID:          fmt.Sprintf("%s-%d", q.config.Name, seq),
		Payload:     payload,
		Priority:    priority,
		Sequence:    seq,
		EnqueuedAt:  now,
		AvailableAt: now.Add(delay),
	}

	if err := appendFn(msg); err != nil {
		return Message{}, err
	}

	if msg.AvailableAt.After(now) {
		heap.Push(&q.delayed, msg)
	} else {
		heap.Push(&q.ready, msg)
	}
	return msg, nil
}

// Receive leases the next eligible message to a consumer. It first
// lazily requeues any lease that has expired since the last call — no
// background timer ever mutates queue state out of band
// (DESIGN_DECISIONS.md #14) — then pops the next message per the queue's
// ordering and hands out a receipt handle unique to this delivery attempt.
// ok is false if nothing is currently deliverable.
func (q *Queue) Receive() (Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()
	q.promoteEligibleLocked()
	q.requeueExpiredLocked(now)

	if q.ready.Len() == 0 {
		return Delivery{}, false
	}

	msg := heap.Pop(&q.ready).(Message)

	entry, ok := q.ledger[msg.ID]
	if !ok {
		entry = &ledgerEntry{msg: msg}
		q.ledger[msg.ID] = entry
	}
	entry.attemptSeq++
	entry.leased = true
	entry.leaseUntil = now.Add(time.Duration(q.config.VisibilityTimeoutMS) * time.Millisecond)

	return Delivery{
		Message:       msg,
		ReceiptHandle: receiptHandle(msg.ID, q.epoch, entry.attemptSeq),
		LeaseUntil:    entry.leaseUntil,
	}, true
}

// requeueExpiredLocked pushes every leased message whose lease has expired
// back into Ready by its original Sequence, without touching attemptSeq —
// bumping only happens at actual redelivery, so a late-but-still-current
// Ack still wins over a bare expiry (DESIGN_DECISIONS.md #14).
func (q *Queue) requeueExpiredLocked(now time.Time) {
	for _, entry := range q.ledger {
		if entry.leased && !entry.leaseUntil.After(now) {
			entry.leased = false
			heap.Push(&q.ready, entry.msg)
		}
	}
}

// Ack completes the delivery identified by receiptHandle. A handle is
// honored only if it names the message's current delivery attempt; this
// covers both an ordinary stale/duplicate Ack and a redelivered message's
// old handle. A late Ack that still matches the current attempt succeeds
// even if its lease already expired — if the message was lazily requeued
// into Ready but nothing has redelivered it yet, it is pulled back out
// here (DESIGN_DECISIONS.md #14).
func (q *Queue) Ack(receiptHandle string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	id, err := q.validateReceiptHandleLocked(receiptHandle)
	if err != nil {
		return err
	}
	q.completeAckLocked(id)
	return nil
}

// AckDurable validates receiptHandle exactly like Ack, then invokes
// appendFn — wired by the Manager to a WAL append+fsync of the ACK record
// — before removing any in-memory trace of the message. Validation happens
// before appendFn (a stale/wrong handle must never touch the WAL at all);
// the in-memory removal happens only after appendFn succeeds, so a failed
// or crashed append leaves the message exactly as it was — still in-flight
// or sitting in Ready after a lazy expiry — and it will simply be
// redelivered later. That is at-least-once working as intended, never a
// silently-dropped ACK.
func (q *Queue) AckDurable(receiptHandle string, appendFn func(messageID string) error) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	id, err := q.validateReceiptHandleLocked(receiptHandle)
	if err != nil {
		return err
	}
	if err := appendFn(id); err != nil {
		return err
	}
	q.completeAckLocked(id)
	return nil
}

// validateReceiptHandleLocked checks receiptHandle against this queue's
// epoch and the message's current delivery attempt, per
// DESIGN_DECISIONS.md #14. Must be called with q.mu held.
func (q *Queue) validateReceiptHandleLocked(receiptHandle string) (id string, err error) {
	id, epoch, seq, ok := parseReceiptHandle(receiptHandle)
	if !ok || epoch != q.epoch {
		return "", ErrStaleReceiptHandle
	}
	entry, ok := q.ledger[id]
	if !ok || seq != entry.attemptSeq {
		return "", ErrStaleReceiptHandle
	}
	return id, nil
}

// completeAckLocked removes all in-memory trace of message id: if its
// lease had already lazily expired back into Ready (the late-Ack-wins
// case, DESIGN_DECISIONS.md #14), it's pulled back out from there too.
// Must be called with q.mu held.
func (q *Queue) completeAckLocked(id string) {
	if entry := q.ledger[id]; entry != nil && !entry.leased {
		q.removeReadyByIDLocked(id)
	}
	delete(q.ledger, id)
}

// removeReadyByIDLocked removes the message with the given ID from the
// Ready heap, if present, in O(log n): an O(1) index lookup followed by
// heap.Remove's O(log n) reshuffle. Reached only on the rare late-Ack-
// after-expiry path, never on the hot Receive/Ack path.
func (q *Queue) removeReadyByIDLocked(id string) {
	if idx, ok := q.ready.index[id]; ok {
		heap.Remove(&q.ready, idx)
	}
}

func receiptHandle(id string, epoch, attemptSeq uint64) string {
	return id + ":" + strconv.FormatUint(epoch, 16) + ":" + strconv.FormatUint(attemptSeq, 10)
}

// parseReceiptHandle splits a handle into id:epoch:attemptSeq. A queue
// name's charset (DESIGN_DECISIONS.md #13) excludes ':', and the message
// ID is "<queueName>-<sequence>", so id itself is always colon-free —
// splitting on ':' unambiguously yields exactly 3 parts for a well-formed
// handle.
func parseReceiptHandle(h string) (id string, epoch, seq uint64, ok bool) {
	parts := strings.Split(h, ":")
	if len(parts) != 3 {
		return "", 0, 0, false
	}
	epoch, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return "", 0, 0, false
	}
	seq, err = strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return "", 0, 0, false
	}
	return parts[0], epoch, seq, true
}

// restoreSequenceState raises nextSeq to at least next, per
// DESIGN_DECISIONS.md #5 ("recovered as max(seen)+1 during WAL replay").
// Called once by the Manager after replaying all ENQUEUE records for this
// queue.
func (q *Queue) restoreSequenceState(next uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if next > q.nextSeq {
		q.nextSeq = next
	}
}
