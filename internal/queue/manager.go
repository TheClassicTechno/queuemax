package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"queuemax/internal/storage"
)

// Manager owns the queue registry and the single shared WAL.
//
// CREATE_QUEUE, ENQUEUE, and ACK are all recorded through this one WAL
// (see DESIGN_DECISIONS.md and CLAUDE.md's concurrency model), so WAL
// append serialization is manager-wide rather than per-queue.
type Manager struct {
	mu     sync.RWMutex
	wal    *storage.WAL
	clock  Clock
	queues map[string]*Queue
}

var (
	// ErrQueueExists is returned by CreateQueue for a name already
	// registered. Not idempotent by design (DESIGN_DECISIONS.md #15):
	// silently accepting a possibly-different config under an existing
	// name would be a worse default than a loud error.
	ErrQueueExists = errors.New("queue: already exists")
	// ErrQueueNotFound is returned by Enqueue for an unregistered queue.
	ErrQueueNotFound = errors.New("queue: not found")
	// ErrInvalidQueueName enforces DESIGN_DECISIONS.md #13.
	ErrInvalidQueueName = errors.New("queue: invalid name")
	// ErrInvalidOrdering is returned for anything other than fifo/lifo.
	ErrInvalidOrdering = errors.New("queue: invalid ordering")
	// ErrInvalidDelay is returned for a negative delay.
	ErrInvalidDelay = errors.New("queue: delay must be >= 0")
	// ErrPayloadTooLarge enforces DESIGN_DECISIONS.md #12 at the queue
	// layer (HTTP will enforce it again at ingestion in Phase 9).
	ErrPayloadTooLarge = errors.New("queue: payload too large")
)

// queueNameRE enforces DESIGN_DECISIONS.md #13: non-empty, <=64 chars,
// charset [A-Za-z0-9_-].
var queueNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// createQueuePayload is the WAL payload shape for OpCreateQueue.
type createQueuePayload struct {
	Name                string
	Ordering            string
	PriorityEnabled     bool
	VisibilityTimeoutMS int64
}

// enqueuePayload is the WAL payload shape for OpEnqueue.
type enqueuePayload struct {
	QueueName   string
	ID          string
	Payload     []byte
	Priority    int
	Sequence    uint64
	EnqueuedAt  time.Time
	AvailableAt time.Time
}

// ackPayload is the WAL payload shape for OpAck.
type ackPayload struct {
	QueueName string
	MessageID string
}

// enqueueQueueRef decodes only the QueueName field out of an OpEnqueue
// record. json.Unmarshal skips decoding JSON fields with no matching
// struct field, so this is used in NewManager's first replay pass to check
// "does this ENQUEUE's queue exist" without allocating for the message
// payload it doesn't need yet.
type enqueueQueueRef struct {
	QueueName string
}

// NewManager opens a Manager backed by wal, replaying every valid durable
// record to reconstruct queues, messages, and per-queue sequence counters
// before returning (CLAUDE.md's "WAL -> validate -> replay -> rebuild
// state -> restore next sequence -> serve traffic"). A nil clock defaults
// to the real wall clock. Replay corruption that Phase 3's WAL.Replay
// classifies as real (non-tail) corruption is propagated as an error
// rather than silently booting (DESIGN_DECISIONS.md #11) — recovery must
// not invent state from data it cannot trust (crash-safety invariant #8).
func NewManager(wal *storage.WAL, clock Clock) (*Manager, error) {
	if clock == nil {
		clock = realClock{}
	}
	m := &Manager{
		wal:    wal,
		clock:  clock,
		queues: make(map[string]*Queue),
	}

	// Pass 1: rebuild the queue registry and the set of durably-ACKed
	// message IDs. ENQUEUE records are only checked for a valid queue
	// reference here (decoded via enqueueQueueRef, so the message payload
	// itself is never allocated) — a WAL of any real size would otherwise
	// force recovery to hold every enqueued message's full payload in
	// memory at once just to find out most of them are unaffected by any
	// ACK. Materialization happens in pass 2 below, one record at a time.
	acked := make(map[string]map[string]bool) // queueName -> messageID -> true

	_, err := wal.Replay(func(rec storage.Record) error {
		switch rec.Op {
		case storage.OpCreateQueue:
			var p createQueuePayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return fmt.Errorf("queue: decode CREATE_QUEUE: %w", err)
			}
			cfg := Config{
				Name:                p.Name,
				Ordering:            Ordering(p.Ordering),
				PriorityEnabled:     p.PriorityEnabled,
				VisibilityTimeoutMS: p.VisibilityTimeoutMS,
			}
			m.queues[cfg.Name] = NewQueue(cfg, clock)

		case storage.OpEnqueue:
			var p enqueueQueueRef
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return fmt.Errorf("queue: decode ENQUEUE: %w", err)
			}
			if _, ok := m.queues[p.QueueName]; !ok {
				// A valid ENQUEUE record for a queue that was never
				// validly CREATE_QUEUE'd cannot happen from this
				// Manager's own writes; treat it as untrusted rather
				// than inventing a queue for it (invariant #8).
				return fmt.Errorf("queue: ENQUEUE record for unknown queue %q", p.QueueName)
			}

		case storage.OpAck:
			var p ackPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return fmt.Errorf("queue: decode ACK: %w", err)
			}
			if _, ok := m.queues[p.QueueName]; !ok {
				return fmt.Errorf("queue: ACK record for unknown queue %q", p.QueueName)
			}
			if acked[p.QueueName] == nil {
				acked[p.QueueName] = make(map[string]bool)
			}
			acked[p.QueueName][p.MessageID] = true
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("queue: recover from WAL: %w", err)
	}

	// Pass 2: materialize every non-ACKed ENQUEUE straight into its
	// queue's Ready/Delayed structure, one record at a time — nothing
	// durable is ever buffered across records, only the small per-queue
	// nextSeq counters below.
	nextSeq := make(map[string]uint64)

	_, err = wal.Replay(func(rec storage.Record) error {
		if rec.Op != storage.OpEnqueue {
			return nil
		}
		var p enqueuePayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("queue: decode ENQUEUE: %w", err)
		}
		// Sequence numbers are never reused, ACKed or not
		// (DESIGN_DECISIONS.md #5) — advance nextSeq regardless of
		// whether this message gets materialized below.
		if p.Sequence+1 > nextSeq[p.QueueName] {
			nextSeq[p.QueueName] = p.Sequence + 1
		}
		if acked[p.QueueName][p.ID] {
			return nil // durably ACKed before the crash — stays gone
		}
		m.queues[p.QueueName].Enqueue(Message{
			ID:          p.ID,
			Payload:     p.Payload,
			Priority:    p.Priority,
			Sequence:    p.Sequence,
			EnqueuedAt:  p.EnqueuedAt,
			AvailableAt: p.AvailableAt,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("queue: recover from WAL (second pass): %w", err)
	}

	for name, next := range nextSeq {
		m.queues[name].restoreSequenceState(next)
	}

	return m, nil
}

// CreateQueue durably registers a new queue. Success means the
// CREATE_QUEUE record is appended and synced before the queue becomes
// visible to Enqueue (DESIGN_DECISIONS.md #6's durability principle
// applied to queue creation, not just messages).
func (m *Manager) CreateQueue(cfg Config) error {
	if !queueNameRE.MatchString(cfg.Name) {
		return ErrInvalidQueueName
	}
	if cfg.Ordering != OrderingFIFO && cfg.Ordering != OrderingLIFO {
		return ErrInvalidOrdering
	}
	if cfg.VisibilityTimeoutMS < 0 {
		return fmt.Errorf("queue: visibility_timeout_ms must be >= 0")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[cfg.Name]; exists {
		return ErrQueueExists
	}

	payload, err := json.Marshal(createQueuePayload{
		Name:                cfg.Name,
		Ordering:            string(cfg.Ordering),
		PriorityEnabled:     cfg.PriorityEnabled,
		VisibilityTimeoutMS: cfg.VisibilityTimeoutMS,
	})
	if err != nil {
		return fmt.Errorf("queue: encode CREATE_QUEUE: %w", err)
	}
	if err := m.wal.Append(storage.OpCreateQueue, payload); err != nil {
		return fmt.Errorf("queue: durable create: %w", err)
	}

	m.queues[cfg.Name] = NewQueue(cfg, m.clock)
	return nil
}

// Enqueue durably appends a new message to the named queue and places it
// into Ready or Delayed. The registry lookup is a brief read-lock, released
// before any disk I/O — an unrelated queue's CreateQueue or the WAL fsync
// itself is never blocked by another queue's Enqueue, or vice versa,
// beyond the WAL's own internal append serialization.
func (m *Manager) Enqueue(queueName string, payload []byte, priority int, delay time.Duration) (Message, error) {
	if len(payload) > storage.MaxPayloadBytes {
		return Message{}, ErrPayloadTooLarge
	}
	if delay < 0 {
		return Message{}, ErrInvalidDelay
	}

	m.mu.RLock()
	q, ok := m.queues[queueName]
	m.mu.RUnlock()
	if !ok {
		return Message{}, ErrQueueNotFound
	}

	return q.EnqueueDurable(payload, priority, delay, func(msg Message) error {
		enc, err := json.Marshal(enqueuePayload{
			QueueName:   queueName,
			ID:          msg.ID,
			Payload:     msg.Payload,
			Priority:    msg.Priority,
			Sequence:    msg.Sequence,
			EnqueuedAt:  msg.EnqueuedAt,
			AvailableAt: msg.AvailableAt,
		})
		if err != nil {
			return fmt.Errorf("queue: encode ENQUEUE: %w", err)
		}
		return m.wal.Append(storage.OpEnqueue, enc)
	})
}

// Receive leases the next eligible message from the named queue.
// RECEIVE is never written to the WAL (DESIGN_DECISIONS.md #9) — this is
// purely an in-memory lease. ok is false if the queue has nothing
// currently deliverable.
func (m *Manager) Receive(queueName string) (Delivery, bool, error) {
	m.mu.RLock()
	q, ok := m.queues[queueName]
	m.mu.RUnlock()
	if !ok {
		return Delivery{}, false, ErrQueueNotFound
	}

	d, ok := q.Receive()
	return d, ok, nil
}

// Ack durably completes a delivery on the named queue by receipt handle.
// Success means the ACK record is appended and synced before the message
// is removed from derived state (DESIGN_DECISIONS.md #7) — a successful
// ACK remains consumed after restart.
func (m *Manager) Ack(queueName, receiptHandle string) error {
	m.mu.RLock()
	q, ok := m.queues[queueName]
	m.mu.RUnlock()
	if !ok {
		return ErrQueueNotFound
	}

	return q.AckDurable(receiptHandle, func(messageID string) error {
		enc, err := json.Marshal(ackPayload{QueueName: queueName, MessageID: messageID})
		if err != nil {
			return fmt.Errorf("queue: encode ACK: %w", err)
		}
		return m.wal.Append(storage.OpAck, enc)
	})
}
