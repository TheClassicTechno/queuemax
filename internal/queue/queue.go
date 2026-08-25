package queue

import (
	"container/heap"
	"sync"
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
// heap ordered per its Config, and a Delayed heap holding messages not yet
// eligible.
type Queue struct {
	mu      sync.Mutex
	config  Config
	clock   Clock
	ready   readyHeap
	delayed delayedHeap
	nextSeq uint64
}

// NewQueue constructs a Queue for the given configuration. A nil clock
// defaults to the real wall clock.
func NewQueue(config Config, clock Clock) *Queue {
	if clock == nil {
		clock = realClock{}
	}
	q := &Queue{
		config: config,
		clock:  clock,
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
