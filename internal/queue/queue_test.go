package queue

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func drain(t *testing.T, q *Queue, n int) []Message {
	t.Helper()
	out := make([]Message, 0, n)
	for i := 0; i < n; i++ {
		msg, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue: got ok=false at index %d, want a message", i)
		}
		out = append(out, msg)
	}
	return out
}

func seqs(msgs []Message) []uint64 {
	out := make([]uint64, len(msgs))
	for i, m := range msgs {
		out[i] = m.Sequence
	}
	return out
}

func assertSeqs(t *testing.T, got []Message, want []uint64) {
	t.Helper()
	gotSeqs := seqs(got)
	if len(gotSeqs) != len(want) {
		t.Fatalf("got %d messages, want %d", len(gotSeqs), len(want))
	}
	for i := range want {
		if gotSeqs[i] != want[i] {
			t.Fatalf("sequence order = %v, want %v", gotSeqs, want)
		}
	}
}

func TestFIFOOrder(t *testing.T) {
	q := NewQueue(Config{Ordering: OrderingFIFO}, NewManualClock(t0))
	for _, seq := range []uint64{5, 1, 3} {
		q.Enqueue(Message{Sequence: seq, AvailableAt: t0})
	}
	got := drain(t, q, 3)
	assertSeqs(t, got, []uint64{1, 3, 5})
}

func TestLIFOOrder(t *testing.T) {
	q := NewQueue(Config{Ordering: OrderingLIFO}, NewManualClock(t0))
	for _, seq := range []uint64{5, 1, 3} {
		q.Enqueue(Message{Sequence: seq, AvailableAt: t0})
	}
	got := drain(t, q, 3)
	assertSeqs(t, got, []uint64{5, 3, 1})
}

func TestPriorityFIFOOrderWithTies(t *testing.T) {
	q := NewQueue(Config{Ordering: OrderingFIFO, PriorityEnabled: true}, NewManualClock(t0))
	q.Enqueue(Message{Sequence: 10, Priority: 5, AvailableAt: t0})
	q.Enqueue(Message{Sequence: 1, Priority: 10, AvailableAt: t0})
	q.Enqueue(Message{Sequence: 2, Priority: 10, AvailableAt: t0})
	q.Enqueue(Message{Sequence: 3, Priority: 1, AvailableAt: t0})

	got := drain(t, q, 4)
	// priority DESC, then seq ASC within a priority tie.
	assertSeqs(t, got, []uint64{1, 2, 10, 3})
}

func TestPriorityLIFOOrderWithTies(t *testing.T) {
	q := NewQueue(Config{Ordering: OrderingLIFO, PriorityEnabled: true}, NewManualClock(t0))
	q.Enqueue(Message{Sequence: 10, Priority: 5, AvailableAt: t0})
	q.Enqueue(Message{Sequence: 1, Priority: 10, AvailableAt: t0})
	q.Enqueue(Message{Sequence: 2, Priority: 10, AvailableAt: t0})
	q.Enqueue(Message{Sequence: 3, Priority: 1, AvailableAt: t0})

	got := drain(t, q, 4)
	// priority DESC, then seq DESC within a priority tie.
	assertSeqs(t, got, []uint64{2, 1, 10, 3})
}

func TestNearIdenticalDelayedTimestampsOrderBySequenceOncePromoted(t *testing.T) {
	clock := NewManualClock(t0)
	q := NewQueue(Config{Ordering: OrderingFIFO}, clock)

	// Deliberately delayed in an order that does NOT match sequence order,
	// with only a few nanoseconds between them.
	q.Enqueue(Message{Sequence: 30, AvailableAt: t0.Add(1 * time.Nanosecond)})
	q.Enqueue(Message{Sequence: 10, AvailableAt: t0.Add(2 * time.Nanosecond)})
	q.Enqueue(Message{Sequence: 20, AvailableAt: t0.Add(3 * time.Nanosecond)})

	if _, ok := q.Dequeue(); ok {
		t.Fatal("Dequeue: got ok=true before any AvailableAt, want false")
	}

	clock.Advance(5 * time.Nanosecond) // past all three

	got := drain(t, q, 3)
	// Ready ordering (sequence ASC) drives the final order, not the
	// timestamp arrival order — timestamps only gate eligibility.
	assertSeqs(t, got, []uint64{10, 20, 30})
}

func TestDelayedHighPriorityDoesNotBlockImmediateLowPriority(t *testing.T) {
	clock := NewManualClock(t0)
	q := NewQueue(Config{Ordering: OrderingFIFO, PriorityEnabled: true}, clock)

	q.Enqueue(Message{Sequence: 1, Priority: 1, AvailableAt: t0})                         // immediate, low priority
	q.Enqueue(Message{Sequence: 2, Priority: 100, AvailableAt: t0.Add(10 * time.Second)}) // delayed, high priority

	msg, ok := q.Dequeue()
	if !ok {
		t.Fatal("Dequeue: got ok=false, want the immediate low-priority message")
	}
	if msg.Sequence != 1 {
		t.Fatalf("Dequeue returned seq %d, want 1 (delayed high-priority must not block it)", msg.Sequence)
	}

	if _, ok := q.Dequeue(); ok {
		t.Fatal("Dequeue: got ok=true, want false (high-priority message is still delayed)")
	}
}

func TestMultipleDelayedItemsBecomeReadyIncrementally(t *testing.T) {
	clock := NewManualClock(t0)
	q := NewQueue(Config{Ordering: OrderingFIFO}, clock)

	q.Enqueue(Message{Sequence: 1, AvailableAt: t0.Add(1 * time.Second)})
	q.Enqueue(Message{Sequence: 2, AvailableAt: t0.Add(2 * time.Second)})
	q.Enqueue(Message{Sequence: 3, AvailableAt: t0.Add(3 * time.Second)})

	if got, want := q.DelayedLen(), 3; got != want {
		t.Fatalf("DelayedLen = %d, want %d", got, want)
	}
	if got, want := q.ReadyLen(), 0; got != want {
		t.Fatalf("ReadyLen = %d, want %d", got, want)
	}

	clock.Set(t0.Add(1 * time.Second))
	if got, want := q.ReadyLen(), 1; got != want {
		t.Fatalf("after +1s: ReadyLen = %d, want %d", got, want)
	}
	if got, want := q.DelayedLen(), 2; got != want {
		t.Fatalf("after +1s: DelayedLen = %d, want %d", got, want)
	}

	clock.Set(t0.Add(2 * time.Second))
	if got, want := q.ReadyLen(), 2; got != want {
		t.Fatalf("after +2s: ReadyLen = %d, want %d", got, want)
	}

	clock.Set(t0.Add(3 * time.Second))
	if got, want := q.ReadyLen(), 3; got != want {
		t.Fatalf("after +3s: ReadyLen = %d, want %d", got, want)
	}
	if got, want := q.DelayedLen(), 0; got != want {
		t.Fatalf("after +3s: DelayedLen = %d, want %d", got, want)
	}
}

func TestNoEarlyDelivery(t *testing.T) {
	clock := NewManualClock(t0)
	q := NewQueue(Config{Ordering: OrderingFIFO}, clock)

	q.Enqueue(Message{Sequence: 1, AvailableAt: t0.Add(1 * time.Second)})

	if _, ok := q.Dequeue(); ok {
		t.Fatal("Dequeue: got ok=true before AvailableAt, want false")
	}

	clock.Advance(500 * time.Millisecond)
	if _, ok := q.Dequeue(); ok {
		t.Fatal("Dequeue: got ok=true before AvailableAt, want false")
	}

	clock.Set(t0.Add(1 * time.Second)) // exactly AvailableAt: eligible per AvailableAt <= now
	msg, ok := q.Dequeue()
	if !ok {
		t.Fatal("Dequeue: got ok=false at AvailableAt boundary, want true")
	}
	if msg.Sequence != 1 {
		t.Fatalf("Dequeue returned seq %d, want 1", msg.Sequence)
	}
}
