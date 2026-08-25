package queue

import (
	"sync"
	"testing"
	"time"
)

func newVisibilityQueue(t *testing.T, timeoutMS int64, clock Clock) *Queue {
	t.Helper()
	return NewQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: timeoutMS}, clock)
}

func TestReceiveThenAck(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	q := newVisibilityQueue(t, 30000, clock)
	q.Enqueue(Message{ID: "jobs-0", Sequence: 0, EnqueuedAt: clock.Now(), AvailableAt: clock.Now()})

	d, ok := q.Receive()
	if !ok {
		t.Fatal("Receive: expected a message")
	}
	if d.Message.ID != "jobs-0" {
		t.Fatalf("Receive ID = %q, want jobs-0", d.Message.ID)
	}

	if err := q.Ack(d.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Second Ack with the same (now-consumed) handle must be rejected.
	if err := q.Ack(d.ReceiptHandle); err != ErrStaleReceiptHandle {
		t.Fatalf("second Ack: got %v, want ErrStaleReceiptHandle", err)
	}

	// Nothing left to receive.
	if _, ok := q.Receive(); ok {
		t.Fatal("Receive after Ack: expected nothing, got a message")
	}
}

func TestReceiveNoAckStaysInFlight(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	q := newVisibilityQueue(t, 30000, clock)
	q.Enqueue(Message{ID: "jobs-0", Sequence: 0, EnqueuedAt: clock.Now(), AvailableAt: clock.Now()})

	if _, ok := q.Receive(); !ok {
		t.Fatal("Receive: expected a message")
	}

	// Lease hasn't expired yet — nothing else to hand out.
	if _, ok := q.Receive(); ok {
		t.Fatal("Receive before expiry: expected nothing, got a message")
	}
}

func TestExpiryRedeliversWithStableIDAndNewHandle(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	q := newVisibilityQueue(t, 1000, clock) // 1s visibility timeout
	q.Enqueue(Message{ID: "jobs-0", Sequence: 0, EnqueuedAt: clock.Now(), AvailableAt: clock.Now()})

	first, ok := q.Receive()
	if !ok {
		t.Fatal("first Receive: expected a message")
	}

	clock.Advance(2 * time.Second) // past the 1s lease

	second, ok := q.Receive()
	if !ok {
		t.Fatal("second Receive after expiry: expected redelivery")
	}

	if second.Message.ID != first.Message.ID {
		t.Fatalf("redelivered ID = %q, want stable ID %q", second.Message.ID, first.Message.ID)
	}
	if second.ReceiptHandle == first.ReceiptHandle {
		t.Fatalf("redelivery reused the same receipt handle %q, want a new one", first.ReceiptHandle)
	}
}

func TestStaleReceiptHandleRejectedAfterRedelivery(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	q := newVisibilityQueue(t, 1000, clock)
	q.Enqueue(Message{ID: "jobs-0", Sequence: 0, EnqueuedAt: clock.Now(), AvailableAt: clock.Now()})

	first, _ := q.Receive()
	clock.Advance(2 * time.Second)
	second, ok := q.Receive()
	if !ok {
		t.Fatal("expected redelivery")
	}

	// The original (now-superseded) handle must be rejected.
	if err := q.Ack(first.ReceiptHandle); err != ErrStaleReceiptHandle {
		t.Fatalf("Ack with stale handle: got %v, want ErrStaleReceiptHandle", err)
	}

	// The new handle still works.
	if err := q.Ack(second.ReceiptHandle); err != nil {
		t.Fatalf("Ack with current handle: %v", err)
	}
}

func TestLateAckBeforeRedeliveryWinsOverExpiry(t *testing.T) {
	clock := NewManualClock(time.Unix(1000, 0))
	q := newVisibilityQueue(t, 1000, clock)
	q.Enqueue(Message{ID: "jobs-0", Sequence: 0, EnqueuedAt: clock.Now(), AvailableAt: clock.Now()})

	d, _ := q.Receive()
	clock.Advance(2 * time.Second) // lease expires, but nothing has redelivered it yet

	if err := q.Ack(d.ReceiptHandle); err != nil {
		t.Fatalf("late Ack before redelivery: got %v, want success (DESIGN_DECISIONS.md #14)", err)
	}

	// It must not be redeliverable — the late Ack pulled it back out of Ready.
	if _, ok := q.Receive(); ok {
		t.Fatal("Receive after late Ack: expected nothing, message should be gone")
	}
}

func TestConcurrentReceiveDeliversEachMessageOnce(t *testing.T) {
	q := newVisibilityQueue(t, 30000, nil)
	const n = 50
	for i := 0; i < n; i++ {
		q.Enqueue(Message{ID: string(rune('a' + i)), Sequence: uint64(i), EnqueuedAt: time.Now(), AvailableAt: time.Now()})
	}

	var mu sync.Mutex
	seen := make(map[string]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, ok := q.Receive()
			if !ok {
				t.Error("Receive: expected a message")
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[d.Message.ID] {
				t.Errorf("message %q delivered more than once", d.Message.ID)
			}
			seen[d.Message.ID] = true
		}()
	}
	wg.Wait()
}
