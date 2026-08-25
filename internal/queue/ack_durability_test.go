package queue

import (
	"sync"
	"testing"
	"time"

	"queuemax/internal/storage"
)

func TestAckSurvivesRestart(t *testing.T) {
	wal, path := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 30000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("a"), 0, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("b"), 0, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	d, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("Receive: ok=%v err=%v", ok, err)
	}
	if string(d.Message.Payload) != "a" {
		t.Fatalf("Receive payload = %q, want %q", d.Message.Payload, "a")
	}
	if err := m.Ack("jobs", d.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	wal.Close()

	wal2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()
	m2 := newTestManager(t, wal2, clock)

	// "a" must not come back; "b" must still be there.
	seen := make(map[string]bool)
	for {
		d, ok, err := m2.Receive("jobs")
		if err != nil {
			t.Fatalf("Receive after restart: %v", err)
		}
		if !ok {
			break
		}
		seen[string(d.Message.Payload)] = true
	}
	if seen["a"] {
		t.Fatal("ACKed message \"a\" reappeared after restart")
	}
	if !seen["b"] {
		t.Fatal("unACKed message \"b\" did not survive restart")
	}
}

func TestSequenceNotReusedAfterAckAndRestart(t *testing.T) {
	wal, path := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 30000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	msg, err := m.Enqueue("jobs", []byte("a"), 0, 0)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ackedSeq := msg.Sequence

	d, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("Receive: ok=%v err=%v", ok, err)
	}
	if err := m.Ack("jobs", d.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	wal.Close()

	wal2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()
	m2 := newTestManager(t, wal2, clock)

	msg2, err := m2.Enqueue("jobs", []byte("b"), 0, 0)
	if err != nil {
		t.Fatalf("Enqueue after restart: %v", err)
	}
	if msg2.Sequence != ackedSeq+1 {
		t.Fatalf("Sequence after restart = %d, want %d (ACKed message's sequence must still count)", msg2.Sequence, ackedSeq+1)
	}
}

func TestDuplicateAckRejectedAsStaleHandle(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 30000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("a"), 0, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	d, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("Receive: ok=%v err=%v", ok, err)
	}
	if err := m.Ack("jobs", d.ReceiptHandle); err != nil {
		t.Fatalf("first Ack: %v", err)
	}

	// DESIGN_DECISIONS.md #7: a duplicate ACK is rejected via the same
	// stale-handle path, not a special-cased idempotent-success branch.
	if err := m.Ack("jobs", d.ReceiptHandle); err != ErrStaleReceiptHandle {
		t.Fatalf("duplicate Ack: got %v, want ErrStaleReceiptHandle", err)
	}
}

func TestStaleAckCannotConsumeRedelivery(t *testing.T) {
	wal, _ := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 1000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("a"), 0, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	first, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("first Receive: ok=%v err=%v", ok, err)
	}
	clock.Advance(2 * time.Second) // lease expires
	second, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("redelivery Receive: ok=%v err=%v", ok, err)
	}

	// The stale (pre-redelivery) handle must not be able to Ack the
	// message out from under the consumer that legitimately holds it now.
	if err := m.Ack("jobs", first.ReceiptHandle); err != ErrStaleReceiptHandle {
		t.Fatalf("stale Ack: got %v, want ErrStaleReceiptHandle", err)
	}

	// The current handle still works.
	if err := m.Ack("jobs", second.ReceiptHandle); err != nil {
		t.Fatalf("current Ack: %v", err)
	}
}

func TestConcurrentAckVsTimeout(t *testing.T) {
	wal, _ := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 1000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("a"), 0, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	d, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("Receive: ok=%v err=%v", ok, err)
	}
	clock.Advance(2 * time.Second) // lease is now logically expired

	// Race an Ack against a Receive that would trigger the lazy expiry
	// sweep. Exactly one of "Ack succeeds" / "message gets redelivered by
	// this Receive" may happen, but never neither and never a duplicate
	// hand-out — the two operations serialize on the same per-queue lock
	// (DESIGN_DECISIONS.md #14). Run under -race to catch any real data race.
	var wg sync.WaitGroup
	var ackErr error
	var redelivered bool
	wg.Add(2)
	go func() {
		defer wg.Done()
		ackErr = m.Ack("jobs", d.ReceiptHandle)
	}()
	go func() {
		defer wg.Done()
		_, ok, _ := m.Receive("jobs")
		redelivered = ok
	}()
	wg.Wait()

	if ackErr == nil && redelivered {
		t.Fatal("both Ack succeeded and the message was redelivered — should be mutually exclusive")
	}
	if ackErr != nil && !redelivered {
		t.Fatal("Ack failed and the message was not redelivered either — message is stuck")
	}
}
