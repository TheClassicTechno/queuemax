package queue

import (
	"path/filepath"
	"testing"
	"time"

	"queuemax/internal/storage"
)

func openTestWAL(t *testing.T) (*storage.WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := storage.Open(path)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

func newTestManager(t *testing.T, wal *storage.WAL, clock Clock) *Manager {
	t.Helper()
	m, err := NewManager(wal, clock)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func TestCreateQueueDuplicateRejected(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	cfg := Config{Name: "jobs", Ordering: OrderingFIFO}
	if err := m.CreateQueue(cfg); err != nil {
		t.Fatalf("first CreateQueue: %v", err)
	}
	if err := m.CreateQueue(cfg); err != ErrQueueExists {
		t.Fatalf("second CreateQueue: got %v, want ErrQueueExists", err)
	}
}

func TestCreateQueueInvalidNameRejected(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	if err := m.CreateQueue(Config{Name: "", Ordering: OrderingFIFO}); err != ErrInvalidQueueName {
		t.Fatalf("empty name: got %v, want ErrInvalidQueueName", err)
	}
	if err := m.CreateQueue(Config{Name: "bad name!", Ordering: OrderingFIFO}); err != ErrInvalidQueueName {
		t.Fatalf("bad charset: got %v, want ErrInvalidQueueName", err)
	}
}

func TestEnqueueUnknownQueueRejected(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	if _, err := m.Enqueue("nope", []byte("x"), 0, 0); err != ErrQueueNotFound {
		t.Fatalf("got %v, want ErrQueueNotFound", err)
	}
}

func TestEnqueueNegativeDelayRejected(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("x"), 0, -time.Second); err != ErrInvalidDelay {
		t.Fatalf("got %v, want ErrInvalidDelay", err)
	}
}

func TestMessagesAndExactOrderSurviveRestart(t *testing.T) {
	wal, path := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	want := []string{"a", "b", "c"}
	for _, p := range want {
		if _, err := m.Enqueue("jobs", []byte(p), 0, 0); err != nil {
			t.Fatalf("Enqueue(%q): %v", p, err)
		}
	}
	wal.Close()

	wal2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()
	m2 := newTestManager(t, wal2, clock)

	q := m2.queues["jobs"]
	if q == nil {
		t.Fatal(`queue "jobs" missing after restart`)
	}
	for _, wantPayload := range want {
		msg, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue: expected message %q, got none", wantPayload)
		}
		if string(msg.Payload) != wantPayload {
			t.Fatalf("Dequeue payload = %q, want %q", msg.Payload, wantPayload)
		}
	}
}

func TestDelaySurvivesRestart(t *testing.T) {
	wal, path := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := m.Enqueue("jobs", []byte("delayed"), 0, 10*time.Second); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	wal.Close()

	wal2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()
	m2 := newTestManager(t, wal2, clock) // clock still reads "now" from before the crash

	q := m2.queues["jobs"]
	if _, ok := q.Dequeue(); ok {
		t.Fatal("Dequeue: got a message before its AvailableAt, want none")
	}

	clock.Advance(10 * time.Second)
	msg, ok := q.Dequeue()
	if !ok {
		t.Fatal("Dequeue after advancing clock: expected the delayed message")
	}
	if string(msg.Payload) != "delayed" {
		t.Fatalf("Dequeue payload = %q, want %q", msg.Payload, "delayed")
	}
}

func TestSequenceNotReusedAfterRestart(t *testing.T) {
	wal, path := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	var lastSeq uint64
	for i := 0; i < 5; i++ {
		msg, err := m.Enqueue("jobs", []byte("x"), 0, 0)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		lastSeq = msg.Sequence
	}
	wal.Close()

	wal2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()
	m2 := newTestManager(t, wal2, clock)

	msg, err := m2.Enqueue("jobs", []byte("y"), 0, 0)
	if err != nil {
		t.Fatalf("Enqueue after restart: %v", err)
	}
	if msg.Sequence != lastSeq+1 {
		t.Fatalf("Sequence after restart = %d, want %d", msg.Sequence, lastSeq+1)
	}
}

func TestConcurrentEnqueueUniqueSequences(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	const n = 100
	seqs := make(chan uint64, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			msg, err := m.Enqueue("jobs", []byte("x"), 0, 0)
			if err != nil {
				errs <- err
				return
			}
			seqs <- msg.Sequence
		}()
	}

	seen := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			t.Fatalf("Enqueue: %v", err)
		case seq := <-seqs:
			if seen[seq] {
				t.Fatalf("duplicate sequence %d", seq)
			}
			seen[seq] = true
		}
	}
}
