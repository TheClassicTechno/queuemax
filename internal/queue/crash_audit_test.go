package queue

import (
	"testing"
	"time"

	"queuemax/internal/storage"
)

// TestDoubleRestartProducesStableState guards against double-replay
// effects (CRASH_AUDIT.md's Phase 11 finding): reopening the same WAL
// twice in a row, with no new writes between the two restarts, must
// reproduce identical queue state both times — never duplicating,
// dropping, or reordering anything just because Replay ran again.
func TestDoubleRestartProducesStableState(t *testing.T) {
	wal, path := openTestWAL(t)
	clock := NewManualClock(time.Unix(1000, 0))
	m := newTestManager(t, wal, clock)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 30000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	for _, payload := range []string{"a", "b", "c"} {
		if _, err := m.Enqueue("jobs", []byte(payload), 0, 0); err != nil {
			t.Fatalf("Enqueue(%q): %v", payload, err)
		}
	}
	// Ack "a" so the acked-filter path is also exercised across both replays.
	d, ok, err := m.Receive("jobs")
	if err != nil || !ok {
		t.Fatalf("Receive: ok=%v err=%v", ok, err)
	}
	if err := m.Ack("jobs", d.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	wal.Close()

	drain := func() []string {
		wal2, err := storage.Open(path)
		if err != nil {
			t.Fatalf("reopen WAL: %v", err)
		}
		defer wal2.Close()
		m2 := newTestManager(t, wal2, clock)

		var got []string
		for {
			d, ok, err := m2.Receive("jobs")
			if err != nil {
				t.Fatalf("Receive after restart: %v", err)
			}
			if !ok {
				break
			}
			got = append(got, string(d.Message.Payload))
		}
		return got
	}

	first := drain()
	second := drain()

	if len(first) != 2 || first[0] != "b" || first[1] != "c" {
		t.Fatalf("first restart drained %v, want [b c]", first)
	}
	if len(second) != 2 || second[0] != "b" || second[1] != "c" {
		t.Fatalf("second restart drained %v, want [b c] (identical to first — no double-replay effect)", second)
	}
}
