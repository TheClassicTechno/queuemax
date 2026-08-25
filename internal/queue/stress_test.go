package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBoundedConcurrentProducersAndConsumers is the Phase 8 stress test:
// multiple producers and multiple consumers hammer one queue concurrently.
// It verifies, under real concurrency (not just per-method unit tests),
// that every message is ACKed exactly once — no message lost, no message
// double-delivered-and-double-ACKed, no deadlock. Bounded by an explicit
// overall timeout so a real bug (lost wakeup, deadlock) fails the test
// instead of hanging the suite forever.
func TestBoundedConcurrentProducersAndConsumers(t *testing.T) {
	wal, _ := openTestWAL(t)
	m := newTestManager(t, wal, nil)

	if err := m.CreateQueue(Config{Name: "jobs", Ordering: OrderingFIFO, VisibilityTimeoutMS: 30000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	const producers = 8
	const perProducer = 50
	const total = producers * perProducer
	const consumers = 8

	var produced sync.WaitGroup
	produced.Add(producers)
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer produced.Done()
			for i := 0; i < perProducer; i++ {
				payload := fmt.Sprintf("p%d-i%d", p, i)
				if _, err := m.Enqueue("jobs", []byte(payload), 0, 0); err != nil {
					t.Errorf("Enqueue: %v", err)
				}
			}
		}(p)
	}
	produced.Wait()

	var mu sync.Mutex
	acked := make(map[string]bool, total)
	var ackedCount int64

	var consumersWG sync.WaitGroup
	consumersWG.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer consumersWG.Done()
			for atomic.LoadInt64(&ackedCount) < int64(total) {
				d, ok, err := m.Receive("jobs")
				if err != nil {
					t.Errorf("Receive: %v", err)
					return
				}
				if !ok {
					// Everything already handed out to other consumers,
					// waiting on their Acks — brief yield, not a spin.
					time.Sleep(time.Millisecond)
					continue
				}
				if err := m.Ack("jobs", d.ReceiptHandle); err != nil {
					t.Errorf("Ack: %v", err)
					continue
				}

				mu.Lock()
				dup := acked[string(d.Message.Payload)]
				acked[string(d.Message.Payload)] = true
				mu.Unlock()
				if dup {
					t.Errorf("message %q ACKed more than once", d.Message.Payload)
					continue
				}
				atomic.AddInt64(&ackedCount, 1)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		consumersWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stress test did not complete within bound — possible deadlock or lost message")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(acked) != total {
		t.Fatalf("distinct ACKed messages = %d, want %d", len(acked), total)
	}
}
