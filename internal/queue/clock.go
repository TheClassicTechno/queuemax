package queue

import (
	"sync"
	"time"
)

// Clock abstracts time so delay/promotion logic can be tested
// deterministically instead of racing against the wall clock.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// ManualClock is a settable Clock for tests.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a ManualClock initialized to t.
func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{t: t}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Set pins the clock to t.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
