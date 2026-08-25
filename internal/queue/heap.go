package queue

// readyHeap is a container/heap-backed priority queue over Ready messages.
// Its ordering is entirely determined by the pluggable less function, so
// one implementation covers all four composable modes (FIFO/LIFO x
// priority) instead of four hardcoded structures — see DESIGN_DECISIONS.md
// #1/#2 and the Phase 4 plan.
type readyHeap struct {
	items []Message
	// index maps message ID -> its current position in items, kept in
	// sync by Swap/Push/Pop. It lets a caller find an arbitrary message's
	// heap position in O(1), so combined with heap.Remove it turns
	// "remove by ID" into O(log n) instead of an O(n) scan — needed for
	// Queue.Ack's late-ACK-after-expiry path (see removeReadyByIDLocked).
	index map[string]int
	less  func(a, b Message) bool
}

func (h *readyHeap) Len() int           { return len(h.items) }
func (h *readyHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }
func (h *readyHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.index[h.items[i].ID] = i
	h.index[h.items[j].ID] = j
}
func (h *readyHeap) Push(x interface{}) {
	msg := x.(Message)
	h.index[msg.ID] = len(h.items)
	h.items = append(h.items, msg)
}
func (h *readyHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	delete(h.index, item.ID)
	return item
}

// delayedHeap is a min-heap ordered by AvailableAt, independent of a
// queue's ordering mode (delay is orthogonal to ordering — see
// DESIGN_DECISIONS.md #3). Ties on AvailableAt break on Sequence, never
// on wall-clock comparisons, for the same determinism reason as the Ready
// heap (DESIGN_DECISIONS.md #4).
type delayedHeap struct {
	items []Message
}

func (h *delayedHeap) Len() int { return len(h.items) }
func (h *delayedHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if !a.AvailableAt.Equal(b.AvailableAt) {
		return a.AvailableAt.Before(b.AvailableAt)
	}
	return a.Sequence < b.Sequence
}
func (h *delayedHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *delayedHeap) Push(x interface{}) { h.items = append(h.items, x.(Message)) }
func (h *delayedHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

// Peek returns the earliest-eligible delayed message without removing it.
func (h *delayedHeap) Peek() (Message, bool) {
	if len(h.items) == 0 {
		return Message{}, false
	}
	return h.items[0], true
}

// lessFuncFor builds the Ready-heap ordering for a queue's configuration,
// covering all four composable modes per DESIGN_DECISIONS.md #1/#2.
func lessFuncFor(cfg Config) func(a, b Message) bool {
	switch {
	case cfg.PriorityEnabled && cfg.Ordering == OrderingLIFO:
		return func(a, b Message) bool {
			if a.Priority != b.Priority {
				return a.Priority > b.Priority // priority DESC
			}
			return a.Sequence > b.Sequence // seq DESC
		}
	case cfg.PriorityEnabled:
		return func(a, b Message) bool {
			if a.Priority != b.Priority {
				return a.Priority > b.Priority // priority DESC
			}
			return a.Sequence < b.Sequence // seq ASC
		}
	case cfg.Ordering == OrderingLIFO:
		return func(a, b Message) bool {
			return a.Sequence > b.Sequence // seq DESC
		}
	default: // FIFO
		return func(a, b Message) bool {
			return a.Sequence < b.Sequence // seq ASC
		}
	}
}
