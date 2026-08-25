package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"queuemax/internal/queue"
)

// maxCreateQueueBodyBytes and maxEnqueueBodyBytes bound the raw HTTP
// request body before any JSON decoding happens, so an oversized or
// unbounded body can't force allocation ahead of the queue layer's own
// payload-length check (storage.MaxPayloadBytes). The enqueue bound is
// larger than MaxPayloadBytes to account for base64's ~4/3 inflation plus
// JSON overhead.
const (
	maxCreateQueueBodyBytes = 4 << 10 // 4 KiB
	maxEnqueueBodyBytes     = 2 << 20 // 2 MiB
)

type handler struct {
	mgr *queue.Manager
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

type queueResponse struct {
	Name                string `json:"name"`
	Ordering            string `json:"ordering"`
	PriorityEnabled     bool   `json:"priority_enabled"`
	VisibilityTimeoutMS int64  `json:"visibility_timeout_ms"`
}

func toQueueResponse(cfg queue.Config) queueResponse {
	return queueResponse{
		Name:                cfg.Name,
		Ordering:            string(cfg.Ordering),
		PriorityEnabled:     cfg.PriorityEnabled,
		VisibilityTimeoutMS: cfg.VisibilityTimeoutMS,
	}
}

// createQueue handles POST /queues.
func (h *handler) createQueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCreateQueueBodyBytes)

	var req queueResponse
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("malformed JSON body"))
		return
	}

	cfg := queue.Config{
		Name:                req.Name,
		Ordering:            queue.Ordering(req.Ordering),
		PriorityEnabled:     req.PriorityEnabled,
		VisibilityTimeoutMS: req.VisibilityTimeoutMS,
	}
	if err := h.mgr.CreateQueue(cfg); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, queue.ErrQueueExists) {
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}

	writeJSON(w, http.StatusCreated, toQueueResponse(cfg))
}

// listQueues handles GET /queues.
func (h *handler) listQueues(w http.ResponseWriter, r *http.Request) {
	cfgs := h.mgr.ListQueues()
	out := make([]queueResponse, len(cfgs))
	for i, cfg := range cfgs {
		out[i] = toQueueResponse(cfg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, out)
}

type enqueueRequest struct {
	Payload  string `json:"payload"` // base64-encoded
	Priority int    `json:"priority"`
	DelayMS  int64  `json:"delay_ms"`
}

type messageResponse struct {
	ID          string    `json:"id"`
	Sequence    uint64    `json:"sequence"`
	Priority    int       `json:"priority"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	AvailableAt time.Time `json:"available_at"`
}

// enqueue handles POST /queues/{name}/messages.
func (h *handler) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxEnqueueBodyBytes)
	name := r.PathValue("name")

	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("malformed JSON body"))
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("payload must be base64-encoded"))
		return
	}

	msg, err := h.mgr.Enqueue(name, payload, req.Priority, time.Duration(req.DelayMS)*time.Millisecond)
	if err != nil {
		writeError(w, enqueueErrorStatus(err), err)
		return
	}

	writeJSON(w, http.StatusCreated, messageResponse{
		ID:          msg.ID,
		Sequence:    msg.Sequence,
		Priority:    msg.Priority,
		EnqueuedAt:  msg.EnqueuedAt,
		AvailableAt: msg.AvailableAt,
	})
}

func enqueueErrorStatus(err error) int {
	switch {
	case errors.Is(err, queue.ErrQueueNotFound):
		return http.StatusNotFound
	case errors.Is(err, queue.ErrPayloadTooLarge):
		return http.StatusRequestEntityTooLarge
	default: // ErrInvalidDelay and any other validation failure
		return http.StatusBadRequest
	}
}

type deliveryResponse struct {
	ID            string    `json:"id"`
	Payload       string    `json:"payload"` // base64-encoded
	Priority      int       `json:"priority"`
	Sequence      uint64    `json:"sequence"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
	AvailableAt   time.Time `json:"available_at"`
	ReceiptHandle string    `json:"receipt_handle"`
	LeaseUntil    time.Time `json:"lease_until"`
}

// receive handles POST /queues/{name}/messages/receive.
func (h *handler) receive(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	d, ok, err := h.mgr.Receive(name)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	writeJSON(w, http.StatusOK, deliveryResponse{
		ID:            d.Message.ID,
		Payload:       base64.StdEncoding.EncodeToString(d.Message.Payload),
		Priority:      d.Message.Priority,
		Sequence:      d.Message.Sequence,
		EnqueuedAt:    d.Message.EnqueuedAt,
		AvailableAt:   d.Message.AvailableAt,
		ReceiptHandle: d.ReceiptHandle,
		LeaseUntil:    d.LeaseUntil,
	})
}

// ack handles POST /queues/{name}/messages/{receiptHandle}/ack.
func (h *handler) ack(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	receiptHandle := r.PathValue("receiptHandle")

	if err := h.mgr.Ack(name, receiptHandle); err != nil {
		status := http.StatusBadRequest
		switch {
		case errors.Is(err, queue.ErrQueueNotFound):
			status = http.StatusNotFound
		case errors.Is(err, queue.ErrStaleReceiptHandle):
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// stats handles GET /queues/{name}/stats.
func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	s, err := h.mgr.Stats(name)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}
