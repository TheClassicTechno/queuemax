package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"queuemax/internal/queue"
	"queuemax/internal/storage"
)

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	wal, err := storage.Open(filepath.Join(t.TempDir(), "test.wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { wal.Close() })

	mgr, err := queue.NewManager(wal, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return NewRouter(mgr)
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = httptest.NewRequest(method, path, bytes.NewReader(buf))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func createTestQueue(t *testing.T, h http.Handler, name string) {
	t.Helper()
	w := doJSON(t, h, "POST", "/queues", map[string]any{
		"name": name, "ordering": "fifo", "priority_enabled": false, "visibility_timeout_ms": 30000,
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("createTestQueue: status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestHealth(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "GET", "/health", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /health: status = %d", w.Code)
	}
}

func TestCreateQueueSuccess(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "POST", "/queues", map[string]any{
		"name": "jobs", "ordering": "fifo", "priority_enabled": true, "visibility_timeout_ms": 5000,
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp queueResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Name != "jobs" || resp.Ordering != "fifo" || !resp.PriorityEnabled || resp.VisibilityTimeoutMS != 5000 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestCreateQueueDuplicateRejected(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	w := doJSON(t, h, "POST", "/queues", map[string]any{
		"name": "jobs", "ordering": "fifo", "visibility_timeout_ms": 30000,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("duplicate CreateQueue: status = %d, want 409", w.Code)
	}
}

func TestCreateQueueInvalidNameRejected(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "POST", "/queues", map[string]any{
		"name": "bad name!", "ordering": "fifo", "visibility_timeout_ms": 30000,
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid name: status = %d, want 400", w.Code)
	}
}

func TestCreateQueueMalformedJSONRejected(t *testing.T) {
	h := newTestRouter(t)
	r := httptest.NewRequest("POST", "/queues", bytes.NewReader([]byte("{not json")))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed JSON: status = %d, want 400", w.Code)
	}
}

func TestListQueues(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "b")
	createTestQueue(t, h, "a")

	w := doJSON(t, h, "GET", "/queues", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp []queueResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 2 || resp[0].Name != "a" || resp[1].Name != "b" {
		t.Fatalf("expected sorted [a b], got %+v", resp)
	}
}

func TestEnqueueSuccess(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")

	payload := base64.StdEncoding.EncodeToString([]byte("hello"))
	w := doJSON(t, h, "POST", "/queues/jobs/messages", map[string]any{
		"payload": payload, "priority": 3, "delay_ms": 0,
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp messageResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID == "" || resp.Priority != 3 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestEnqueueUnknownQueueRejected(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "POST", "/queues/ghost/messages", map[string]any{"payload": ""})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestEnqueueNegativeDelayRejected(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	w := doJSON(t, h, "POST", "/queues/jobs/messages", map[string]any{"payload": "", "delay_ms": -1})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestEnqueueBadBase64Rejected(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	w := doJSON(t, h, "POST", "/queues/jobs/messages", map[string]any{"payload": "not-base64!!"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestEnqueueOversizedPayloadRejected(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	big := make([]byte, storage.MaxPayloadBytes+1)
	w := doJSON(t, h, "POST", "/queues/jobs/messages", map[string]any{
		"payload": base64.StdEncoding.EncodeToString(big),
	})
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", w.Code)
	}
}

func TestReceiveEmptyQueueReturns204(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	w := doJSON(t, h, "POST", "/queues/jobs/messages/receive", nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
}

func TestReceiveUnknownQueueRejected(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "POST", "/queues/ghost/messages/receive", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestReceiveThenAckRoundTrip(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	payload := base64.StdEncoding.EncodeToString([]byte("hi"))
	doJSON(t, h, "POST", "/queues/jobs/messages", map[string]any{"payload": payload})

	w := doJSON(t, h, "POST", "/queues/jobs/messages/receive", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("Receive: status = %d, body = %s", w.Code, w.Body.String())
	}
	var d deliveryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.ReceiptHandle == "" {
		t.Fatal("expected non-empty receipt handle")
	}

	ackW := doJSON(t, h, "POST", "/queues/jobs/messages/"+d.ReceiptHandle+"/ack", nil)
	if ackW.Code != http.StatusOK {
		t.Fatalf("Ack: status = %d, body = %s", ackW.Code, ackW.Body.String())
	}
}

func TestAckStaleHandleRejected(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	w := doJSON(t, h, "POST", "/queues/jobs/messages/bogus:1:1/ack", nil)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestAckUnknownQueueRejected(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "POST", "/queues/ghost/messages/bogus:1:1/ack", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestStats(t *testing.T) {
	h := newTestRouter(t)
	createTestQueue(t, h, "jobs")
	payload := base64.StdEncoding.EncodeToString([]byte("x"))
	doJSON(t, h, "POST", "/queues/jobs/messages", map[string]any{"payload": payload})

	w := doJSON(t, h, "GET", "/queues/jobs/stats", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var s queue.QueueStats
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s.Name != "jobs" || s.Ready != 1 || s.Delayed != 0 || s.InFlight != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestStatsUnknownQueueRejected(t *testing.T) {
	h := newTestRouter(t)
	w := doJSON(t, h, "GET", "/queues/ghost/stats", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}
