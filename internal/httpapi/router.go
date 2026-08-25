package httpapi

import (
	"net/http"

	"queuemax/internal/queue"
)

// NewRouter builds the HTTP handler tree for the queue API. Handlers only
// decode requests, call the Manager, and translate its results/errors into
// HTTP status codes — ordering, durability, and validation semantics all
// live in the queue package (CLAUDE.md's "handlers do validation and
// translation only").
func NewRouter(mgr *queue.Manager) http.Handler {
	h := &handler{mgr: mgr}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /queues", h.createQueue)
	mux.HandleFunc("GET /queues", h.listQueues)
	mux.HandleFunc("POST /queues/{name}/messages", h.enqueue)
	mux.HandleFunc("POST /queues/{name}/messages/receive", h.receive)
	mux.HandleFunc("POST /queues/{name}/messages/{receiptHandle}/ack", h.ack)
	mux.HandleFunc("GET /queues/{name}/stats", h.stats)
	return mux
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
