package httpapi

import (
	"net/http"

	"queuemax/internal/queue"
)

// NewRouter builds the HTTP handler tree for the queue API.
//
// Only /health is wired up in this phase; the remaining endpoints
// (queues, messages, receive, ack, stats) are added once the queue
// domain and WAL are implemented in later phases.
func NewRouter(mgr *queue.Manager) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	return mux
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
