package main

import (
	"log"
	"net/http"

	"queuemax/internal/httpapi"
	"queuemax/internal/queue"
	"queuemax/internal/storage"
)

func main() {
	wal, err := storage.Open("frankenqueue.wal")
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	mgr, err := queue.NewManager(wal, nil)
	if err != nil {
		log.Fatalf("recover from WAL: %v", err)
	}
	handler := httpapi.NewRouter(mgr)

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", handler); err != nil {
		log.Fatal(err)
	}
}
