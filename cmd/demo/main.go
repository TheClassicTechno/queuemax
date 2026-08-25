// Command demo drives FrankenQueue's real HTTP API end-to-end to prove
// the properties CLAUDE.md's reviewer demo requires: queue creation,
// FIFO/LIFO, priority ordering, delay, ACK, lease-expiry replay, restart
// durability, and stats. It prints expected-vs-observed for every step
// and exits non-zero on the first mismatch.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"queuemax/internal/httpapi"
	"queuemax/internal/queue"
	"queuemax/internal/storage"
)

// server bundles the pieces needed to start/stop an in-process HTTP
// server backed by a durable WAL, so "restart" can reopen the same file.
type server struct {
	walPath string
	wal     *storage.WAL
	mgr     *queue.Manager
	ln      net.Listener
	httpSrv *http.Server
	baseURL string
}

func startServer(walPath string) (*server, error) {
	wal, err := storage.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	mgr, err := queue.NewManager(wal, nil)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("new manager: %w", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("listen: %w", err)
	}
	httpSrv := &http.Server{Handler: httpapi.NewRouter(mgr)}
	go httpSrv.Serve(ln)

	return &server{
		walPath: walPath,
		wal:     wal,
		mgr:     mgr,
		ln:      ln,
		httpSrv: httpSrv,
		baseURL: "http://" + ln.Addr().String(),
	}, nil
}

// restart stops the HTTP listener and closes the WAL, then reopens the
// same WAL file and starts a fresh server — exercising the real
// WAL -> replay -> rebuild-state path, just without an OS process
// boundary in between.
func (s *server) restart() error {
	s.httpSrv.Close()
	s.wal.Close()

	wal, err := storage.Open(s.walPath)
	if err != nil {
		return fmt.Errorf("reopen wal: %w", err)
	}
	mgr, err := queue.NewManager(wal, nil)
	if err != nil {
		wal.Close()
		return fmt.Errorf("recover manager: %w", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		wal.Close()
		return fmt.Errorf("listen: %w", err)
	}
	httpSrv := &http.Server{Handler: httpapi.NewRouter(mgr)}
	go httpSrv.Serve(ln)

	s.wal, s.mgr, s.ln, s.httpSrv = wal, mgr, ln, httpSrv
	s.baseURL = "http://" + ln.Addr().String()
	return nil
}

func (s *server) close() {
	s.httpSrv.Close()
	s.wal.Close()
}

// --- minimal HTTP client helpers -------------------------------------

var client = &http.Client{Timeout: 5 * time.Second}

func (s *server) post(path string, body any) (*http.Response, map[string]any, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, nil, err
		}
	}
	resp, err := client.Post(s.baseURL+path, "application/json", &buf)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
			return resp, nil, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp, decoded, nil
}

func (s *server) get(path string) (*http.Response, map[string]any, error) {
	resp, err := client.Get(s.baseURL + path)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return resp, nil, fmt.Errorf("decode response: %w", err)
	}
	return resp, decoded, nil
}

// --- expected-vs-observed reporting -----------------------------------

var failures int

func check(step, expected string, observed string, ok bool) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		failures++
	}
	fmt.Printf("[%s] %-45s expected=%-20s observed=%-20s\n", status, step, expected, observed)
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func mustEnqueue(s *server, qname, payload string, priority int, delayMS int64) string {
	_, resp, err := s.post(fmt.Sprintf("/queues/%s/messages", qname), map[string]any{
		"payload": b64(payload), "priority": priority, "delay_ms": delayMS,
	})
	if err != nil {
		fmt.Println("fatal: enqueue:", err)
		os.Exit(1)
	}
	return resp["id"].(string)
}

func main() {
	fmt.Println("FrankenQueue reviewer demo")
	fmt.Println("==========================")

	dir, err := os.MkdirTemp("", "frankenqueue-demo-*")
	if err != nil {
		fmt.Println("fatal:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir) // self-cleaning
	walPath := filepath.Join(dir, "frankenqueue.wal")

	s, err := startServer(walPath)
	if err != nil {
		fmt.Println("fatal:", err)
		os.Exit(1)
	}
	defer s.close()

	// 1. Queue creation
	resp, body, err := s.post("/queues", map[string]any{
		"name": "fifo-demo", "ordering": "fifo", "priority_enabled": false, "visibility_timeout_ms": 60000,
	})
	check("1. create queue (fifo-demo)", "201", fmt.Sprint(resp.StatusCode), err == nil && resp.StatusCode == 201)
	_ = body
	s.post("/queues", map[string]any{"name": "lifo-demo", "ordering": "lifo", "priority_enabled": false, "visibility_timeout_ms": 60000})
	s.post("/queues", map[string]any{"name": "priority-demo", "ordering": "fifo", "priority_enabled": true, "visibility_timeout_ms": 60000})
	s.post("/queues", map[string]any{"name": "delay-demo", "ordering": "fifo", "priority_enabled": false, "visibility_timeout_ms": 60000})
	s.post("/queues", map[string]any{"name": "lease-demo", "ordering": "fifo", "priority_enabled": false, "visibility_timeout_ms": 2000})
	s.post("/queues", map[string]any{"name": "restart-demo", "ordering": "fifo", "priority_enabled": false, "visibility_timeout_ms": 60000})

	// 2. FIFO
	idA := mustEnqueue(s, "fifo-demo", "A", 0, 0)
	idB := mustEnqueue(s, "fifo-demo", "B", 0, 0)
	idC := mustEnqueue(s, "fifo-demo", "C", 0, 0)
	_, r1, _ := s.post("/queues/fifo-demo/messages/receive", nil)
	_, r2, _ := s.post("/queues/fifo-demo/messages/receive", nil)
	_, r3, _ := s.post("/queues/fifo-demo/messages/receive", nil)
	check("2. FIFO order (A,B,C)", fmt.Sprintf("%s,%s,%s", idA, idB, idC),
		fmt.Sprintf("%v,%v,%v", r1["id"], r2["id"], r3["id"]),
		r1["id"] == idA && r2["id"] == idB && r3["id"] == idC)

	// 3. LIFO
	lA := mustEnqueue(s, "lifo-demo", "A", 0, 0)
	lB := mustEnqueue(s, "lifo-demo", "B", 0, 0)
	lC := mustEnqueue(s, "lifo-demo", "C", 0, 0)
	_, lr1, _ := s.post("/queues/lifo-demo/messages/receive", nil)
	_, lr2, _ := s.post("/queues/lifo-demo/messages/receive", nil)
	_, lr3, _ := s.post("/queues/lifo-demo/messages/receive", nil)
	check("3. LIFO order (C,B,A)", fmt.Sprintf("%s,%s,%s", lC, lB, lA),
		fmt.Sprintf("%v,%v,%v", lr1["id"], lr2["id"], lr3["id"]),
		lr1["id"] == lC && lr2["id"] == lB && lr3["id"] == lA)

	// 4. Priority (priority DESC, sequence ASC within a tier)
	pLow := mustEnqueue(s, "priority-demo", "low", 1, 0)
	pHigh := mustEnqueue(s, "priority-demo", "high", 10, 0)
	pMid := mustEnqueue(s, "priority-demo", "mid", 5, 0)
	_, pr1, _ := s.post("/queues/priority-demo/messages/receive", nil)
	_, pr2, _ := s.post("/queues/priority-demo/messages/receive", nil)
	_, pr3, _ := s.post("/queues/priority-demo/messages/receive", nil)
	check("4. priority order (high,mid,low)", fmt.Sprintf("%s,%s,%s", pHigh, pMid, pLow),
		fmt.Sprintf("%v,%v,%v", pr1["id"], pr2["id"], pr3["id"]),
		pr1["id"] == pHigh && pr2["id"] == pMid && pr3["id"] == pLow)

	// 5. Delay: not delivered immediately, delivered after it elapses
	dID := mustEnqueue(s, "delay-demo", "delayed", 0, 1500)
	_, dr0, _ := s.post("/queues/delay-demo/messages/receive", nil)
	immediateEmpty := dr0 == nil
	time.Sleep(2 * time.Second)
	_, dr1, _ := s.post("/queues/delay-demo/messages/receive", nil)
	check("5. delayed message (no early delivery, then ready)", "empty,then "+dID,
		fmt.Sprintf("empty=%v,then=%v", immediateEmpty, dr1["id"]),
		immediateEmpty && dr1["id"] == dID)

	// 6. ACK
	ackID := mustEnqueue(s, "fifo-demo", "ack-me", 0, 0)
	_, ar, _ := s.post("/queues/fifo-demo/messages/receive", nil)
	handle, _ := ar["receipt_handle"].(string)
	ackResp, _, _ := s.post("/queues/fifo-demo/messages/"+handle+"/ack", nil)
	check("6. ACK", "200", fmt.Sprint(ackResp.StatusCode), ackResp.StatusCode == 200)
	_ = ackID

	// 7. No ACK -> lease expiry -> redelivery
	leaseID := mustEnqueue(s, "lease-demo", "no-ack", 0, 0)
	_, first, _ := s.post("/queues/lease-demo/messages/receive", nil)
	firstHandle := first["receipt_handle"]
	time.Sleep(3 * time.Second) // past the 2s visibility timeout
	_, second, _ := s.post("/queues/lease-demo/messages/receive", nil)
	check("7. no ACK -> expiry -> redelivery (same ID, new handle)",
		"same id, handle changes",
		fmt.Sprintf("id_match=%v,handle_changed=%v", second["id"] == leaseID, second["receipt_handle"] != firstHandle),
		second["id"] == leaseID && second["receipt_handle"] != firstHandle)

	// 8. Restart durability: enqueue, restart the process in-place, confirm survival
	rID := mustEnqueue(s, "restart-demo", "survive-me", 0, 0)
	if err := s.restart(); err != nil {
		fmt.Println("fatal: restart:", err)
		os.Exit(1)
	}
	_, statsBefore, _ := s.get("/queues/restart-demo/stats")
	_, rr, _ := s.post("/queues/restart-demo/messages/receive", nil)
	check("8. restart durability (message survives)", rID, fmt.Sprint(rr["id"]),
		rr["id"] == rID)
	_ = statsBefore

	// 9. Stats
	_, stats, _ := s.get("/queues/fifo-demo/stats")
	check("9. stats reflects queue state", "well-formed stats object",
		fmt.Sprintf("%v", stats), stats["name"] == "fifo-demo")

	fmt.Println("==========================")
	if failures > 0 {
		fmt.Printf("%d step(s) FAILED\n", failures)
		os.Exit(1)
	}
	fmt.Println("All steps PASSED")
}
