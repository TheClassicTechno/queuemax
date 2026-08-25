.PHONY: demo test race

demo:
	go run ./cmd/demo

test:
	go test ./...

race:
	go test -race ./...
