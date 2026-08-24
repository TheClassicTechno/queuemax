package queue

import "time"

// Message is a durable unit of work in a queue.
type Message struct {
	ID          string
	Payload     []byte
	Priority    int
	Sequence    uint64
	EnqueuedAt  time.Time
	AvailableAt time.Time
}

// Delivery represents one delivery attempt of a Message to a consumer.
type Delivery struct {
	Message
	ReceiptHandle string
	LeaseUntil    time.Time
}
