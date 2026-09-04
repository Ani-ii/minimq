package main

import (
	"fmt"
	"sync"
)

// Message is the unit of data moved through the broker. The broker treats
// Body as an opaque payload — it never inspects or parses its contents.
// Producers and consumers own their own serialization format.
type Message struct {
	Body []byte
}

// Broker holds all queues in memory. Each queue is backed by a native Go
// channel, which gives FIFO ordering and safe concurrent send/receive for
// free. The map itself is protected separately, since Go maps are not safe
// for concurrent access on their own.
type Broker struct {
	mu     sync.RWMutex
	queues map[string]chan Message
}

// NewBroker returns a ready-to-use Broker.
func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]chan Message),
	}
}

const defaultQueueCapacity = 1000

// getOrCreateQueue returns the channel for queueName, creating it if it
// doesn't exist yet. Uses the double-checked locking pattern: an initial
// unprotected existence check is done by the caller under a read lock: this
// function itself always acquires the write lock and re-checks, since two
// callers could otherwise race to create the same queue.
func (b *Broker) getOrCreateQueue(queueName string) chan Message {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.queues[queueName]
	if !ok {
		ch = make(chan Message, defaultQueueCapacity)
		b.queues[queueName] = ch
		fmt.Printf("created new queue: %s\n", queueName)
	}
	return ch
}

// Publish sends msg to queueName, creating the queue if it doesn't exist.
// It never blocks: if the queue's buffer is full, Publish fails immediately
// and returns false rather than waiting for space.
func (b *Broker) Publish(queueName string, msg Message) bool {
	b.mu.RLock()
	ch, ok := b.queues[queueName]
	b.mu.RUnlock()

	if !ok {
		ch = b.getOrCreateQueue(queueName)
	}

	select {
	case ch <- msg:
		return true
	default:
		return false
	}
}

// Consume blocks until a message is available on queueName and returns it.
// It returns (Message{}, false) immediately if the queue doesn't exist —
// by design, a queue must already have been created by a producer's
// Publish call before a consumer can read from it.
func (b *Broker) Consume(queueName string) (Message, bool) {
	b.mu.RLock()
	ch, ok := b.queues[queueName]
	b.mu.RUnlock()

	if !ok {
		fmt.Printf("consume failed: queue %q does not exist\n", queueName)
		return Message{}, false
	}

	msg := <-ch
	return msg, true
}

func main() {
	// TCP server wiring goes here next — for now the broker is exercised
	// directly via Publish/Consume calls, e.g. from tests.
	b := NewBroker()
	_ = b
}
