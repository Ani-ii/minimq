# minimq

A lightweight, in-memory message broker written from scratch in Go — durable queuing, concurrent producer/consumer access, and work-queue-style delivery, built on Go's native concurrency primitives.

## Why this exists

Most message-broker usage is "point a client library at a managed service and call it a day." This project goes the other direction: implement the actual queuing and concurrency primitives by hand, to build a real understanding of how these systems work under the hood — not just how to use them.

## Design

- **Delivery model:** work-queue style (like a Kafka consumer group) — each message is delivered to exactly one consumer, not broadcast to all listeners. Pub/sub-style broadcast is intentionally out of scope for this project.
- **Queues:** created implicitly by the first producer to publish to a given name. Consumers only operate on queues that already exist — this keeps queue lifecycle ownership on the producer side.
- **Concurrency:** each queue is backed by a native Go buffered channel, which gives FIFO ordering and safe concurrent send/receive for free. A `sync.RWMutex` protects the shared map of queue name → channel, since Go maps aren't safe for concurrent access on their own.
- **Non-blocking publish:** publishing to a full queue fails fast (via `select`/`default`) rather than blocking the caller indefinitely.
- **Wire protocol:** a simple length-prefixed framing over TCP, with JSON payloads. The broker treats message bodies as opaque bytes — it never inspects or parses payload content. Producers and consumers own their own serialization.

## Status

- [x] Core `Broker` struct: queue creation, `Publish`, `Consume` — in-process, concurrency-safe
- [ ] TCP server accepting producer/consumer connections
- [ ] Length-prefixed JSON wire protocol
- [ ] Example producer/consumer client programs
- [ ] Message acknowledgment
- [ ] Persistence

## Running

```bash
go run main.go
```

*(TCP server and example clients coming next — currently exercised via direct Go function calls / tests.)*

## What this is not

This isn't a production-grade broker, and doesn't aim to be. It skips exchanges, routing keys, message acknowledgment, persistence, and clustering — the goal is a correct, well-reasoned implementation of the core primitives, not feature completeness.
