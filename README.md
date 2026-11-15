# StreamBridge

[![StreamBridge CI](https://github.com/yoohooshantanu/streambridge/actions/workflows/ci.yml/badge.svg)](https://github.com/yoohooshantanu/streambridge/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/yoohooshantanu/streambridge)](https://goreportcard.com/report/github.com/yoohooshantanu/streambridge)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**StreamBridge** is a high-throughput, low-latency gRPC bidirectional streaming gateway engineered for real-time telephony and AI voice processing infrastructure. It ingests streaming audio frames from callers, coordinates concurrent AI workers (ASR transcription, real-time sentiment analysis, intent classification), and fans out live event streams to external watchers (CRMs, supervisor dashboards, analytics sinks).

---

## Key Capabilities

* **Bidirectional gRPC Streaming (`StreamCall`)**: Full duplex client-to-server and server-to-client streaming over HTTP/2 with independent goroutine isolation for `Recv()` and `Send()`.
* **Server-Side Event Fan-Out (`WatchCall`)**: Multi-tenant event hub enabling external services to observe live telephony streams with zero impact on caller stream latency.
* **Non-Blocking Backpressure Protection**: Bounded subscriber ring buffers with non-blocking drops to prevent slow consumers from lagging upstream voice pipelines.
* **Hermetic Testing via In-Memory `bufconn`**: 100% network-socket-free unit and integration test suite executing full gRPC interceptor chains without OS port allocation conflicts.
* **gRPC Interceptor Chains**: Structured metadata authentication (`x-api-key`), execution latency instrumentation, and status code audit logging.
* **Native gRPC Health Checking**: Fully implements the standard `grpc_health_v1` protocol (`Check` and `Watch`).
* **Zero-Allocation Telemetry**: Live Prometheus exposition (`/metrics`), `/healthz`, and runtime profiling (`net/http/pprof`) via an isolated HTTP sidecar.

---

## System Architecture

```
                       ┌──────────────────────────────────────────────────────────┐
                       │                       StreamBridge                       │
                       │                                                          │
Telephony Client ─────►│  gRPC Server (:50051)                                    │
(AudioChunk stream)    │    ├─ UnaryAuthInterceptor & StreamAuthInterceptor       │
                       │    └─ Logging & Metrics Interceptors                     │
                       │                                                          │
                       │  StreamCall Handler (Bidirectional)                      │
                       │    ├─ Goroutine A: stream.Recv() loop                   │
                       │    │    └─► Pushes to AIWorkerPool queue                 │
                       │    │                                                     │
                       │    └─ Goroutine B: stream.Send() loop                   │
                       │         ▲                                                │
                       │         │                                                │
                       │  EventRouter (Fan-Out Hub) ◄─── Emits CallEvents ───┐   │
                       │    ├─ Subscribers map[callID][]chan CallEvent       │   │
                       │    └─ Non-blocking select (backpressure drop)       │   │
                       │                                                     │   │
                       │  AIWorkerPool (Bounded Goroutine Pool)              │   │
                       │    ├─ ASR Engine (Partial / Final transcripts) ─────┤   │
                       │    ├─ Sentiment Analyzer (Positive/Neutral) ────────┤   │
                       │    └─ Intent Classifier (Inquiry/Booking) ──────────┘   │
                       │                                                          │
Supervisor / CRM ─────►│  WatchCall Handler (Server Streaming)                   │
(WatchRequest)         │    └─ Streams CallEvents to external subscribers        │
                       └──────────────────────────────────────────────────────────┘
                                                 │
                               HTTP Sidecar (:8083) ──► /metrics, /healthz, pprof
```

---

## gRPC Protocol Specification (`proto/streambridge.proto`)

```protobuf
syntax = "proto3";

package streambridge.v1;

option go_package = "github.com/yoohooshantanu/streambridge/internal/pb";

service StreamBridge {
  // Bidirectional audio chunk stream with real-time event responses
  rpc StreamCall(stream AudioChunk) returns (stream CallEvent);

  // Server-side stream subscribing to a live call's fanned-out events
  rpc WatchCall(WatchRequest) returns (stream CallEvent);

  // Unary query retrieving post-call summary and aggregated analytics
  rpc GetCallSummary(SummaryRequest) returns (CallSummary);
}

message AudioChunk {
  string call_id   = 1;
  string tenant_id = 2;
  uint64 sequence  = 3;
  bytes  data      = 4;
  bool   is_final  = 5;
  int64  timestamp = 6;
}

message CallEvent {
  string    call_id   = 1;
  EventType type      = 2;
  string    payload   = 3;
  uint64    sequence  = 4;
  int64     timestamp = 5;
}

enum EventType {
  EVENT_UNKNOWN            = 0;
  EVENT_TRANSCRIPT_PARTIAL = 1;
  EVENT_TRANSCRIPT_FINAL   = 2;
  EVENT_SENTIMENT          = 3;
  EVENT_INTENT_DETECTED    = 4;
  EVENT_CALL_ENDED         = 5;
}
```

---

## Concurrency & Thread-Safety Guarantees

### 1. Independent Recv/Send Goroutine Isolation
In gRPC Go, calling `stream.Send()` and `stream.Recv()` concurrently on the same stream is safe, but calling `stream.Send()` from multiple goroutines concurrently is **unsafe**. `StreamCall` enforces strict isolation:
- **Goroutine A** exclusively calls `stream.Recv()` and dispatches audio frames into the worker pool.
- **Goroutine B** exclusively reads from the per-call `eventCh` and calls `stream.Send()`.
- A shared `doneCh` coordination pattern ensures that whenever either half finishes or errors, the counterpart goroutine drains and terminates cleanly without leaking resources.

### 2. Lock Ordering & Send-on-Closed-Channel Prevention
To prevent `send on closed channel` panics during subscriber churn, `EventRouter.Publish()` holds `r.mu.RLock()` during channel dispatch:
```go
func (r *EventRouter) Publish(callID string, event *pb.CallEvent) {
    r.mu.RLock()
    defer r.mu.RUnlock()

    if r.closed {
        return
    }

    for _, ch := range r.subscribers[callID] {
        select {
        case ch <- event:
        default:
            metrics.DefaultMetrics.IncDropped()
        }
    }
}
```
Because `unsubscribe()` must acquire `r.mu.Lock()` before removing and closing `ch`, no `Publish()` invocation can attempt to send to a channel currently being closed or already closed.

---

## Benchmark Results

Real benchmark executed via in-process `bufconn` listener (Windows amd64, AMD Ryzen 5 3500U):

```
pkg: github.com/yoohooshantanu/streambridge/tests
BenchmarkStreamBridgeThroughput-8    1911    736811 ns/op    30999 B/op    479 allocs/op
```

* **~736.8 µs per full end-to-end stream session**, including:
  * HTTP/2 bidi stream initialization
  * Ingesting 3 audio chunks
  * AI worker pipeline processing (ASR, Sentiment, Intent)
  * Event fan-out dispatch
  * Client stream closure and full response drain

---

## Getting Started

### Prerequisites
- Go 1.23+
- (Optional) Docker & Docker Compose

### Running Locally

```bash
# Clone the repository
git clone https://github.com/yoohooshantanu/streambridge.git
cd streambridge

# Build binary
go build -o bin/streambridge ./cmd/server

# Start server
./bin/streambridge
# gRPC Gateway: :50051
# HTTP Metrics & Healthz: :8083
```

### Running Tests

```bash
# Run hermetic tests with race detector
go test -v -race ./tests/...

# Run throughput benchmark
go test -bench=BenchmarkStreamBridgeThroughput -benchmem -run=^$ ./tests/...
```

---

## Endpoints

| Port | Protocol | Path / Method | Description |
|---|---|---|---|
| `:50051` | gRPC | `StreamBridge/StreamCall` | Bidirectional audio & event streaming |
| `:50051` | gRPC | `StreamBridge/WatchCall` | Server-streaming live event observer |
| `:50051` | gRPC | `StreamBridge/GetCallSummary`| Post-call analytics lookup |
| `:50051` | gRPC | `grpc.health.v1.Health/Check`| Standard gRPC health check |
| `:8083` | HTTP | `GET /healthz` | Kubernetes liveness/readiness probe |
| `:8083` | HTTP | `GET /metrics` | Prometheus exposition endpoint |
| `:8083` | HTTP | `GET /debug/pprof/` | Runtime profiling index |

---

## License

MIT © Shantanu Juvekar (`yoohooshantanu`)
