# StreamBridge

[![StreamBridge CI](https://github.com/yoohooshantanu/streambridge/actions/workflows/ci.yml/badge.svg)](https://github.com/yoohooshantanu/streambridge/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/yoohooshantanu/streambridge)](https://goreportcard.com/report/github.com/yoohooshantanu/streambridge)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**StreamBridge** is a high-throughput, low-latency gRPC bidirectional streaming gateway engineered for real-time telephony and AI voice processing infrastructure. It ingests streaming audio frames from callers, coordinates concurrent AI workers (ASR transcription, real-time sentiment analysis, intent classification), and fans out live event streams to external watchers (CRMs, supervisor dashboards, analytics sinks).

---

## Key Capabilities

* **Bidirectional gRPC Streaming (`StreamCall`)**: Full duplex client-to-server and server-to-client streaming over HTTP/2 with independent goroutine isolation for `Recv()` and `Send()`.
* **Hash-Partitioned Worker Queues & Serial Chunk Ordering**: Partitioned actor channels keyed by `callID` (`hash(callID) % numWorkers`) eliminate cross-worker lock contention and strictly serialize chunk processing per call.
* **Per-Call Sequencing & Reassembly**: Monotonic sequence tracking (`nextExpectedSeq`) buffers out-of-order frames, guaranteeing that finalization (`is_final = true`) never races ahead of preceding frames.
* **Multi-Tenant Isolation**: Rigorous tenant ownership binding in `EventRouter` prevents cross-tenant access. Subscriptions (`WatchCall`) and post-call analytics (`GetCallSummary`) reject cross-tenant snooping with `codes.PermissionDenied`.
* **Zero-Wildcard Authentication Registry**: Centralized `KeyRegistry` binds authorized keys to tenant identities with zero prefix wildcards (eliminating prefix-based bypass risks).
* **Server-Side Event Fan-Out (`WatchCall`)**: Multi-tenant event hub enabling external services to observe live telephony streams with zero impact on caller stream latency.
* **Non-Blocking Backpressure Protection**: Bounded subscriber ring buffers with non-blocking drops to prevent slow consumers from lagging upstream voice pipelines.
* **Hermetic Testing via In-Memory `bufconn`**: 100% network-socket-free unit and integration test suite executing full gRPC interceptor chains without OS port allocation conflicts.
* **Zero-Allocation Telemetry**: Live Prometheus exposition (`/metrics`), `/healthz`, and runtime profiling (`net/http/pprof`) via an isolated HTTP sidecar.

---

## Architectural Scope & Deterministic AI Engines

> [!NOTE]
> **Transport Architecture vs. Model Inference**: StreamBridge is designed as a carrier-grade streaming network gateway focusing on bidirectional framing, backpressure handling, fan-out distribution, and tenant isolation.
>
> The built-in worker processors (ASR streaming transcriber, sentiment analyzer, and intent detector) are **deterministic in-memory streaming mock engines**. This architectural separation allows high-throughput performance benchmarking, hermetic integration testing, and predictable pipeline latency without external GPU clusters, network egress overhead, or third-party SaaS API credits. In production environments, these worker routines act as adapters forwarding raw frames to proprietary or vendor ASR/LLM inference endpoints.

---

## System Architecture

```
                       ┌──────────────────────────────────────────────────────────┐
                       │                       StreamBridge                       │
                       │                                                          │
Telephony Client ─────►│  gRPC Server (:50051)                                    │
(AudioChunk stream)    │    ├─ UnaryAuthInterceptor & StreamAuthInterceptor       │
                       │    │    └─► KeyRegistry (Strict Tenant Binding)          │
                       │    └─ Logging & Metrics Interceptors                     │
                       │                                                          │
                       │  StreamCall Handler (Bidirectional)                      │
                       │    ├─ Goroutine A: stream.Recv() loop (Ingests to EOF)   │
                       │    │    └─► Submit(hash(callID) % numWorkers)            │
                       │    │                                                     │
                       │    └─ Goroutine B: stream.Send() loop                   │
                       │         ▲                                                │
                       │         │                                                │
                       │  EventRouter (Tenant-Isolated Fan-Out Hub) ──────────────┼───┐
                       │    ├─ callTenants map[callID]tenantID                    │   │
                       │    ├─ Subscribers map[callID][]chan CallEvent            │   │
                       │    └─ Non-blocking select (backpressure drop)            │   │
                       │                                                          │   │
                       │  AIWorkerPool (Hash-Partitioned Workers)                 │   │
                       │    ├─ Worker 0..N-1 (Dedicated Per-Worker Queues)        │   │
                       │    ├─ Out-of-Order Sequence Reassembly Buffer            │   │
                       │    ├─ ASR Engine (Partial / Final transcripts) ──────────┤   │
                       │    ├─ Sentiment Analyzer (Positive/Neutral) ─────────────┤   │
                       │    └─ Intent Classifier (Inquiry/Booking) ───────────────┘   │
                       │                                                          │
Supervisor / CRM ─────►│  WatchCall Handler (Server Streaming)                   │
(WatchRequest)         │    └─ Enforces Tenant ID Match (codes.PermissionDenied)  │
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
  rpc StreamCall(stream AudioChunk) returns (stream CallEvent);
  rpc WatchCall(WatchRequest) returns (stream CallEvent);
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
  EVENT_CALL_ERROR         = 6;
}
```

---

## Concurrency & Thread-Safety Guarantees

### 1. Independent Recv/Send Goroutine Isolation
In gRPC Go, calling `stream.Send()` and `stream.Recv()` concurrently on the same stream is safe, but calling `stream.Send()` from multiple goroutines concurrently is **unsafe**. `StreamCall` enforces strict isolation:
- **Goroutine A** exclusively calls `stream.Recv()` and dispatches audio frames into the worker pool.
- **Goroutine B** exclusively reads from the per-call `eventCh` and calls `stream.Send()`.
- A shared `doneCh` coordination pattern ensures that whenever either half finishes or errors, the counterpart goroutine drains and terminates cleanly without leaking resources.

### 2. Hash-Partitioned Worker Queues & Bounded Sequence Reassembly
To eliminate lock contention and race conditions during concurrent audio chunk ingestion:
- Each worker goroutine in `AIWorkerPool` has an independent, non-shared channel (`workerQueues[workerID]`).
- Audio chunks are deterministically routed to a specific worker using `hash(callID) % numWorkers`.
- All chunks for a given call are processed by the exact same worker in strict order.
- **Bounded Out-of-Order Sequence Reassembly**: An out-of-order sequence reassembly buffer (`pendingChunks map[uint64]*WorkerJob`) holds any frames that arrive ahead of `nextExpectedSeq`. Frames are capped at `MaxPendingChunksPerCall = 64` and `MaxSequenceGap = 128`.
- **Explicit Caller Notification**: If an incoming frame exceeds the maximum sequence gap or the buffer is saturated, the frame is dropped and the client is immediately notified via an `EVENT_CALL_ERROR` event.
- **Deterministic Gap Timeout & Eviction**: If a sequence gap remains stalled without the missing frame arriving, the worker's background ticker aborts the stalled session after `DefaultGapTimeout` (2 seconds), emits an `EVENT_CALL_ERROR` with `sequence_gap_timeout`, terminates the gRPC stream with `codes.DataLoss`, and evicts worker session memory to prevent indefinite hanging or memory leakage.
- **Worker-Local Tombstone Protection (Zombie Session Prevention)**: When a call aborts due to a gap timeout, an entry is inserted into a worker-local `abortedCalls map[string]time.Time` cache (`AbortedCallTombstoneTTL = 60s`, capped at `MaxAbortedTombstones = 1024`). Any delayed/retransmitted audio frames arriving after the abort are dropped immediately without re-instantiating a new session expecting sequence 1. Because each worker exclusively owns its call partition, tombstone checking requires zero mutex locks.

### 3. Strict Tenant Isolation & Key Registry
- Incoming API keys are verified against a strict `KeyRegistry`. Wildcard prefixes (`strings.HasPrefix`) are strictly rejected.
- Each call is registered to its authenticated tenant.
- Cross-tenant subscriptions (`WatchCall`) and summary requests (`GetCallSummary`) are rejected with `codes.PermissionDenied`.

### 4. Lock Ordering & Send-on-Closed-Channel Prevention
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

All benchmarks executed via in-process `bufconn` listener (Windows amd64, AMD Ryzen 5 3500U, Go 1.23.12).

### 1. Sequential Stream Throughput
```
pkg: github.com/yoohooshantanu/streambridge/tests
BenchmarkStreamBridgeThroughput-8    4726    233320 ns/op    13952 B/op    227 allocs/op
```
* **~233 µs per full end-to-end stream session**, including HTTP/2 bidi stream initialization, 3 audio chunks, ASR/Sentiment/Intent processing, event fan-out, and clean response drain.

### 2. Concurrent Out-of-Order Streams Profiling & Optimization
A high-concurrency benchmark (`BenchmarkConcurrentStreamsWithGaps`) simulates 20 concurrent bidirectional gRPC voice calls delivering scrambled out-of-order audio frames.

Profiling (`go tool pprof`) revealed that dynamic `json.Marshal(map[string]interface{}{...})` calls inside `executeChunk` accounted for 18.0% of CPU runtime and 21.1% of heap allocations due to reflection, map sorting, and `interface{}` boxing.

**Optimization**:
1. Static payload caching (`positiveSentimentPayload`, `neutralSentimentPayload`, `intentDetectedPayload`).
2. Reflection-free formatted string templates for partial/final transcripts and error payloads.
3. Right-sized subscriber channel buffers from 512 to 128 items in `StreamCall`.

**Statistical Comparison (`benchstat -count=10 baseline.txt optimized.txt`)**:
```
goarch: amd64
pkg: github.com/yoohooshantanu/streambridge/tests
cpu: AMD Ryzen 5 3500U with Radeon Vega Mobile Gfx  
                            │ baseline.txt │            optimized.txt             │
                            │    sec/op    │    sec/op     vs base                │
ConcurrentStreamsWithGaps-8   5.316m ± 23%   4.015m ± 10%  -24.46% (p=0.000 n=10)

                            │ baseline.txt │            optimized.txt             │
                            │     B/op     │     B/op      vs base                │
ConcurrentStreamsWithGaps-8   896.5Ki ± 0%   683.4Ki ± 0%  -23.77% (p=0.000 n=10)

                            │ baseline.txt │            optimized.txt            │
                            │  allocs/op   │  allocs/op   vs base                │
ConcurrentStreamsWithGaps-8    15.73k ± 0%   12.44k ± 0%  -20.90% (p=0.000 n=10)
```
* **Latency**: Reduced from 5.316 ms to 4.015 ms (**-24.46%** faster).
* **Heap Allocated**: Reduced from 896.5 KiB to 683.4 KiB (**-23.77%** less memory).
* **Allocations**: Reduced from 15.73k to 12.44k (**-20.90%** fewer allocations per operation).

---

## Getting Started

### Prerequisites
- Go 1.23+
- Make (optional)
- Docker (optional)

### Running Tests Locally
```bash
go test -v -race ./tests/...
```

### Starting the Server
```bash
go run cmd/server/main.go
```
The gRPC streaming server listens on `:50051`, and the HTTP metrics/pprof sidecar listens on `:8083`.

---

## License
MIT License. Created by Shantanu Juvekar (`santanujuvekar@gmail.com`).
