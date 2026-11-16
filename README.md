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
- **Worker-Local Tombstone Protection (Zombie Session Prevention)**: When a call aborts due to a gap timeout, an entry is recorded in a worker-local `abortedCalls map[string]time.Time` cache (`AbortedCallTombstoneTTL = 60s`, capped at `MaxAbortedTombstones = 1024`). Any delayed or retransmitted audio frames arriving after the abort are dropped immediately without re-instantiating a new session expecting sequence 1.
  - **Share-Nothing Concurrency**: Because workers exclusively own their partitioned calls via `hash(callID) % numWorkers`, tombstone checking requires zero mutex locks and zero atomic operations.
  - **Capacity Overflow (FIFO/LRU Eviction)**: When the map reaches 1024 entries, `RecordAbortedCall` evicts the oldest recorded tombstone by timestamp, ensuring newly aborted calls are always protected without memory leakage. The 60-second TTL is an engineered operational window exceeding typical carrier SIP/WebRTC renegotiation and teardown windows.

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

All benchmarks executed via in-process `bufconn` listener (Windows amd64, AMD Ryzen 5 3500U, Go 1.23.12, run without `-race`).

### 1. Sequential Stream Throughput
```
pkg: github.com/yoohooshantanu/streambridge/tests
BenchmarkStreamBridgeThroughput-8    4726    233320 ns/op    13952 B/op    227 allocs/op
```
* **~233 µs per full end-to-end stream session**, including HTTP/2 bidi stream initialization, 3 audio chunks, ASR/Sentiment/Intent processing, event fan-out, and clean response drain.

### 2. Concurrent Out-of-Order Streams Profiling & Optimization
A high-concurrency benchmark (`BenchmarkConcurrentStreamsWithGaps`) simulates 20 concurrent bidirectional gRPC voice calls delivering scrambled out-of-order audio frames.

**Profiling Diagnostics (`go tool pprof`)**:
Profiling CPU and heap allocation profiles revealed two primary contributors:
1. Dynamic `json.Marshal(map[string]interface{}{...})` calls inside `executeChunk` accounted for 18.0% of CPU runtime and 21.1% of heap allocations due to reflection, dynamic map sorting, and `interface{}` boxing.
2. In `StreamCall`, allocating a 512-item subscriber channel buffer per stream created ~4 KB of channel pointer storage per call. In the benchmark's high-turnover loop across thousands of calls, this cumulatively accounted for 419 MB of transient allocations.

**Optimizations Applied**:
1. **Strongly-Typed JSON Structs**: Replaced dynamic `map[string]interface{}` with concrete structs (`PartialTranscriptEvent`, `FinalTranscriptEvent`, `CallErrorEvent`), eliminating map creation and key sorting while guaranteeing RFC 8259-compliant escaping for quotes, backslashes, newlines, and UTF-8 emojis.
2. **Static Invariant Payload Caching**: Pre-serialized immutable event types (`intentDetectedPayload`, `positiveSentimentPayload`, `neutralSentimentPayload`, `{"status":"completed"}`), incurring 0 heap allocations.
3. **Right-Sized Channel Buffers**: Reduced per-stream subscriber buffers from 512 to 128 items in `StreamCall`.

> [!NOTE]
> **Performance Contribution & Backpressure Trade-off**:
> Both the JSON serialization redesign and the subscriber channel buffer reduction contributed to the overall benchmark gains. Reducing the subscriber channel capacity from 512 to 128 lowers baseline memory consumption per call (~1 KB vs ~4 KB pointer table), but reduces absorption capacity for slow subscribers: lagging downstream watchers will hit non-blocking backpressure drops (`metrics.DefaultMetrics.IncDropped()`) sooner during sustained downstream stalls.

**Exact Benchmark Commands**:
```bash
go test -run='^$' -bench='BenchmarkConcurrentStreamsWithGaps' -benchmem -count=10 ./tests/... > baseline.txt
go test -run='^$' -bench='BenchmarkConcurrentStreamsWithGaps' -benchmem -count=10 ./tests/... > optimized.txt
benchstat baseline.txt optimized.txt
```

**Statistical Comparison (`benchstat -count=10 baseline.txt optimized.txt`)**:
```
goarch: amd64
pkg: github.com/yoohooshantanu/streambridge/tests
cpu: AMD Ryzen 5 3500U with Radeon Vega Mobile Gfx  
                            │ baseline.txt │            optimized.txt             │
                            │    sec/op    │    sec/op     vs base                │
ConcurrentStreamsWithGaps-8   5.125m ± 18%   4.370m ± 12%  -14.72% (p=0.003 n=10)

                            │ baseline.txt │            optimized.txt             │
                            │     B/op     │     B/op      vs base                │
ConcurrentStreamsWithGaps-8   895.7Ki ± 0%   712.8Ki ± 0%  -20.42% (p=0.000 n=10)

                            │ baseline.txt │            optimized.txt            │
                            │  allocs/op   │  allocs/op   vs base                │
ConcurrentStreamsWithGaps-8    15.73k ± 0%   12.97k ± 0%  -17.57% (p=0.000 n=10)
```
* **Latency**: Reduced from 5.125 ms to 4.370 ms (**-14.72%** faster per operation, $p=0.003$).
* **Memory Allocated**: Reduced from 895.7 KiB to 712.8 KiB (**-20.42%** less memory per operation, $p=0.000$).
* **Allocations**: Reduced from 15.73k to 12.97k (**-17.57%** fewer allocations per operation, $p=0.000$).

---

## High-Concurrency Load Testing & Saturation Analysis

A dedicated load generator (`cmd/loadgen`) drives realistic multi-stream bidirectional VoIP traffic (20ms PCM audio frames at 50 pps per call) across configurable concurrency steps (50, 100, 200, 400, and 800 concurrent calls) to characterize real-time streaming boundaries, queue-wait inflection points, and resource utilization.

### Test Environment & Methodology Disclaimers
* **Hardware**: AMD Ryzen 5 3500U with Radeon Vega Mobile Gfx (4 Physical Cores, 8 Logical Processors), 8 GB RAM.
* **Operating System**: Windows 11 Home Single Language (64-bit).
* **Toolchain**: `go version go1.27.0 windows/amd64` (module target: `go 1.23.0`).
* **Co-Location Disclaimer**: The load generator and the StreamBridge server were co-located on the same physical host and communicated over the local loopback interface (`127.0.0.1`). Both processes directly competed for CPU cores, Go runtime scheduler time slices, and OS network socket buffers. These benchmarks measure local process behavior under contention and make **no claims of production capacity or cloud deployment throughput**.
* **Generator Self-Check & Timing Precision**: Frame pacing uses Windows multimedia high-resolution timer interrupt alignment (`timeBeginPeriod(1)`) to avoid Windows default 15.6ms scheduler quantum jitter. The generator tracks tick arrival lateness relative to scheduled 20ms intervals; ticks delayed >5ms advance schedule without catch-up bursting. A step is marked throttled if late ticks exceed 2% of offered packets.
* **Run Discipline**: Stream starts are staggered over 1.5 seconds (`-stagger-window=1500ms`), the first 4 seconds of each step are discarded as warmup (`-warmup-duration=4s`), and between steps the generator waits until `streambridge_active_streams == 0`. The server binary is restarted cleanly between each of the 3 repeated benchmark runs. Metrics (p50/p95/p99 queue-wait and processing latency, memory, goroutines) are extracted directly from the server's Prometheus registry (`/metrics`), not from the client.

### Load Test Commands
```powershell
# Build binaries
go build -o bin/streambridge.exe ./cmd/server
go build -o bin/loadgen.exe ./cmd/loadgen

# Execute 3-run automated load test with auto-restart and pprof capture at knee
.\bin\loadgen.exe -steps="50,100,200,400,800" -step-duration=15s -warmup-duration=4s -stagger-window=1500ms -runs=3 -auto-restart=true -profile-dir=./loadtest_profiles
```

### Aggregated Results (3 Runs, `[min - max]` Ranges)

| Calls | Target PPS | Throughput (pps) | Queue Wait (p99) | Proc Time (p99) | Client RTT (p99) | Errors | RSS Memory | Goroutines | Notes |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---|
| **50** | 2,500 | 2,494 - 2,505 | 1.63ms - 1.72ms | 0.83ms - 0.90ms | 4.34ms - 12.03ms | 0 - 0 | 21.1MB - 21.6MB | 37 - 37 | Linear throughput, sub-2ms queue wait |
| **100** | 5,000 | 4,986 - 5,010 | 0.99ms - 2.48ms | 0.87ms - 0.90ms | 6.20ms - 61.09ms | 0 - 0 | 24.0MB - 24.7MB | 37 - 37 | Linear scaling, stable worker queues |
| **200** | 10,000 | 9,908 - 10,007 | 4.29ms - 14.44ms | 0.86ms - 0.89ms | 63.15ms - 99.91ms | 0 - 0 | 29.3MB - 30.4MB | 37 - 37 | **Knee Threshold**: Queue wait reaches 10ms boundary |
| **400** | 20,000 | 19,097 - 19,986 | 29.61ms - 45.25ms | 0.81ms - 0.85ms | 101.49ms - 249.31ms | 0 - 0 | 38.9MB - 40.9MB | 37 - 37 | **Saturated**: Queue wait rises 30x over proc time |
| **800** | 40,000 | 31,831 - 34,207 | 198.67ms - 300.20ms | 0.70ms - 0.77ms | 955.38ms - 2,805.48ms | 0 - 0 | 46.8MB - 64.1MB | 37 - 37 | Host CPU saturated; generator throttled |

*\* Note: Knee detected between 200 and 400 concurrent calls when p99 queue wait exceeded 10ms (half the 20ms frame duration) while chunk processing execution time remained flat.*

### Saturation Bottleneck Analysis (`pprof`)

CPU profiling captured during sustained traffic at the saturation knee (200–400 concurrent calls) shows that packet processing execution time inside the workers remained virtually constant at 0.70ms–0.90ms, while packet queue wait time climbed steeply from 1.6ms at 50 calls to 45.2ms at 400 calls and 300ms at 800 calls. Analyzing `cpu_knee_200calls_r1.pprof` via `go tool pprof` confirms that the primary CPU consumer was gRPC framing and outbound network socket I/O (`google.golang.org/grpc/internal/transport.(*loopyWriter).run`, `internal/poll.(*FD).execIO`, and `syscall.WSASend` accounting for 28.3% of total CPU time), followed by runtime channel locking and goroutine scheduling (`runtime.semasleep`, `runtime.selectgo`, `runtime.lock2` accounting for 17.8% of CPU). Because the 4 partitioned AI workers share 4 physical CPU cores with the incoming gRPC reader goroutines and socket writers, packet arrival rates exceeding ~10,000 pps caused worker channel queues (`workerQueues[workerIdx]`) to accumulate backlogs, resulting in queue wait time escalating well before internal processing execution time degraded.

### Raw Histogram Bucket Counts Appendix (Run 1)

<details>
<summary><b>Click to expand raw cumulative histogram bucket counts per concurrency step</b></summary>

#### Step 1: 50 Concurrent Calls (2,500 offered pps)
```
Queue Wait Raw Buckets:
  le <= 0.0001s  : 25,148
  le <= 0.0005s  : 25,274
  le <= 0.0010s  : 26,998
  le <= 0.0025s  : 27,333
  le <= 0.0050s  : 27,397
  le <= 0.0100s  : 27,432
  le <= 0.0250s  : 27,434
  le <= +Inf     : 27,434

Processing Raw Buckets:
  le <= 0.0001s  : 26,426 (96.3% <= 100µs)
  le <= 0.0005s  : 26,492
  le <= 0.0010s  : 27,380
  le <= 0.0025s  : 27,417
  le <= 0.0100s  : 27,433
  le <= +Inf     : 27,434
```

#### Step 2: 100 Concurrent Calls (5,000 offered pps)
```
Queue Wait Raw Buckets:
  le <= 0.0001s  : 51,427
  le <= 0.0005s  : 51,662
  le <= 0.0010s  : 54,645
  le <= 0.0025s  : 54,977
  le <= 0.0050s  : 55,022
  le <= 0.0100s  : 55,069
  le <= 0.0250s  : 55,111
  le <= +Inf     : 55,111

Processing Raw Buckets:
  le <= 0.0001s  : 53,036 (96.2% <= 100µs)
  le <= 0.0005s  : 53,123
  le <= 0.0010s  : 55,010
  le <= 0.0025s  : 55,092
  le <= 0.0100s  : 55,111
  le <= +Inf     : 55,111
```

#### Step 3: 200 Concurrent Calls (10,000 offered pps)
```
Queue Wait Raw Buckets:
  le <= 0.0001s  : 102,061
  le <= 0.0005s  : 102,269
  le <= 0.0010s  : 105,684
  le <= 0.0025s  : 107,155
  le <= 0.0050s  : 107,980
  le <= 0.0100s  : 108,619
  le <= 0.0250s  : 109,321
  le <= 0.0500s  : 109,492
  le <= +Inf     : 109,522

Processing Raw Buckets:
  le <= 0.0001s  : 106,518 (97.3% <= 100µs)
  le <= 0.0005s  : 106,637
  le <= 0.0010s  : 109,125
  le <= 0.0025s  : 109,453
  le <= 0.0100s  : 109,510
  le <= +Inf     : 109,522
```

#### Step 4: 400 Concurrent Calls (20,000 offered pps)
```
Queue Wait Raw Buckets:
  le <= 0.0001s  : 169,759
  le <= 0.0005s  : 170,377
  le <= 0.0010s  : 175,531
  le <= 0.0025s  : 184,542
  le <= 0.0050s  : 191,657
  le <= 0.0100s  : 200,038
  le <= 0.0250s  : 210,734
  le <= 0.0500s  : 214,419
  le <= 0.1000s  : 215,320
  le <= 0.2500s  : 215,878
  le <= +Inf     : 215,878

Processing Raw Buckets:
  le <= 0.0001s  : 212,228 (98.3% <= 100µs)
  le <= 0.0005s  : 212,445
  le <= 0.0010s  : 214,238
  le <= 0.0025s  : 215,572
  le <= 0.0100s  : 215,729
  le <= 0.0500s  : 215,863
  le <= +Inf     : 215,872
```

#### Step 5: 800 Concurrent Calls (40,000 offered pps)
```
Queue Wait Raw Buckets:
  le <= 0.0001s  : 191,261
  le <= 0.0005s  : 192,102
  le <= 0.0010s  : 203,164
  le <= 0.0025s  : 224,052
  le <= 0.0050s  : 244,927
  le <= 0.0100s  : 273,237
  le <= 0.0250s  : 313,017
  le <= 0.0500s  : 342,404
  le <= 0.1000s  : 364,618
  le <= 0.2500s  : 375,311
  le <= 0.5000s  : 376,353
  le <= +Inf     : 376,353

Processing Raw Buckets:
  le <= 0.0001s  : 371,090 (98.6% <= 100µs)
  le <= 0.0005s  : 371,344
  le <= 0.0010s  : 373,729
  le <= 0.0025s  : 375,778
  le <= 0.0100s  : 376,049
  le <= 0.0500s  : 376,316
  le <= +Inf     : 376,350
```
</details>

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
