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

| Calls | Target PPS | Achieved PPS | Queue Wait (p99) | Proc Time (p99) | Mean Svc Time | Implied Util ($\rho$) | Client RTT (p99) | Server CPU% | Host CPU% | Mid Goroutines | Notes |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---|
| **50** | 2,500 | 2,502 - 2,505 | 1.21ms - 1.77ms | 0.82ms - 0.89ms | 18.3µs - 26.7µs | 1.1% - 1.7% | 5.18ms - 8.60ms | 71.4% - 96.4% | 60.5% - 82.3% | 137 - 137 | Linear throughput, sub-2ms queue wait |
| **100** | 5,000 | 5,008 - 5,011 | 1.01ms - 7.39ms | 0.83ms - 0.88ms | 19.3µs - 30.3µs | 2.4% - 3.8% | 6.92ms - 79.05ms | 114.6% - 137.1% | 75.1% - 90.5% | 236 - 237 | Linear scaling, stable worker queues |
| **200** | 10,000 | 9,691 - 9,891 | 8.55ms - 33.98ms | 0.83ms - 0.89ms | 20.9µs - 33.5µs | 5.2% - 8.1% | 56.82ms - 214.60ms | 161.4% - 190.8% | 93.1% - 98.7% | 437 - 437 | **Knee Threshold**: Queue wait crosses 10ms; client RTT rises |
| **400** | 20,000 | 16,331 - 20,029 | 17.35ms - 231.29ms | 0.67ms - 0.80ms | 19.8µs - 57.7µs | 9.9% - 23.5% | 75.49ms - 1,163.25ms | 150.7% - 300.7% | 99.9% - 100.0% | 837 - 837 | **Host Saturated**: Host CPU hits 100%; generator throttled |
| **800** | 40,000 | 22,202 - 38,570 | 88.10ms - 244.93ms | 0.53ms - 0.63ms | 25.2µs - 52.9µs | 24.3% - 29.4% | 180.15ms - 3,053.17ms | 162.9% - 346.4% | 99.8% - 100.0% | 1,637 - 1,637 | Severe host exhaustion; client timeouts |

*\* Note: Goroutines are sampled directly from `go_goroutines` mid-step during steady-state traffic, reflecting active per-stream read/write routines scaling linearly with concurrency.*

---

### Understanding the Caller Knee vs. Server-Side Knee

#### What Client RTT Measures
**Client RTT** measures the total round-trip elapsed duration from the exact instant the load generator initiates `stream.Send()` for an audio chunk until receipt of the corresponding acknowledgment `CallEvent` on `stream.Recv()`. This end-to-end span encompasses:
1. Client-side HTTP/2 frame buffering and gRPC transport scheduling
2. Operating system network socket write (`WSASend`) and loopback transit
3. Server-side gRPC ingest, stream demultiplexing, and frame validation
4. Partitioned worker queue wait in `workerQueues[workerIdx]`
5. In-worker sequencing, reassembly, and JSON event formatting
6. Event router fan-out to subscriber channels
7. Server-side outbound response serialization (`loopyWriter.run` / `WSASend`)
8. Loopback transit and client-side unmarshaling.

#### Divergence at the Knee
Because Client RTT captures the entire socket and transport pipeline, **a caller perceives the saturation knee significantly earlier than server-side execution metrics indicate**:
* At **200 concurrent calls**, server-side chunk execution time remains completely flat (<1ms, mean service time ~21µs–34µs), and server worker queue wait is still near 8.5ms–34.0ms. However, client-observed RTT p99 climbs to **56.8ms–214.6ms**, crossing real-time interactive thresholds.
* At **400 concurrent calls**, host CPU reaches 100% saturation. While worker execution loops continue to process chunks in <1ms once dequeued, client RTT escalates to **75ms–1,163ms** due to socket transport contention and OS thread scheduling delays.

---

### Worker Pool Scalability Experiment (4 vs. 8 vs. 16 Workers)

To determine whether the saturation knee moves with worker count, steps 200 and 400 were re-evaluated across 4, 8, and 16 worker configurations (3 repeated runs each):

| Config | Calls | Offered PPS | Achieved PPS | Queue Wait (p99) | Mean Svc Time | Implied Util ($\rho$) | Client RTT (p99) | Server CPU% | Host CPU% |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **4 Workers** | 200 | 10,000 | 9,934 - 10,045 | 5.17ms - 8.71ms | 21.7µs - 23.7µs | 5.5% - 5.9% | 32.95ms - 59.31ms | 191.0% - 228.5% | 91.3% - 97.2% |
| **4 Workers** | 400 | 20,000 | 18,515 - 19,643 | 87.46ms - 158.69ms | 35.0µs - 40.3µs | 16.7% - 19.8% | 254.37ms - 265.05ms | 228.6% - 253.0% | 99.5% - 99.9% |
| **8 Workers** | 200 | 10,000 | 9,815 - 10,027 | 0.95ms - 12.16ms | 17.3µs - 25.0µs | 4.3% - 6.1% | 11.55ms - 62.98ms | 157.1% - 230.2% | 59.8% - 97.1% |
| **8 Workers** | 400 | 20,000 | 17,039 - 20,075 | 6.63ms - 242.33ms | 18.4µs - 63.8µs | 9.2% - 27.2% | 42.09ms - 547.76ms | 176.9% - 307.5% | 90.8% - 100.0% |
| **16 Workers** | 200 | 10,000 | 9,901 - 10,027 | 2.41ms - 16.14ms | 20.4µs - 26.9µs | 5.1% - 6.7% | 35.21ms - 122.77ms | 166.7% - 204.2% | 89.1% - 99.2% |
| **16 Workers** | 400 | 20,000 | 18,952 - 19,837 | 39.00ms - 47.37ms | 24.9µs - 36.3µs | 12.3% - 17.2% | 153.13ms - 203.38ms | 231.9% - 268.8% | 99.4% - 99.5% |

#### Knee Shift Analysis
Increasing the worker pool from 4 to 8 or 16 **does not significantly move the saturation knee**:
1. **Low Worker Utilization**: Mean service time per chunk is ~18µs–40µs, meaning implied worker utilization ($\rho = \frac{\text{Throughput} \times S_{\text{mean}}}{\text{numWorkers}}$) remains low (4%–7% at 200 calls, 12%–27% at 400 calls). The workers themselves have substantial idle headroom.
2. **Host Core & Transport Bound**: On this 4-core machine, the CPU bottleneck at $\ge 400$ calls is driven by gRPC HTTP/2 framing, socket I/O (`loopyWriter.run`, `WSASend`), and OS context switching. Because total host CPU reaches 99%–100%, adding more worker goroutines does not increase processing capacity; it slightly increases scheduler thread contention.

---

### Saturation Bottleneck Diagnostics (`pprof`)

Profiling was captured during sustained traffic at the saturation knee (200–400 concurrent calls) with block and mutex profiling enabled (`runtime.SetBlockProfileRate(10000)`, `runtime.SetMutexProfileFraction(5)`):

* **CPU Profile (`cpu_knee_*.pprof`)**: The primary CPU consumers are gRPC HTTP/2 framing and outbound network socket I/O (`google.golang.org/grpc/internal/transport.(*loopyWriter).run`, `syscall.WSASend` accounting for ~28% of CPU), followed by runtime channel operations and goroutine scheduling (`runtime.selectgo`, `runtime.semasleep`, `runtime.lock2` accounting for ~18% of CPU).
* **Block Profile (`block_knee_*.pprof`)**: `runtime.selectgo` accounts for 99.31% of blocking delay. Specifically, 50.17% of blocking time is in `StreamCall` waiting for downstream event fan-out or stream context cancellation, and 45.19% is in `GenericServerStream.Recv` waiting for network frame delivery.
* **Mutex Profile (`mutex_knee_*.pprof`)**: Mutex wait delay is concentrated in gRPC stream serialization (`GenericServerStream.Send` -> `http2Server.Write` / `loopyWriter`), which accounts for ~48.6% of mutex wait time across concurrent streams. In contrast, worker pool submission and dispatch locks account for <6% of mutex wait time.
* **Conclusion**: These diagnostic profiles are **consistent with** host CPU and network transport saturation bounding throughput and escalating queue wait time before worker execution capacity is exhausted.

---

### `hashCallID` Microbenchmark & Register Inlining

Partitioning incoming chunks across workers requires hashing the `call_id` string to select `workerQueues[workerIdx]`. Previously, this allocated a `hash/fnv` `hash.Hash32` interface on each chunk submission. Replacing this with an inline, unrolled 32-bit FNV-1a calculation (`hashCallIDInline`) keeps state entirely in CPU registers.

#### Microbenchmark Results (`go test -bench="." -benchmem`)
```
goos: windows
goarch: amd64
pkg: github.com/yoohooshantanu/streambridge/internal/worker
cpu: AMD Ryzen 5 3500U with Radeon Vega Mobile Gfx  
BenchmarkHashCallID_Old-8      	17404849	        69.36 ns/op	       0 B/op	       0 allocs/op
BenchmarkHashCallID_Inline-8   	37257129	        34.65 ns/op	       0 B/op	       0 allocs/op
```
* **Latency**: Reduced from ~69.4 ns/op to ~34.7 ns/op (**~50% faster** per chunk submission).
* **Allocations**: 0 B/op, 0 allocs/op in both versions (the inline version eliminates interface method dispatch overhead).
* **Equivalence Verification**: Bit-for-bit equivalence between `hashCallID` and `fnv.New32a` was verified via `TestHashCallIDEquivalence` over 10,000 randomized call IDs across varying worker counts (1 to 64).
* **Load Test Impact**: Because `hashCallID` is a micro-optimization on the fast path (~35ns out of ~20µs mean service time), it modestly reduces CPU instructions per chunk but does not alter the macro-level host CPU saturation knee.

---

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
