package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CARDINALITY SAFETY GUARANTEE:
// Under NO circumstances may high-cardinality values such as call_id, tenant_id, or user_id
// be used as Prometheus label dimensions. In high-throughput real-time telephony processing
// millions of calls, dynamic IDs in labels cause catastrophic metric cardinality explosion,
// exhaust Prometheus RAM, and cause scrape timeouts. Label dimensions in StreamBridge are
// strictly bounded to finite enumerations: method, code, direction, event_type, worker_id.
// High-cardinality attributes belong exclusively in structured logs and distributed tracing.

type Metrics struct {
	Registry *prometheus.Registry

	ActiveStreamsGauge              prometheus.Gauge
	ActiveWatchersGauge             prometheus.Gauge
	WorkerQueueDepth                *prometheus.GaugeVec
	SequenceGapTimeoutsTotal        prometheus.Counter
	DuplicateFramesDroppedTotal     prometheus.Counter
	LateFramesTombstoneDroppedTotal prometheus.Counter
	AudioPacketsTotal               *prometheus.CounterVec
	AudioBytesTotal                 *prometheus.CounterVec
	EventsEmittedTotal              *prometheus.CounterVec
	EventsDroppedTotal              prometheus.Counter
	GRPCRequestsTotal               *prometheus.CounterVec
	PacketQueueWaitHistogram        prometheus.Histogram
	PacketProcessingHistogram       prometheus.Histogram
	GRPCRequestDurationHistogram    *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	// Sub-millisecond packet latency buckets: 50µs, 100µs, 250µs, 500µs, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms
	packetLatencyBuckets := []float64{
		0.00005, 0.0001, 0.00025, 0.0005, 0.001,
		0.0025, 0.005, 0.010, 0.025, 0.050, 0.100,
	}

	// gRPC RPC latency buckets: 500µs up to 5s
	rpcLatencyBuckets := []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
		0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
	}

	m := &Metrics{
		Registry: reg,

		ActiveStreamsGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streambridge_active_streams",
			Help: "Current number of active bidirectional call streams",
		}),

		ActiveWatchersGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streambridge_active_watchers",
			Help: "Current number of active server-streaming watchers",
		}),

		WorkerQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streambridge_worker_queue_depth",
			Help: "Current number of buffered audio chunk jobs per worker queue",
		}, []string{"worker_id"}),

		SequenceGapTimeoutsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streambridge_sequence_gap_timeouts_total",
			Help: "Cumulative count of session terminations caused by missing audio frame sequence gaps exceeding timeout",
		}),

		DuplicateFramesDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streambridge_duplicate_frames_dropped_total",
			Help: "Cumulative count of redundant audio chunks dropped due to already processed sequence numbers",
		}),

		LateFramesTombstoneDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streambridge_late_frames_tombstone_dropped_total",
			Help: "Cumulative count of late-arriving audio chunks discarded because their session was previously aborted (tombstone)",
		}),

		AudioPacketsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streambridge_audio_packets_total",
			Help: "Cumulative count of audio chunks ingested or emitted",
		}, []string{"direction"}),

		AudioBytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streambridge_audio_bytes_total",
			Help: "Cumulative count of audio payload bytes transferred",
		}, []string{"direction"}),

		EventsEmittedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streambridge_events_emitted_total",
			Help: "Cumulative count of real-time call events published",
		}, []string{"event_type"}),

		EventsDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streambridge_events_dropped_total",
			Help: "Cumulative count of events dropped due to slow subscriber channel backpressure",
		}),

		GRPCRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streambridge_grpc_requests_total",
			Help: "Cumulative count of gRPC invocations completed",
		}, []string{"method", "code"}),

		PacketQueueWaitHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "streambridge_packet_queue_wait_seconds",
			Help:    "Duration audio packets wait in worker channel buffer before execution begins",
			Buckets: packetLatencyBuckets,
		}),

		PacketProcessingHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "streambridge_packet_processing_seconds",
			Help:    "Execution duration for reassembly, sequencing, and AI inference dispatch per audio chunk",
			Buckets: packetLatencyBuckets,
		}),

		GRPCRequestDurationHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "streambridge_grpc_request_duration_seconds",
			Help:    "Total invocation duration of gRPC calls",
			Buckets: rpcLatencyBuckets,
		}, []string{"method", "code"}),
	}

	reg.MustRegister(
		m.ActiveStreamsGauge,
		m.ActiveWatchersGauge,
		m.WorkerQueueDepth,
		m.SequenceGapTimeoutsTotal,
		m.DuplicateFramesDroppedTotal,
		m.LateFramesTombstoneDroppedTotal,
		m.AudioPacketsTotal,
		m.AudioBytesTotal,
		m.EventsEmittedTotal,
		m.EventsDroppedTotal,
		m.GRPCRequestsTotal,
		m.PacketQueueWaitHistogram,
		m.PacketProcessingHistogram,
		m.GRPCRequestDurationHistogram,
	)

	// Pre-initialize standard label dimensions so metric descriptors and zero-values appear in exposition immediately
	for i := 0; i < 4; i++ {
		m.WorkerQueueDepth.WithLabelValues(fmt.Sprintf("%d", i)).Set(0)
	}
	m.AudioPacketsTotal.WithLabelValues("inbound")
	m.AudioPacketsTotal.WithLabelValues("outbound")
	m.AudioBytesTotal.WithLabelValues("inbound")
	m.AudioBytesTotal.WithLabelValues("outbound")
	m.EventsEmittedTotal.WithLabelValues("unknown")
	m.GRPCRequestsTotal.WithLabelValues("unknown", "OK")

	return m
}

var DefaultMetrics = NewMetrics()

// Stream Lifecycle Gauges
func (m *Metrics) IncStream()  { m.ActiveStreamsGauge.Inc() }
func (m *Metrics) DecStream()  { m.ActiveStreamsGauge.Dec() }
func (m *Metrics) IncWatcher() { m.ActiveWatchersGauge.Inc() }
func (m *Metrics) DecWatcher() { m.ActiveWatchersGauge.Dec() }

// Worker Queue Depth
func (m *Metrics) SetWorkerQueueDepth(workerID int, depth float64) {
	m.WorkerQueueDepth.WithLabelValues(fmt.Sprintf("%d", workerID)).Set(depth)
}

// Flow & Anomaly Counters
func (m *Metrics) IncSequenceGapTimeout()        { m.SequenceGapTimeoutsTotal.Inc() }
func (m *Metrics) IncDuplicateFrameDropped()     { m.DuplicateFramesDroppedTotal.Inc() }
func (m *Metrics) IncLateFrameTombstoneDropped() { m.LateFramesTombstoneDroppedTotal.Inc() }

func (m *Metrics) IncChunk(bytes int) {
	m.AudioPacketsTotal.WithLabelValues("inbound").Inc()
	m.AudioBytesTotal.WithLabelValues("inbound").Add(float64(bytes))
}

func (m *Metrics) IncChunkOutbound(bytes int) {
	m.AudioPacketsTotal.WithLabelValues("outbound").Inc()
	m.AudioBytesTotal.WithLabelValues("outbound").Add(float64(bytes))
}

func (m *Metrics) IncEvent() {
	m.EventsEmittedTotal.WithLabelValues("unknown").Inc()
}

func (m *Metrics) IncEventType(eventType string) {
	m.EventsEmittedTotal.WithLabelValues(eventType).Inc()
}

func (m *Metrics) IncDropped() {
	m.EventsDroppedTotal.Inc()
}

func (m *Metrics) IncRPC(err bool) {
	code := "OK"
	if err {
		code = "Error"
	}
	m.GRPCRequestsTotal.WithLabelValues("unknown", code).Inc()
}

func (m *Metrics) RecordRPC(method, code string, durationSeconds float64) {
	m.GRPCRequestsTotal.WithLabelValues(method, code).Inc()
	m.GRPCRequestDurationHistogram.WithLabelValues(method, code).Observe(durationSeconds)
}

// Sub-millisecond Packet Latencies
func (m *Metrics) ObservePacketQueueWait(durationSeconds float64) {
	m.PacketQueueWaitHistogram.Observe(durationSeconds)
}

func (m *Metrics) ObservePacketProcessing(durationSeconds float64) {
	m.PacketProcessingHistogram.Observe(durationSeconds)
}

// HTTP Handler
func (m *Metrics) Handler() http.HandlerFunc {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}).ServeHTTP
}
