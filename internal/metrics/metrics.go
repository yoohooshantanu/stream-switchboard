package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	ActiveStreams    int64
	ActiveWatchers   int64
	ChunksReceived   int64
	AudioBytesTotal  int64
	EventsEmitted    int64
	EventsDropped    int64
	RPCCallsTotal    int64
	RPCErrorsTotal   int64
	WorkerProcessing int64
}

var DefaultMetrics = &Metrics{}

func (m *Metrics) IncStream()           { atomic.AddInt64(&m.ActiveStreams, 1) }
func (m *Metrics) DecStream()           { atomic.AddInt64(&m.ActiveStreams, -1) }
func (m *Metrics) IncWatcher()          { atomic.AddInt64(&m.ActiveWatchers, 1) }
func (m *Metrics) DecWatcher()          { atomic.AddInt64(&m.ActiveWatchers, -1) }
func (m *Metrics) IncChunk(bytes int)   { atomic.AddInt64(&m.ChunksReceived, 1); atomic.AddInt64(&m.AudioBytesTotal, int64(bytes)) }
func (m *Metrics) IncEvent()            { atomic.AddInt64(&m.EventsEmitted, 1) }
func (m *Metrics) IncDropped()          { atomic.AddInt64(&m.EventsDropped, 1) }
func (m *Metrics) IncRPC(err bool) {
	atomic.AddInt64(&m.RPCCallsTotal, 1)
	if err {
		atomic.AddInt64(&m.RPCErrorsTotal, 1)
	}
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP streambridge_active_streams Current number of active bidirectional call streams\n")
		fmt.Fprintf(w, "# TYPE streambridge_active_streams gauge\n")
		fmt.Fprintf(w, "streambridge_active_streams %d\n\n", atomic.LoadInt64(&m.ActiveStreams))

		fmt.Fprintf(w, "# HELP streambridge_active_watchers Current number of active server-streaming watchers\n")
		fmt.Fprintf(w, "# TYPE streambridge_active_watchers gauge\n")
		fmt.Fprintf(w, "streambridge_active_watchers %d\n\n", atomic.LoadInt64(&m.ActiveWatchers))

		fmt.Fprintf(w, "# HELP streambridge_chunks_received_total Cumulative audio chunks received\n")
		fmt.Fprintf(w, "# TYPE streambridge_chunks_received_total counter\n")
		fmt.Fprintf(w, "streambridge_chunks_received_total %d\n\n", atomic.LoadInt64(&m.ChunksReceived))

		fmt.Fprintf(w, "# HELP streambridge_audio_bytes_total Cumulative audio payload bytes ingested\n")
		fmt.Fprintf(w, "# TYPE streambridge_audio_bytes_total counter\n")
		fmt.Fprintf(w, "streambridge_audio_bytes_total %d\n\n", atomic.LoadInt64(&m.AudioBytesTotal))

		fmt.Fprintf(w, "# HELP streambridge_events_emitted_total Cumulative real-time events published\n")
		fmt.Fprintf(w, "# TYPE streambridge_events_emitted_total counter\n")
		fmt.Fprintf(w, "streambridge_events_emitted_total %d\n\n", atomic.LoadInt64(&m.EventsEmitted))

		fmt.Fprintf(w, "# HELP streambridge_events_dropped_total Events dropped due to slow consumer backpressure\n")
		fmt.Fprintf(w, "# TYPE streambridge_events_dropped_total counter\n")
		fmt.Fprintf(w, "streambridge_events_dropped_total %d\n\n", atomic.LoadInt64(&m.EventsDropped))

		fmt.Fprintf(w, "# HELP streambridge_rpc_calls_total Cumulative gRPC invocations\n")
		fmt.Fprintf(w, "# TYPE streambridge_rpc_calls_total counter\n")
		fmt.Fprintf(w, "streambridge_rpc_calls_total %d\n\n", atomic.LoadInt64(&m.RPCCallsTotal))

		fmt.Fprintf(w, "# HELP streambridge_rpc_errors_total Cumulative gRPC invocation failures\n")
		fmt.Fprintf(w, "# TYPE streambridge_rpc_errors_total counter\n")
		fmt.Fprintf(w, "streambridge_rpc_errors_total %d\n", atomic.LoadInt64(&m.RPCErrorsTotal))
	}
}
