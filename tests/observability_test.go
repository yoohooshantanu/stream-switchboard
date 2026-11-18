package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/yoohooshantanu/audio-gateway/internal/logger"
	"github.com/yoohooshantanu/audio-gateway/internal/metrics"
	"github.com/yoohooshantanu/audio-gateway/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// logCaptureBuffer captures structured slog entries thread-safely
type logCaptureBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logCaptureBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logCaptureBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *logCaptureBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// -------
// Test official Prometheus metrics registration and exposition
// -------
func TestPrometheusOfficialMetricsRegistered(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	metrics.DefaultMetrics.Handler().ServeHTTP(w, req)
	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /metrics HTTP 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed reading /metrics response: %v", err)
	}
	bodyStr := string(body)

	requiredMetrics := []string{
		"audio_gateway_active_streams",
		"audio_gateway_active_watchers",
		"audio_gateway_worker_queue_depth",
		"audio_gateway_sequence_gap_timeouts_total",
		"audio_gateway_duplicate_frames_dropped_total",
		"audio_gateway_late_frames_tombstone_dropped_total",
		"audio_gateway_audio_packets_total",
		"audio_gateway_audio_bytes_total",
		"audio_gateway_events_emitted_total",
		"audio_gateway_events_dropped_total",
		"audio_gateway_grpc_requests_total",
		"audio_gateway_packet_queue_wait_seconds",
		"audio_gateway_packet_processing_seconds",
	}

	for _, metricName := range requiredMetrics {
		if !strings.Contains(bodyStr, metricName) {
			t.Fatalf("expected /metrics exposition to contain registered metric %q, but was not found in response:\n%s", metricName, bodyStr)
		}
	}
}

// -------
// Test active_streams gauge decrement on all exit paths: Normal Disconnect, Gap Timeout Abort, Panic Recovery
// -------
func TestActiveStreamsGaugeDecrementedOnAllExitPaths(t *testing.T) {
	t.Run("NormalDisconnect", func(t *testing.T) {
		h := setupTestHarness(t)
		ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

		stream, err := h.client.StreamCall(ctx)
		if err != nil {
			t.Fatalf("failed initiating stream: %v", err)
		}

		// -------
		// Initial chunk to establish stream (ongoing call)
		// -------
		chunk1 := &pb.AudioChunk{
			CallId:   "call_obs_disconnect_01",
			TenantId: "tenant_test",
			Sequence: 1,
			Data:     []byte("pcm_data_01"),
			IsFinal:  false,
		}
		if err := stream.Send(chunk1); err != nil {
			t.Fatalf("send 1 failed: %v", err)
		}

		time.Sleep(10 * time.Millisecond)

		if active := testutil.ToFloat64(metrics.DefaultMetrics.ActiveStreamsGauge); active < 1 {
			t.Fatalf("expected audio_gateway_active_streams >= 1 during active call, got: %f", active)
		}

		// -------
		// Send final chunk to complete stream gracefully
		// -------
		chunk2 := &pb.AudioChunk{
			CallId:   "call_obs_disconnect_01",
			TenantId: "tenant_test",
			Sequence: 2,
			Data:     []byte("pcm_data_02"),
			IsFinal:  true,
		}
		if err := stream.Send(chunk2); err != nil {
			t.Fatalf("send 2 failed: %v", err)
		}

		_ = stream.CloseSend()
		for {
			_, err := stream.Recv()
			if err != nil {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)

		if active := testutil.ToFloat64(metrics.DefaultMetrics.ActiveStreamsGauge); active != 0 {
			t.Fatalf("expected audio_gateway_active_streams == 0 after normal disconnect, got: %f", active)
		}
	})

	t.Run("SequenceGapTimeoutAbort", func(t *testing.T) {
		h := setupTestHarness(t)
		h.pool.SetGapTimeout(100 * time.Millisecond)

		ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")
		stream, err := h.client.StreamCall(ctx)
		if err != nil {
			t.Fatalf("failed initiating stream: %v", err)
		}

		_ = stream.Send(&pb.AudioChunk{
			CallId:   "call_obs_gap_01",
			TenantId: "tenant_test",
			Sequence: 1,
			Data:     []byte("frame_1"),
		})

		_ = stream.Send(&pb.AudioChunk{
			CallId:   "call_obs_gap_01",
			TenantId: "tenant_test",
			Sequence: 3,
			Data:     []byte("frame_3"),
		})

		for {
			_, err := stream.Recv()
			if err != nil {
				if status.Code(err) != codes.DataLoss {
					t.Fatalf("expected DataLoss on gap timeout, got %v", err)
				}
				break
			}
		}

		time.Sleep(50 * time.Millisecond)

		if active := testutil.ToFloat64(metrics.DefaultMetrics.ActiveStreamsGauge); active != 0 {
			t.Fatalf("expected audio_gateway_active_streams == 0 after gap-timeout abort, got: %f", active)
		}
	})

	t.Run("PanicRecovery", func(t *testing.T) {
		h := setupTestHarness(t)
		ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

		// -------
		// Force panic inside router by publishing with nil event or trigger hook
		// -------
		stream, err := h.client.StreamCall(ctx)
		if err != nil {
			t.Fatalf("failed initiating stream: %v", err)
		}

		// Send initial chunk
		_ = stream.Send(&pb.AudioChunk{
			CallId:   "call_obs_panic_01",
			TenantId: "tenant_test",
			Sequence: 1,
			Data:     []byte("panic_trigger_data"),
		})

		// -------
		// Publish nil event to trigger error/recovery in stream loop
		// -------
		time.Sleep(20 * time.Millisecond)
		h.router.Publish("call_obs_panic_01", nil)

		// -------
		// Stream should terminate cleanly with Internal error rather than crashing the process
		// -------
		for {
			_, err := stream.Recv()
			if err != nil {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)

		if active := testutil.ToFloat64(metrics.DefaultMetrics.ActiveStreamsGauge); active != 0 {
			t.Fatalf("expected audio_gateway_active_streams == 0 after panic recovery, got: %f", active)
		}
	})
}

// -------
// Test structured logging: every log line within a call carries call_id
// -------
func TestStructuredLoggingCarriesCallID(t *testing.T) {
	logBuf := &logCaptureBuffer{}
	handler := slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})
	origLogger := logger.L
	logger.L = slog.New(handler)
	defer func() {
		logger.L = origLogger
	}()

	h := setupTestHarness(t)
	targetCallID := "call_structured_log_audit_999"
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

	stream, err := h.client.StreamCall(ctx)
	if err != nil {
		t.Fatalf("stream init failed: %v", err)
	}

	for seq := uint64(1); seq <= 3; seq++ {
		err := stream.Send(&pb.AudioChunk{
			CallId:   targetCallID,
			TenantId: "tenant_test",
			Sequence: seq,
			Data:     []byte(fmt.Sprintf("pcm_chunk_%d", seq)),
			IsFinal:  (seq == 3),
		})
		if err != nil {
			t.Fatalf("send seq %d failed: %v", seq, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = stream.CloseSend()
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}

	time.Sleep(50 * time.Millisecond)

	logOutput := logBuf.String()
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")
	if len(lines) == 0 {
		t.Fatalf("expected log lines to be captured, got none")
	}

	callLogsFound := 0
	for _, line := range lines {
		if strings.Contains(line, targetCallID) {
			callLogsFound++
			if !strings.Contains(line, `"call_id":"`+targetCallID+`"`) {
				t.Fatalf("log line mentions call but does not contain structured call_id attribute: %s", line)
			}
		}
	}

	if callLogsFound < 2 {
		t.Fatalf("expected at least 2 structured log lines carrying call_id, found %d. Logs:\n%s", callLogsFound, logOutput)
	}
}

// -------
// Test cardinality safety: drive 1000 distinct call IDs and assert series count stays constant
// -------
func TestCardinalitySafety1000CallsConstantSeriesCount(t *testing.T) {
	h := setupTestHarness(t)

	// -------
	// Warm up with one call to initialize any lazy-registered static label vectors (e.g. event types)
	// -------
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")
	warmupStream, err := h.client.StreamCall(ctx)
	if err == nil {
		_ = warmupStream.Send(&pb.AudioChunk{
			CallId:   "call_warmup",
			TenantId: "tenant_test",
			Sequence: 1,
			Data:     []byte("warmup"),
			IsFinal:  true,
		})
		_ = warmupStream.CloseSend()
		for {
			if _, err := warmupStream.Recv(); err != nil {
				break
			}
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Scrape initial metrics series count
	wInit := httptest.NewRecorder()
	metrics.DefaultMetrics.Handler().ServeHTTP(wInit, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	initialBody := wInit.Body.String()

	// -------
	// Count number of application metric sample lines (excluding comments, blank lines, and runtime metrics)
	// -------
	countSeries := func(exposition string) int {
		count := 0
		for _, line := range strings.Split(exposition, "\n") {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") && strings.HasPrefix(line, "audio_gateway_") {
				count++
			}
		}
		return count
	}

	initialSeriesCount := countSeries(initialBody)

	// -------
	// Drive 1,000 distinct call IDs through the system
	// -------
	for i := 1; i <= 1000; i++ {
		callID := fmt.Sprintf("call_cardinality_burst_%04d", i)
		stream, err := h.client.StreamCall(ctx)
		if err != nil {
			t.Fatalf("call %d failed initiating: %v", i, err)
		}

		_ = stream.Send(&pb.AudioChunk{
			CallId:   callID,
			TenantId: "tenant_test",
			Sequence: 1,
			Data:     []byte("test_cardinality_sample"),
			IsFinal:  true,
		})
		_ = stream.CloseSend()
		for {
			_, err := stream.Recv()
			if err != nil {
				break
			}
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Scrape metrics after 1,000 calls
	wAfter := httptest.NewRecorder()
	metrics.DefaultMetrics.Handler().ServeHTTP(wAfter, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	afterBody := wAfter.Body.String()

	// -------
	// Assert NO metric line contains call_id or tenant_id
	// -------
	if strings.Contains(afterBody, "call_cardinality_burst") {
		t.Fatalf("CARDINALITY EXPLOSION: found dynamic call_id in Prometheus metrics output:\n%s", afterBody)
	}
	if strings.Contains(afterBody, `call_id="`) || strings.Contains(afterBody, `tenant_id="`) {
		t.Fatalf("CARDINALITY EXPLOSION: found prohibited call_id or tenant_id label dimension in Prometheus metrics output:\n%s", afterBody)
	}

	// -------
	// Assert series count remains constant
	// -------
	afterSeriesCount := countSeries(afterBody)
	if afterSeriesCount != initialSeriesCount {
		t.Fatalf("expected constant metric series count of %d after 1,000 unique calls, but got %d", initialSeriesCount, afterSeriesCount)
	}
}

// -------
// Test sub-millisecond packet latency histogram buckets
// -------
func TestSubMillisecondPacketLatencyHistograms(t *testing.T) {
	h := setupTestHarness(t)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

	stream, err := h.client.StreamCall(ctx)
	if err != nil {
		t.Fatalf("stream init failed: %v", err)
	}

	for seq := uint64(1); seq <= 10; seq++ {
		_ = stream.Send(&pb.AudioChunk{
			CallId:   "call_histogram_test_01",
			TenantId: "tenant_test",
			Sequence: seq,
			Data:     []byte("audio_packet_histogram"),
			IsFinal:  (seq == 10),
		})
		time.Sleep(5 * time.Millisecond)
	}

	_ = stream.CloseSend()
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}

	time.Sleep(50 * time.Millisecond)

	w := httptest.NewRecorder()
	metrics.DefaultMetrics.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := w.Body.String()

	expectedSubMsBuckets := []string{
		`audio_gateway_packet_processing_seconds_bucket{le="0.0005"}`,
		`audio_gateway_packet_processing_seconds_bucket{le="0.001"}`,
		`audio_gateway_packet_queue_wait_seconds_bucket{le="0.0005"}`,
		`audio_gateway_packet_queue_wait_seconds_bucket{le="0.001"}`,
	}

	for _, bucket := range expectedSubMsBuckets {
		if !strings.Contains(body, bucket) {
			t.Fatalf("expected sub-millisecond histogram bucket %q in /metrics exposition, got:\n%s", bucket, body)
		}
	}
}
