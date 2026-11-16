package server

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/yoohooshantanu/streambridge/internal/interceptor"
	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/metrics"
	"github.com/yoohooshantanu/streambridge/internal/pb"
	"github.com/yoohooshantanu/streambridge/internal/router"
	"github.com/yoohooshantanu/streambridge/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StreamBridgeServer struct {
	pb.UnimplementedStreamBridgeServer
	router *router.EventRouter
	pool   *worker.AIWorkerPool
}

func NewStreamBridgeServer(r *router.EventRouter, p *worker.AIWorkerPool) *StreamBridgeServer {
	return &StreamBridgeServer{
		router: r,
		pool:   p,
	}
}

// StreamCall manages bidirectional streaming between telephony client and AI workers
func (s *StreamBridgeServer) StreamCall(stream grpc.BidiStreamingServer[pb.AudioChunk, pb.CallEvent]) error {
	metrics.DefaultMetrics.IncStream()
	defer metrics.DefaultMetrics.DecStream()

	ctx := stream.Context()
	authTenantID, _ := interceptor.TenantFromContext(ctx)

	// Wait for the first chunk to establish call identity
	firstChunk, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return status.Errorf(codes.Canceled, "failed reading initial audio chunk: %v", err)
	}

	callID := firstChunk.CallId
	if callID == "" {
		return status.Error(codes.InvalidArgument, "call_id cannot be empty")
	}

	// Validate tenant consistency with authenticated credentials
	if firstChunk.TenantId != "" && authTenantID != "" && firstChunk.TenantId != authTenantID {
		return status.Errorf(codes.PermissionDenied, "audio chunk tenant_id %q does not match authenticated tenant %q", firstChunk.TenantId, authTenantID)
	}
	if firstChunk.TenantId == "" && authTenantID != "" {
		firstChunk.TenantId = authTenantID
	}

	// Register call ownership with tenant isolation
	if regErr := s.router.RegisterCall(callID, firstChunk.TenantId); regErr != nil {
		return regErr
	}

	logger.L.Info("bidi_stream_connected", "call_id", callID, "tenant_id", firstChunk.TenantId)

	// Subscribe to event router to receive real-time events for this call
	eventCh, unsubscribe, subErr := s.router.SubscribeForTenant(callID, firstChunk.TenantId, 128)
	if subErr != nil {
		return subErr
	}
	defer unsubscribe()

	// Track audio ingestion metrics
	metrics.DefaultMetrics.IncChunk(len(firstChunk.Data))
	if !s.pool.Submit(&worker.WorkerJob{
		CallID: callID,
		Chunk:  firstChunk,
	}) {
		return status.Errorf(codes.ResourceExhausted, "worker pool queue saturated")
	}

	doneCh := make(chan struct{})
	var closeOnce sync.Once
	signalDone := func() {
		closeOnce.Do(func() {
			close(doneCh)
		})
	}

	errCh := make(chan error, 2)

	// Goroutine A: Client Ingestion Loop (stream.Recv is exclusively called here)
	go func() {
		defer signalDone()

		for {
			chunk, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr != io.EOF {
					errCh <- recvErr
				}
				return
			}

			// Validate chunk tenant consistency
			if chunk.TenantId != "" && authTenantID != "" && chunk.TenantId != authTenantID {
				errCh <- status.Errorf(codes.PermissionDenied, "audio chunk tenant_id %q does not match authenticated tenant %q", chunk.TenantId, authTenantID)
				return
			}
			if chunk.TenantId == "" && authTenantID != "" {
				chunk.TenantId = authTenantID
			}

			metrics.DefaultMetrics.IncChunk(len(chunk.Data))
			if !s.pool.Submit(&worker.WorkerJob{
				CallID: callID,
				Chunk:  chunk,
			}) {
				errCh <- status.Errorf(codes.ResourceExhausted, "worker pool queue saturated")
				return
			}
		}
	}()

	// Goroutine B: Real-time Event Emission Loop (stream.Send is exclusively called here)
	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()

		case err := <-errCh:
			return status.Errorf(codes.Internal, "stream ingest failed: %v", err)

		case event, ok := <-eventCh:
			if !ok {
				return nil
			}

			if sendErr := stream.Send(event); sendErr != nil {
				logger.L.Warn("bidi_stream_send_failed", "call_id", callID, "error", sendErr)
				return status.Errorf(codes.Unavailable, "failed emitting call event: %v", sendErr)
			}

			// If unrecoverable gap timeout occurred, terminate stream with DataLoss gRPC status
			if event.Type == pb.EventType_EVENT_CALL_ERROR && strings.Contains(event.Payload, "sequence_gap_timeout") {
				return status.Errorf(codes.DataLoss, "stream error: %s", event.Payload)
			}

			// If call ended event is sent, complete the stream gracefully
			if event.Type == pb.EventType_EVENT_CALL_ENDED {
				return nil
			}

		case <-doneCh:
			// Client finished sending chunks; continue draining remaining events until EVENT_CALL_ENDED
			// while continuously respecting ctx.Done() cancellation
			for {
				select {
				case <-ctx.Done():
					return status.FromContextError(ctx.Err()).Err()
				case event, ok := <-eventCh:
					if !ok {
						return nil
					}
					if sendErr := stream.Send(event); sendErr != nil {
						return nil
					}
					if event.Type == pb.EventType_EVENT_CALL_ERROR && strings.Contains(event.Payload, "sequence_gap_timeout") {
						return status.Errorf(codes.DataLoss, "stream error: %s", event.Payload)
					}
					if event.Type == pb.EventType_EVENT_CALL_ENDED {
						return nil
					}
				}
			}
		}
	}
}

// WatchCall handles server-side streaming for external supervisors, analytics, and CRMs
func (s *StreamBridgeServer) WatchCall(req *pb.WatchRequest, stream grpc.ServerStreamingServer[pb.CallEvent]) error {
	if req.CallId == "" {
		return status.Error(codes.InvalidArgument, "call_id is required")
	}

	authTenantID, _ := interceptor.TenantFromContext(stream.Context())

	// Validate cross-tenant watcher permission
	if req.TenantId != "" && authTenantID != "" && req.TenantId != authTenantID {
		return status.Errorf(codes.PermissionDenied, "requested tenant_id %q does not match authenticated tenant %q", req.TenantId, authTenantID)
	}

	targetTenant := authTenantID
	if targetTenant == "" {
		targetTenant = req.TenantId
	}

	logger.L.Info("watch_call_subscription_started", "call_id", req.CallId, "tenant_id", targetTenant)

	eventCh, unsubscribe, err := s.router.SubscribeForTenant(req.CallId, targetTenant, 512)
	if err != nil {
		return err
	}
	defer unsubscribe()

	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			logger.L.Info("watch_call_client_disconnected", "call_id", req.CallId)
			return status.FromContextError(ctx.Err()).Err()

		case event, ok := <-eventCh:
			if !ok {
				return nil
			}

			if err := stream.Send(event); err != nil {
				logger.L.Warn("watch_call_send_failed", "call_id", req.CallId, "error", err)
				return status.Errorf(codes.Unavailable, "failed sending watch event: %v", err)
			}

			if event.Type == pb.EventType_EVENT_CALL_ENDED || event.Type == pb.EventType_EVENT_CALL_ERROR {
				return nil
			}
		}
	}
}

// GetCallSummary provides unary post-call analytics lookup with tenant isolation
func (s *StreamBridgeServer) GetCallSummary(ctx context.Context, req *pb.SummaryRequest) (*pb.CallSummary, error) {
	if req.CallId == "" {
		return nil, status.Error(codes.InvalidArgument, "call_id is required")
	}

	authTenantID, _ := interceptor.TenantFromContext(ctx)

	summary, callTenant, exists := s.router.GetSummaryWithTenant(req.CallId)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "no completed call summary found for call_id: %s", req.CallId)
	}

	if authTenantID != "" && callTenant != "" && callTenant != authTenantID {
		return nil, status.Errorf(codes.PermissionDenied, "access denied: call summary belongs to another tenant")
	}

	return summary, nil
}
