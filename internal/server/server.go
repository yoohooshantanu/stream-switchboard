package server

import (
	"context"
	"io"
	"sync"

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

	logger.L.Info("bidi_stream_connected", "call_id", callID, "tenant_id", firstChunk.TenantId)

	// Subscribe to event router to receive real-time events for this call
	eventCh, unsubscribe := s.router.Subscribe(callID, 512)
	defer unsubscribe()

	// Track audio ingestion metrics
	metrics.DefaultMetrics.IncChunk(len(firstChunk.Data))
	s.pool.Submit(&worker.WorkerJob{
		CallID: callID,
		Chunk:  firstChunk,
	})

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

			metrics.DefaultMetrics.IncChunk(len(chunk.Data))
			s.pool.Submit(&worker.WorkerJob{
				CallID: callID,
				Chunk:  chunk,
			})

			if chunk.IsFinal {
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

	logger.L.Info("watch_call_subscription_started", "call_id", req.CallId, "tenant_id", req.TenantId)

	eventCh, unsubscribe := s.router.Subscribe(req.CallId, 512)
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

			if event.Type == pb.EventType_EVENT_CALL_ENDED {
				return nil
			}
		}
	}
}

// GetCallSummary provides unary post-call analytics lookup
func (s *StreamBridgeServer) GetCallSummary(ctx context.Context, req *pb.SummaryRequest) (*pb.CallSummary, error) {
	if req.CallId == "" {
		return nil, status.Error(codes.InvalidArgument, "call_id is required")
	}

	summary, exists := s.router.GetSummary(req.CallId)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "no completed call summary found for call_id: %s", req.CallId)
	}

	return summary, nil
}
