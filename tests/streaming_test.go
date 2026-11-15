package tests

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/health"
	"github.com/yoohooshantanu/streambridge/internal/interceptor"
	"github.com/yoohooshantanu/streambridge/internal/pb"
	"github.com/yoohooshantanu/streambridge/internal/router"
	"github.com/yoohooshantanu/streambridge/internal/server"
	"github.com/yoohooshantanu/streambridge/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type testHarness struct {
	lis        *bufconn.Listener
	grpcServer *grpc.Server
	router     *router.EventRouter
	pool       *worker.AIWorkerPool
	client     pb.StreamBridgeClient
	health     healthpb.HealthClient
	conn       *grpc.ClientConn
}

func setupTestHarness(t *testing.T) *testHarness {
	lis := bufconn.Listen(bufSize)
	router := router.NewEventRouter()
	pool := worker.NewAIWorkerPool(router, 4, 1024)
	pool.Start()

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptor.UnaryAuthInterceptor),
		grpc.ChainStreamInterceptor(interceptor.StreamAuthInterceptor),
	)

	bridgeServer := server.NewStreamBridgeServer(router, pool)
	pb.RegisterStreamBridgeServer(grpcServer, bridgeServer)
	healthServer := health.NewHealthServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := pb.NewStreamBridgeClient(conn)
	healthClient := healthpb.NewHealthClient(conn)

	h := &testHarness{
		lis:        lis,
		grpcServer: grpcServer,
		router:     router,
		pool:       pool,
		client:     client,
		health:     healthClient,
		conn:       conn,
	}

	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.GracefulStop()
		pool.Stop()
		router.Close()
		_ = lis.Close()
	})

	return h
}

func TestBidirectionalStreamCall(t *testing.T) {
	h := setupTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attach valid authentication credentials
	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_test_key_123")

	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening bidi stream: %v", err)
	}

	callID := "call_test_stream_01"
	tenantID := "tenant_acme_corp"

	// Channel to collect received events asynchronously
	eventsCh := make(chan *pb.CallEvent, 32)
	errCh := make(chan error, 1)

	go func() {
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr != io.EOF {
					errCh <- recvErr
				}
				close(eventsCh)
				return
			}
			eventsCh <- event
		}
	}()

	// Stream 6 audio chunks (5 regular frames, 6th is final)
	totalChunks := 6
	for i := 1; i <= totalChunks; i++ {
		isFinal := (i == totalChunks)
		chunk := &pb.AudioChunk{
			CallId:    callID,
			TenantId:  tenantID,
			Sequence:  uint64(i),
			Data:      []byte("simulated_pcm_16khz_audio_payload"),
			IsFinal:   isFinal,
			Timestamp: time.Now().UnixMilli(),
		}
		if sendErr := stream.Send(chunk); sendErr != nil {
			t.Fatalf("failed sending chunk %d: %v", i, sendErr)
		}
		time.Sleep(15 * time.Millisecond) // Simulate telephony frame arrival cadence
	}

	// Close client send half
	_ = stream.CloseSend()

	// Collect received events
	var receivedEvents []*pb.CallEvent
	for ev := range eventsCh {
		receivedEvents = append(receivedEvents, ev)
	}

	select {
	case err := <-errCh:
		t.Fatalf("stream receive error: %v", err)
	default:
	}

	if len(receivedEvents) == 0 {
		t.Fatalf("expected call events over bidi stream, got 0")
	}

	// Validate presence of expected event types
	hasPartial := false
	hasFinal := false
	hasSentiment := false
	hasIntent := false
	hasEnd := false

	for _, ev := range receivedEvents {
		if ev.CallId != callID {
			t.Errorf("expected call_id %s, got %s", callID, ev.CallId)
		}
		switch ev.Type {
		case pb.EventType_EVENT_TRANSCRIPT_PARTIAL:
			hasPartial = true
		case pb.EventType_EVENT_TRANSCRIPT_FINAL:
			hasFinal = true
		case pb.EventType_EVENT_SENTIMENT:
			hasSentiment = true
		case pb.EventType_EVENT_INTENT_DETECTED:
			hasIntent = true
		case pb.EventType_EVENT_CALL_ENDED:
			hasEnd = true
		}
	}

	if !hasPartial {
		t.Errorf("missing EVENT_TRANSCRIPT_PARTIAL")
	}
	if !hasFinal {
		t.Errorf("missing EVENT_TRANSCRIPT_FINAL")
	}
	if !hasSentiment {
		t.Errorf("missing EVENT_SENTIMENT")
	}
	if !hasIntent {
		t.Errorf("missing EVENT_INTENT_DETECTED")
	}
	if !hasEnd {
		t.Errorf("missing EVENT_CALL_ENDED")
	}

	// Verify post-call aggregated summary unary RPC
	summary, err := h.client.GetCallSummary(authCtx, &pb.SummaryRequest{CallId: callID})
	if err != nil {
		t.Fatalf("failed fetching call summary: %v", err)
	}

	if summary.CallId != callID {
		t.Errorf("summary call_id mismatch: expected %s, got %s", callID, summary.CallId)
	}
	if summary.TotalChunks != int32(totalChunks) {
		t.Errorf("summary total_chunks mismatch: expected %d, got %d", totalChunks, summary.TotalChunks)
	}
	if summary.Transcript == "" {
		t.Errorf("expected non-empty transcript in summary")
	}
}

func TestStreamCallContextCancellationDuringDrain(t *testing.T) {
	h := setupTestHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_test_key_123")

	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening stream: %v", err)
	}

	// Send initial chunk without is_final
	err = stream.Send(&pb.AudioChunk{
		CallId:    "call_cancel_test_01",
		TenantId:  "tenant_test",
		Sequence:  1,
		Data:      []byte("initial_frame"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed sending chunk: %v", err)
	}

	// Close client send half -> triggers server doneCh branch
	_ = stream.CloseSend()

	// Immediately cancel context while in drain phase
	cancel()

	// Reading from stream must return non-nil error promptly (context canceled)
	done := make(chan struct{})
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
		// Succeeded in cleanly terminating
	case <-time.After(2 * time.Second):
		t.Fatalf("stream failed to terminate within timeout upon context cancellation")
	}
}
