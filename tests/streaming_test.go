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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type testHarness struct {
	lis        *bufconn.Listener
	grpcServer *grpc.Server
	router     *router.EventRouter
	pool       *worker.AIWorkerPool
	keyReg     *interceptor.MemoryKeyRegistry
	client     pb.StreamBridgeClient
	health     healthpb.HealthClient
	conn       *grpc.ClientConn
}

func setupTestHarness(t *testing.T) *testHarness {
	lis := bufconn.Listen(bufSize)
	router := router.NewEventRouter()
	pool := worker.NewAIWorkerPool(router, 4, 1024)
	pool.Start()

	keyReg := interceptor.NewKeyRegistry()
	keyReg.RegisterKey("sb_test_key_123", "tenant_test")
	keyReg.RegisterKey("sb_live_acme_corp_sec99", "tenant_acme_corp")
	keyReg.RegisterKey("sb_live_growth_tier_key", "tenant_growth_tier")

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptor.NewUnaryAuthInterceptor(keyReg)),
		grpc.ChainStreamInterceptor(interceptor.NewStreamAuthInterceptor(keyReg)),
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
		keyReg:     keyReg,
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

	// Attach valid authentication credentials bound to tenant_acme_corp
	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_live_acme_corp_sec99")

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

func TestStreamCallEmptyCallIDRejection(t *testing.T) {
	h := setupTestHarness(t)

	authCtx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")
	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening stream: %v", err)
	}

	// Send chunk with empty CallId
	err = stream.Send(&pb.AudioChunk{
		CallId:    "",
		TenantId:  "tenant_test",
		Sequence:  1,
		Data:      []byte("chunk_data"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed sending initial chunk: %v", err)
	}

	// Receive should fail with codes.InvalidArgument
	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatalf("expected error on empty call_id, got nil")
	}

	st, ok := status.FromError(recvErr)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got: %v", recvErr)
	}
}

func TestTenantIsolationCrossTenantAccessBlocked(t *testing.T) {
	h := setupTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tenantACtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_live_acme_corp_sec99") // tenant_acme_corp
	tenantBCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_live_growth_tier_key") // tenant_growth_tier

	callID := "call_tenant_isolated_42"

	// 1. Tenant A starts a call
	streamA, err := h.client.StreamCall(tenantACtx)
	if err != nil {
		t.Fatalf("failed starting stream for tenant A: %v", err)
	}

	err = streamA.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_acme_corp",
		Sequence:  1,
		Data:      []byte("tenant_a_audio_data"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("tenant A failed sending initial chunk: %v", err)
	}

	// 2. Tenant B attempts to WatchCall on Tenant A's call -> Must be PermissionDenied!
	watchStreamB, err := h.client.WatchCall(tenantBCtx, &pb.WatchRequest{
		CallId:   callID,
		TenantId: "tenant_growth_tier",
	})
	if err == nil {
		_, recvErr := watchStreamB.Recv()
		if recvErr == nil {
			t.Fatalf("expected WatchCall cross-tenant subscription to be forbidden")
		}
		if st, ok := status.FromError(recvErr); !ok || st.Code() != codes.PermissionDenied {
			t.Fatalf("expected PermissionDenied on cross-tenant WatchCall, got: %v", recvErr)
		}
	} else {
		if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
			t.Fatalf("expected PermissionDenied on cross-tenant WatchCall dial, got: %v", err)
		}
	}

	// 3. Tenant A finishes call with final chunk
	err = streamA.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_acme_corp",
		Sequence:  2,
		Data:      []byte("tenant_a_final_audio"),
		IsFinal:   true,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("tenant A failed sending final chunk: %v", err)
	}
	_ = streamA.CloseSend()

	// Wait for summary to be saved
	time.Sleep(100 * time.Millisecond)

	// 4. Tenant B attempts to GetCallSummary of Tenant A's call -> Must be PermissionDenied!
	_, err = h.client.GetCallSummary(tenantBCtx, &pb.SummaryRequest{CallId: callID})
	if err == nil {
		t.Fatalf("expected GetCallSummary to fail with PermissionDenied for cross-tenant request")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied on cross-tenant summary lookup, got: %v", err)
	}

	// 5. Tenant A requests GetCallSummary -> Must succeed!
	summaryA, err := h.client.GetCallSummary(tenantACtx, &pb.SummaryRequest{CallId: callID})
	if err != nil {
		t.Fatalf("tenant A failed retrieving own summary: %v", err)
	}
	if summaryA.CallId != callID || summaryA.TotalChunks != 2 {
		t.Fatalf("unexpected summary details for tenant A: %+v", summaryA)
	}
}

func TestOutOfOrderChunkArrivalSequencing(t *testing.T) {
	h := setupTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_test_key_123")
	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening stream: %v", err)
	}

	callID := "call_reordering_test_99"
	eventsCh := make(chan *pb.CallEvent, 32)

	go func() {
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				close(eventsCh)
				return
			}
			eventsCh <- event
		}
	}()

	// Send Chunk 1 (Sequence 1)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  1,
		Data:      []byte("chunk_1_audio"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// Send Chunk 3 (Sequence 3, IsFinal: true) BEFORE Chunk 2!
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  3,
		Data:      []byte("chunk_3_final_audio"),
		IsFinal:   true,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(30 * time.Millisecond)

	// Send Chunk 2 (Sequence 2) late
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  2,
		Data:      []byte("chunk_2_intermediate_audio"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})

	_ = stream.CloseSend()

	// Collect received events
	var receivedEvents []*pb.CallEvent
	for ev := range eventsCh {
		receivedEvents = append(receivedEvents, ev)
	}

	summary, err := h.client.GetCallSummary(authCtx, &pb.SummaryRequest{CallId: callID})
	if err != nil {
		t.Fatalf("failed retrieving summary: %v", err)
	}

	if summary.TotalChunks != 3 {
		t.Fatalf("expected 3 total chunks in summary despite out-of-order arrival, got %d", summary.TotalChunks)
	}

	if len(receivedEvents) == 0 {
		t.Fatalf("expected events, got 0")
	}
	lastEvent := receivedEvents[len(receivedEvents)-1]
	if lastEvent.Type != pb.EventType_EVENT_CALL_ENDED {
		t.Fatalf("expected last event to be EVENT_CALL_ENDED, got type %v", lastEvent.Type)
	}
}

func TestInitialSequenceGapReassembly(t *testing.T) {
	h := setupTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_test_key_123")
	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening stream: %v", err)
	}

	callID := "call_initial_gap_test_01"
	eventsCh := make(chan *pb.CallEvent, 32)

	go func() {
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				close(eventsCh)
				return
			}
			eventsCh <- event
		}
	}()

	// 1. Send Sequence 2 FIRST (Stream begins with an initial gap, missing sequence 1!)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  2,
		Data:      []byte("chunk_2_received_first"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 2. Send Sequence 3 (IsFinal: true)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  3,
		Data:      []byte("chunk_3_final_audio"),
		IsFinal:   true,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 3. Send Sequence 1 LAST (Missing first chunk finally arrives)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  1,
		Data:      []byte("chunk_1_arrived_last"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})

	_ = stream.CloseSend()

	// Drain events
	for range eventsCh {
	}

	// Verify that summary accurately captured all 3 chunks in order
	summary, err := h.client.GetCallSummary(authCtx, &pb.SummaryRequest{CallId: callID})
	if err != nil {
		t.Fatalf("failed retrieving summary: %v", err)
	}

	if summary.TotalChunks != 3 {
		t.Fatalf("expected 3 total chunks after initial gap reassembly, got %d", summary.TotalChunks)
	}
}

func TestDuplicateSequenceDrop(t *testing.T) {
	h := setupTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_test_key_123")
	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening stream: %v", err)
	}

	callID := "call_duplicate_seq_test_02"
	eventsCh := make(chan *pb.CallEvent, 32)

	go func() {
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				close(eventsCh)
				return
			}
			eventsCh <- event
		}
	}()

	// 1. Send Sequence 1 (processed)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  1,
		Data:      []byte("chunk_1"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 2. Re-send Sequence 1 (Duplicate: already processed! Must be dropped)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  1,
		Data:      []byte("chunk_1_duplicate"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 3. Send Sequence 3 (Buffered, waiting for sequence 2)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  3,
		Data:      []byte("chunk_3"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 4. Re-send Sequence 3 (Duplicate while buffered! Must be dropped)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  3,
		Data:      []byte("chunk_3_duplicate"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 5. Send Sequence 2 (Missing chunk: triggers contiguous processing of 2 and 3)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  2,
		Data:      []byte("chunk_2"),
		IsFinal:   false,
		Timestamp: time.Now().UnixMilli(),
	})
	time.Sleep(20 * time.Millisecond)

	// 6. Send Sequence 4 (Final frame)
	_ = stream.Send(&pb.AudioChunk{
		CallId:    callID,
		TenantId:  "tenant_test",
		Sequence:  4,
		Data:      []byte("chunk_4_final"),
		IsFinal:   true,
		Timestamp: time.Now().UnixMilli(),
	})

	_ = stream.CloseSend()

	for range eventsCh {
	}

	summary, err := h.client.GetCallSummary(authCtx, &pb.SummaryRequest{CallId: callID})
	if err != nil {
		t.Fatalf("failed retrieving summary: %v", err)
	}

	// Must be exactly 4 unique chunks (not 6!)
	if summary.TotalChunks != 4 {
		t.Fatalf("expected exactly 4 total chunks after deduplication, got %d", summary.TotalChunks)
	}
}
