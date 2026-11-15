package tests

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/pb"
	"google.golang.org/grpc/metadata"
)

func TestWatchCallFanOut(t *testing.T) {
	h := setupTestHarness(t)

	callID := "call_fanout_multitenant_01"
	authCtx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

	// 1. Subscribe 3 concurrent WatchCall listeners (representing CRM, Analytics, Supervisor)
	numWatchers := 3
	var wg sync.WaitGroup
	watcherReceived := make([]int, numWatchers)
	watcherCancels := make([]context.CancelFunc, numWatchers)

	for i := 0; i < numWatchers; i++ {
		wIdx := i
		wCtx, cancel := context.WithCancel(authCtx)
		watcherCancels[wIdx] = cancel

		watchStream, err := h.client.WatchCall(wCtx, &pb.WatchRequest{
			CallId:   callID,
			TenantId: "tenant_growth_tier",
		})
		if err != nil {
			t.Fatalf("watcher %d failed to subscribe: %v", wIdx, err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ev, err := watchStream.Recv()
				if err != nil {
					return
				}
				if ev != nil && ev.CallId == callID {
					watcherReceived[wIdx]++
				}
			}
		}()
	}

	// Give watcher subscriptions a brief moment to register
	time.Sleep(30 * time.Millisecond)

	// Verify EventRouter has 3 active subscribers registered
	if count := h.router.SubscriberCount(callID); count != numWatchers {
		t.Fatalf("expected %d active subscribers in router, got %d", numWatchers, count)
	}

	// 2. Open StreamCall and stream audio frames into the call
	stream, err := h.client.StreamCall(authCtx)
	if err != nil {
		t.Fatalf("failed opening caller stream: %v", err)
	}

	// Read stream responses in background to keep stream flow healthy
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF || err != nil {
				return
			}
		}
	}()

	totalFrames := 4
	for seq := 1; seq <= totalFrames; seq++ {
		isFinal := (seq == totalFrames)
		_ = stream.Send(&pb.AudioChunk{
			CallId:    callID,
			TenantId:  "tenant_growth_tier",
			Sequence:  uint64(seq),
			Data:      []byte("telephony_streaming_frame"),
			IsFinal:   isFinal,
			Timestamp: time.Now().UnixMilli(),
		})
		time.Sleep(20 * time.Millisecond)
	}
	_ = stream.CloseSend()

	// Wait briefly for all AI worker events to fan out to watchers
	time.Sleep(100 * time.Millisecond)

	// Cancel watchers to cleanly terminate their recv loops
	for _, cancel := range watcherCancels {
		cancel()
	}
	wg.Wait()

	// 3. Verify all 3 watchers received the fanned out events
	for i := 0; i < numWatchers; i++ {
		if watcherReceived[i] == 0 {
			t.Errorf("watcher %d received 0 events", i)
		}
	}

	// 4. Verify subscriber list was cleaned up in router
	time.Sleep(20 * time.Millisecond)
	if remaining := h.router.SubscriberCount(callID); remaining != 0 {
		t.Errorf("expected 0 subscribers remaining after unsubscription, got %d", remaining)
	}
}
