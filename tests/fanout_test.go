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
	// Use growth tier credentials which authenticate as tenant_growth_tier
	authCtx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_live_growth_tier_key")

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

	// Poll until EventRouter has registered all 3 active subscribers
	subDeadline := time.Now().Add(2 * time.Second)
	for h.router.SubscriberCount(callID) < numWatchers {
		if time.Now().After(subDeadline) {
			t.Fatalf("timed out waiting for %d active subscribers in router, got %d", numWatchers, h.router.SubscriberCount(callID))
		}
		time.Sleep(2 * time.Millisecond)
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

func TestSubscriberBufferSaturationBackpressureDrop(t *testing.T) {
	h := setupTestHarness(t)

	callID := "call_backpressure_test_01"
	tenantID := "tenant_test"

	// Subscribe with a small buffer of 4 items
	const bufferCap = 4
	ch, unsubscribe, err := h.router.SubscribeForTenant(callID, tenantID, bufferCap)
	if err != nil {
		t.Fatalf("failed subscribing: %v", err)
	}
	defer unsubscribe()

	// Publish 10 events without draining channel to saturate the buffer
	const totalEvents = 10
	for i := 1; i <= totalEvents; i++ {
		h.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_TRANSCRIPT_PARTIAL,
			Sequence:  uint64(i),
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Verify the channel contains exactly bufferCap (4) events
	// Non-blocking drop must not stall publisher or crash
	var receivedCount int
	for i := 0; i < bufferCap; i++ {
		select {
		case ev := <-ch:
			receivedCount++
			if ev.Sequence != uint64(i+1) {
				t.Errorf("expected sequence %d, got %d", i+1, ev.Sequence)
			}
		default:
			t.Fatalf("expected buffered event %d, but channel was empty", i+1)
		}
	}

	// Channel must now be empty; events 5 through 10 were dropped due to non-blocking backpressure
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra event in saturated channel: seq=%d", extra.Sequence)
	default:
		// Expected: buffer was bounded at 4
	}

	if receivedCount != bufferCap {
		t.Fatalf("expected exactly %d received events, got %d", bufferCap, receivedCount)
	}
}
