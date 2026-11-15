package tests

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/pb"
	"google.golang.org/grpc/metadata"
)

func BenchmarkStreamBridgeThroughput(b *testing.B) {
	lis := setupTestHarnessBench(b)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		callID := fmt.Sprintf("call_bench_%d", i)
		stream, err := lis.client.StreamCall(ctx)
		if err != nil {
			b.Fatalf("failed opening stream: %v", err)
		}

		// Send 3 audio frames
		for seq := 1; seq <= 3; seq++ {
			_ = stream.Send(&pb.AudioChunk{
				CallId:    callID,
				TenantId:  "bench_tenant",
				Sequence:  uint64(seq),
				Data:      []byte("benchmark_audio_pcm_frame_bytes"),
				IsFinal:   seq == 3,
				Timestamp: time.Now().UnixMilli(),
			})
		}
		_ = stream.CloseSend()

		// Drain responses
		for {
			_, err := stream.Recv()
			if err == io.EOF || err != nil {
				break
			}
		}
	}
}

func setupTestHarnessBench(b *testing.B) *testHarness {
	// Re-use same setup logic for benchmark
	var t testing.T
	h := setupTestHarness(&t)
	b.Cleanup(func() {
		_ = h.conn.Close()
		h.grpcServer.Stop()
		h.pool.Stop()
		h.router.Close()
		_ = h.lis.Close()
	})
	return h
}
