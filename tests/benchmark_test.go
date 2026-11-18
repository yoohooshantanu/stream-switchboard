package tests

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/yoohooshantanu/audio-gateway/internal/logger"
	"github.com/yoohooshantanu/audio-gateway/internal/pb"
	"google.golang.org/grpc/metadata"
)

func BenchmarkAudioGatewayThroughput(b *testing.B) {
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

		for seq := 1; seq <= 3; seq++ {
			_ = stream.Send(&pb.AudioChunk{
				CallId:    callID,
				TenantId:  "tenant_test",
				Sequence:  uint64(seq),
				Data:      []byte("benchmark_audio_pcm_frame_bytes"),
				IsFinal:   seq == 3,
				Timestamp: time.Now().UnixMilli(),
			})
		}
		_ = stream.CloseSend()

		for {
			_, err := stream.Recv()
			if err == io.EOF || err != nil {
				break
			}
		}
	}
}

func BenchmarkConcurrentStreamsWithGaps(b *testing.B) {
	h := setupTestHarnessBench(b)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "sb_test_key_123")

	// -------
	// Pre-generate deterministic scrambled sequence orders using fixed seed 42
	// to isolate audio-gateway reassembly & processing overhead from RNG calls.
	// -------
	rnd := rand.New(rand.NewSource(42))
	const numConcurrentCalls = 20
	const chunksPerCall = 8

	type callPattern struct {
		seqs []uint64
	}
	patterns := make([]callPattern, numConcurrentCalls)
	for c := 0; c < numConcurrentCalls; c++ {
		perm := rnd.Perm(chunksPerCall)
		seqs := make([]uint64, chunksPerCall)
		for idx, p := range perm {
			seqs[idx] = uint64(p + 1)
		}
		patterns[c] = callPattern{seqs: seqs}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numConcurrentCalls)

		for c := 0; c < numConcurrentCalls; c++ {
			callIdx := c
			seqOrder := patterns[callIdx].seqs

			go func(cID int, seqs []uint64) {
				defer wg.Done()

				callID := fmt.Sprintf("bench_gap_%d_%d", i, cID)
				stream, err := h.client.StreamCall(ctx)
				if err != nil {
					return
				}

				for _, seq := range seqs {
					isFinal := (seq == uint64(chunksPerCall))
					_ = stream.Send(&pb.AudioChunk{
						CallId:    callID,
						TenantId:  "tenant_test",
						Sequence:  seq,
						Data:      []byte("simulated_pcm_16khz_audio_payload_bytes_for_bench"),
						IsFinal:   isFinal,
						Timestamp: 1700000000000 + int64(seq),
					})
				}
				_ = stream.CloseSend()

				for {
					_, recvErr := stream.Recv()
					if recvErr != nil {
						break
					}
				}
			}(callIdx, seqOrder)
		}
		wg.Wait()
	}
}

func setupTestHarnessBench(b *testing.B) *testHarness {
	logger.SetDiscard()
	var t testing.T
	h := setupTestHarness(&t)
	b.Cleanup(func() {
		_ = h.conn.Close()
		h.grpcServer.Stop()
		h.pool.Stop()
		h.router.Close()
		_ = h.lis.Close()
		logger.ResetStdout()
	})
	return h
}
