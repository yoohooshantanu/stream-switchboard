package worker

import (
	"hash/fnv"
	"math/rand"
	"testing"
	"time"
)

// hashCallIDStandardFNV reflects the pre-optimization implementation allocating a new fnv.New32a hasher and []byte slice per call
func hashCallIDStandardFNV(callID string, numWorkers int) int {
	if numWorkers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(callID))
	return int(h.Sum32() % uint32(numWorkers))
}

// TestHashCallIDEquivalence verifies that the inline zero-alloc FNV-1a hash matches fnv.New32a bit-for-bit
// across 10,000 random call IDs and diverse worker counts.
func TestHashCallIDEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	workerCounts := []int{1, 2, 3, 4, 5, 8, 12, 16, 32, 64}
	charsets := []string{
		"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
		"0123456789",
		"uuid-sample-format-0123456789abcdef",
	}

	for i := 0; i < 10000; i++ {
		charset := charsets[rng.Intn(len(charsets))]
		length := rng.Intn(48) + 1
		buf := make([]byte, length)
		for j := range buf {
			buf[j] = charset[rng.Intn(len(charset))]
		}
		callID := string(buf)

		for _, wc := range workerCounts {
			expected := hashCallIDStandardFNV(callID, wc)
			actual := hashCallID(callID, wc)
			if actual != expected {
				t.Fatalf("mismatch for callID %q with %d workers: expected %d, got %d", callID, wc, expected, actual)
			}
		}
	}
}

func BenchmarkHashCallID_Old(b *testing.B) {
	sampleCallIDs := []string{
		"call_load_0001",
		"session_telephony_audio_stream_8471947",
		"uuid-9f1c7d2e-3b4a-5c6d-7e8f-9a0b1c2d3e4f",
		"c_12345",
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cid := sampleCallIDs[i%len(sampleCallIDs)]
		_ = hashCallIDStandardFNV(cid, 4)
	}
}

func BenchmarkHashCallID_Inline(b *testing.B) {
	sampleCallIDs := []string{
		"call_load_0001",
		"session_telephony_audio_stream_8471947",
		"uuid-9f1c7d2e-3b4a-5c6d-7e8f-9a0b1c2d3e4f",
		"c_12345",
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cid := sampleCallIDs[i%len(sampleCallIDs)]
		_ = hashCallID(cid, 4)
	}
}
