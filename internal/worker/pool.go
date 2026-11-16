package worker

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/pb"
	"github.com/yoohooshantanu/streambridge/internal/router"
)

const (
	// MaxPendingChunksPerCall limits out-of-order buffered frames per call to prevent memory exhaustion
	MaxPendingChunksPerCall = 64
	// MaxSequenceGap limits the forward gap distance from nextExpectedSeq to detect malformed streams
	MaxSequenceGap = 128
	// DefaultGapTimeout is the maximum duration an out-of-order sequence gap can stall before the session aborts with EVENT_CALL_ERROR
	DefaultGapTimeout = 2 * time.Second
	// AbortedCallTombstoneTTL defines how long aborted callIDs are remembered to prevent zombie resurrects.
	// 60 seconds exceeds typical carrier SIP/WebRTC renegotiation and teardown windows.
	AbortedCallTombstoneTTL = 60 * time.Second
	// MaxAbortedTombstones caps worker-local memory to 1024 tombstones per worker.
	MaxAbortedTombstones = 1024

	// Pre-computed static JSON event payloads to avoid reflection and heap allocation in hot audio path
	intentDetectedPayload   = `{"confidence":0.94,"intent":"customer_support_inquiry"}`
	positiveSentimentPayload = `{"score":0.88,"sentiment":"positive"}`
	neutralSentimentPayload  = `{"score":0.88,"sentiment":"neutral"}`
)

type WorkerJob struct {
	CallID string
	Chunk  *pb.AudioChunk
}

type callSession struct {
	callID          string
	nextExpectedSeq uint64
	pendingChunks   map[uint64]*WorkerJob
	gapStartTime    time.Time
	chunkCount      int32
	eventCount      int32
	startTime       time.Time
	lastActivity    time.Time
}

type AIWorkerPool struct {
	router       *router.EventRouter
	numWorkers   int
	workerQueues []chan *WorkerJob
	wg           sync.WaitGroup
	stopCh       chan struct{}
	stopOnce     sync.Once
	eventSeq     uint64
	gapTimeout   time.Duration
}

func hashCallID(callID string, numWorkers int) int {
	if numWorkers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(callID))
	return int(h.Sum32() % uint32(numWorkers))
}

func NewAIWorkerPool(router *router.EventRouter, numWorkers, queueCapacity int) *AIWorkerPool {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if queueCapacity <= 0 {
		queueCapacity = 1024
	}

	perWorkerCap := queueCapacity / numWorkers
	if perWorkerCap < 64 {
		perWorkerCap = 64
	}

	workerQueues := make([]chan *WorkerJob, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerQueues[i] = make(chan *WorkerJob, perWorkerCap)
	}

	return &AIWorkerPool{
		router:       router,
		numWorkers:   numWorkers,
		workerQueues: workerQueues,
		stopCh:       make(chan struct{}),
		gapTimeout:   DefaultGapTimeout,
	}
}

// SetGapTimeout configures the maximum duration a sequence gap can remain open before aborting
func (p *AIWorkerPool) SetGapTimeout(d time.Duration) {
	if d > 0 {
		p.gapTimeout = d
	}
}

func (p *AIWorkerPool) emitCallError(callID, errCode, errMsg string) {
	payload := fmt.Sprintf(`{"error":%q,"message":%q}`, errCode, errMsg)
	seq := atomic.AddUint64(&p.eventSeq, 1)
	p.router.Publish(callID, &pb.CallEvent{
		CallId:    callID,
		Type:      pb.EventType_EVENT_CALL_ERROR,
		Payload:   payload,
		Sequence:  seq,
		Timestamp: time.Now().UnixMilli(),
	})
}

func (p *AIWorkerPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
	logger.L.Info("ai_worker_pool_started", "workers", p.numWorkers)
}

func (p *AIWorkerPool) Submit(job *WorkerJob) bool {
	if job == nil || job.CallID == "" {
		return false
	}

	workerIdx := hashCallID(job.CallID, p.numWorkers)

	select {
	case <-p.stopCh:
		return false
	case p.workerQueues[workerIdx] <- job:
		return true
	}
}

func (p *AIWorkerPool) workerLoop(workerID int) {
	defer p.wg.Done()

	// Share-Nothing Worker State:
	// Each worker goroutine exclusively owns the state for its partitioned callIDs.
	// No mutex contention across workers!
	sessions := make(map[string]*callSession)
	abortedCalls := make(map[string]time.Time)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	queue := p.workerQueues[workerID]

	for {
		select {
		case <-p.stopCh:
			return

		case <-ticker.C:
			now := time.Now()

			// Evict expired tombstones
			for cID, abortedAt := range abortedCalls {
				if now.Sub(abortedAt) > AbortedCallTombstoneTTL {
					delete(abortedCalls, cID)
				}
			}

			for callID, sess := range sessions {
				// Evict abandoned sessions with no activity for > 15 minutes
				if now.Sub(sess.lastActivity) > 15*time.Minute {
					delete(sessions, callID)
					logger.L.Debug("evicted_abandoned_session", "call_id", callID, "worker_id", workerID)
					continue
				}

				// Sequence gap timeout: if an out-of-order gap stall exceeds gapTimeout, abort with explicit EVENT_CALL_ERROR
				if len(sess.pendingChunks) > 0 && !sess.gapStartTime.IsZero() && now.Sub(sess.gapStartTime) >= p.gapTimeout {
					logger.L.Warn("session_sequence_gap_timeout_expired", "call_id", callID, "expected", sess.nextExpectedSeq, "pending", len(sess.pendingChunks), "waited", now.Sub(sess.gapStartTime))
					p.emitCallError(callID, "sequence_gap_timeout", fmt.Sprintf("stream stalled waiting for frame sequence %d for %v; missing frame never arrived", sess.nextExpectedSeq, p.gapTimeout))
					delete(sessions, callID)

					// Bounded tombstone insertion: evict oldest if capacity is reached
					if len(abortedCalls) >= MaxAbortedTombstones {
						var oldestKey string
						var oldestTime time.Time
						for k, t := range abortedCalls {
							if oldestKey == "" || t.Before(oldestTime) {
								oldestKey = k
								oldestTime = t
							}
						}
						delete(abortedCalls, oldestKey)
					}
					abortedCalls[callID] = now
				}
			}

		case job, ok := <-queue:
			if !ok {
				return
			}
			p.dispatchJob(sessions, abortedCalls, job, workerID)
		}
	}
}

func (p *AIWorkerPool) dispatchJob(sessions map[string]*callSession, abortedCalls map[string]time.Time, job *WorkerJob, workerID int) {
	defer func() {
		if r := recover(); r != nil {
			logger.L.Error("worker_job_dispatch_panic_recovered", "call_id", job.CallID, "worker_id", workerID, "panic", r)
		}
	}()

	callID := job.CallID
	chunk := job.Chunk
	if chunk == nil {
		return
	}

	// Drop chunks destined for previously aborted calls to prevent zombie session resurrects
	if abortedAt, isAborted := abortedCalls[callID]; isAborted {
		if time.Since(abortedAt) <= AbortedCallTombstoneTTL {
			logger.L.Debug("dropping_chunk_for_aborted_session", "call_id", callID, "seq", chunk.Sequence)
			return
		}
		delete(abortedCalls, callID)
	}

	sess, exists := sessions[callID]
	if !exists {
		sess = &callSession{
			callID:          callID,
			nextExpectedSeq: 1, // Standard streams always sequence from 1
			pendingChunks:   make(map[uint64]*WorkerJob),
			startTime:       time.Now(),
			lastActivity:    time.Now(),
		}
		sessions[callID] = sess
	}
	sess.lastActivity = time.Now()

	// Handle monotonic sequencing for sequenced frames (Sequence > 0)
	if chunk.Sequence > 0 {
		// 1. Drop already-processed duplicate sequence
		if chunk.Sequence < sess.nextExpectedSeq {
			logger.L.Debug("ignoring_duplicate_processed_chunk", "call_id", callID, "seq", chunk.Sequence, "expected", sess.nextExpectedSeq)
			return
		}

		// 2. Buffer out-of-order future chunks (including initial gap where seq > 1 arrives first)
		if chunk.Sequence > sess.nextExpectedSeq {
			// Reject excessively large sequence gaps (protects against malformed attacks) and notify caller
			if chunk.Sequence > sess.nextExpectedSeq+MaxSequenceGap {
				logger.L.Warn("dropping_chunk_excessive_sequence_gap", "call_id", callID, "seq", chunk.Sequence, "expected", sess.nextExpectedSeq)
				p.emitCallError(callID, "excessive_sequence_gap", fmt.Sprintf("chunk sequence %d exceeds max forward gap (%d frames from %d)", chunk.Sequence, MaxSequenceGap, sess.nextExpectedSeq))
				return
			}

			// Reject frame if per-call pending buffer is saturated (protects against memory exhaustion) and notify caller
			if len(sess.pendingChunks) >= MaxPendingChunksPerCall {
				logger.L.Warn("dropping_chunk_pending_buffer_saturated", "call_id", callID, "seq", chunk.Sequence, "buffered", len(sess.pendingChunks))
				p.emitCallError(callID, "pending_buffer_saturated", fmt.Sprintf("pending reassembly buffer saturated (%d frames max)", MaxPendingChunksPerCall))
				return
			}

			if _, alreadyBuffered := sess.pendingChunks[chunk.Sequence]; alreadyBuffered {
				logger.L.Debug("ignoring_duplicate_buffered_chunk", "call_id", callID, "seq", chunk.Sequence)
				return
			}
			if len(sess.pendingChunks) == 0 {
				sess.gapStartTime = time.Now()
			}
			sess.pendingChunks[chunk.Sequence] = job
			return
		}
	}

	// 3. Process current chunk (matches nextExpectedSeq, or chunk.Sequence == 0)
	p.executeChunk(sess, chunk)
	if chunk.Sequence > 0 {
		sess.nextExpectedSeq = chunk.Sequence + 1
	}

	isFinalEncountered := chunk.IsFinal

	// 4. Process any buffered chunks that now match contiguous sequence
	for {
		nextJob, hasNext := sess.pendingChunks[sess.nextExpectedSeq]
		if !hasNext {
			break
		}
		delete(sess.pendingChunks, sess.nextExpectedSeq)
		p.executeChunk(sess, nextJob.Chunk)
		if nextJob.Chunk.IsFinal {
			isFinalEncountered = true
		}
		sess.nextExpectedSeq++
	}

	if len(sess.pendingChunks) == 0 {
		sess.gapStartTime = time.Time{}
	}

	// 5. If call completed, remove session from worker state
	if isFinalEncountered {
		delete(sessions, callID)
	}
}

func (p *AIWorkerPool) executeChunk(sess *callSession, chunk *pb.AudioChunk) {
	callID := sess.callID
	sess.chunkCount++
	chunkCount := sess.chunkCount
	nowMs := time.Now().UnixMilli()

	// 1. ASR: emit partial transcript on chunk
	// Formatted directly without dynamic map allocation or reflection-based JSON marshaling
	partialPayload := fmt.Sprintf(`{"confidence":0.96,"is_speaking":%t,"text":"Speaker audio frame %d acknowledged (%d bytes)"}`,
		!chunk.IsFinal, chunk.Sequence, len(chunk.Data))

	seq := atomic.AddUint64(&p.eventSeq, 1)
	p.router.Publish(callID, &pb.CallEvent{
		CallId:    callID,
		Type:      pb.EventType_EVENT_TRANSCRIPT_PARTIAL,
		Payload:   partialPayload,
		Sequence:  seq,
		Timestamp: nowMs,
	})
	sess.eventCount++

	// 2. Real-time Sentiment every 3rd chunk (zero-alloc pre-computed payload)
	if chunkCount%3 == 0 {
		sentPayload := positiveSentimentPayload
		if chunkCount%2 == 0 {
			sentPayload = neutralSentimentPayload
		}
		sSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_SENTIMENT,
			Payload:   sentPayload,
			Sequence:  sSeq,
			Timestamp: nowMs,
		})
		sess.eventCount++
	}

	// 3. Finalization events upon last chunk
	if chunk.IsFinal {
		dur := time.Since(sess.startTime).Milliseconds()

		// Final Transcript Event
		finalText := fmt.Sprintf("Full transcribed call session consisting of %d chunks.", chunkCount)
		finalPayload := fmt.Sprintf(`{"confidence":0.98,"duration_ms":%d,"transcript":%q}`, dur, finalText)
		fSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_TRANSCRIPT_FINAL,
			Payload:   finalPayload,
			Sequence:  fSeq,
			Timestamp: nowMs,
		})
		sess.eventCount++

		// Intent Detection Event (zero-alloc static payload)
		iSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_INTENT_DETECTED,
			Payload:   intentDetectedPayload,
			Sequence:  iSeq,
			Timestamp: nowMs,
		})
		sess.eventCount++

		// Call Ended Event
		endSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_CALL_ENDED,
			Payload:   `{"status":"completed"}`,
			Sequence:  endSeq,
			Timestamp: nowMs,
		})
		sess.eventCount++

		totEvents := sess.eventCount

		summary := &pb.CallSummary{
			CallId:      callID,
			Transcript:  finalText,
			Sentiment:   "positive",
			Intent:      "customer_support_inquiry",
			DurationMs:  dur,
			TotalChunks: chunkCount,
			TotalEvents: totEvents,
		}
		p.router.SaveSummary(summary)
		logger.L.Info("call_summary_saved", "call_id", callID, "chunks", chunkCount, "events", totEvents)
	}
}

func (p *AIWorkerPool) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.wg.Wait()
	logger.L.Info("ai_worker_pool_stopped")
}
