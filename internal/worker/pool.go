package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/metrics"
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

	// Pre-computed static JSON event payloads for invariant event types to avoid reflection and heap allocation
	intentDetectedPayload    = `{"confidence":0.94,"intent":"customer_support_inquiry"}`
	positiveSentimentPayload = `{"score":0.88,"sentiment":"positive"}`
	neutralSentimentPayload  = `{"score":0.88,"sentiment":"neutral"}`
)

// Strongly typed event payload structs for dynamic events.
// Typed structs eliminate dynamic map allocation, interface{} boxing, and key sorting,
// while guaranteeing RFC 8259-compliant JSON escaping for quotes, backslashes, newlines, and UTF-8 emojis.
type PartialTranscriptEvent struct {
	Text       string  `json:"text"`
	Confidence float64 `json:"confidence"`
	IsSpeaking bool    `json:"is_speaking"`
}

type FinalTranscriptEvent struct {
	Transcript string  `json:"transcript"`
	Confidence float64 `json:"confidence"`
	DurationMs int64   `json:"duration_ms"`
}

type CallErrorEvent struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type WorkerJob struct {
	CallID     string
	Chunk      *pb.AudioChunk
	EnqueuedAt time.Time
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
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	var hash uint32 = offset32
	for i := 0; i < len(callID); i++ {
		hash ^= uint32(callID[i])
		hash *= prime32
	}
	return int(hash % uint32(numWorkers))
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
	payloadBytes, _ := json.Marshal(&CallErrorEvent{
		Error:   errCode,
		Message: errMsg,
	})
	seq := atomic.AddUint64(&p.eventSeq, 1)
	p.router.Publish(callID, &pb.CallEvent{
		CallId:    callID,
		Type:      pb.EventType_EVENT_CALL_ERROR,
		Payload:   string(payloadBytes),
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
	job.EnqueuedAt = time.Now()

	select {
	case <-p.stopCh:
		return false
	case p.workerQueues[workerIdx] <- job:
		metrics.DefaultMetrics.SetWorkerQueueDepth(workerIdx, float64(len(p.workerQueues[workerIdx])))
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
			metrics.DefaultMetrics.SetWorkerQueueDepth(workerID, float64(len(queue)))

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
					metrics.DefaultMetrics.IncSequenceGapTimeout()
					logger.L.Warn("session_sequence_gap_timeout_expired", "call_id", callID, "expected", sess.nextExpectedSeq, "pending", len(sess.pendingChunks), "waited", now.Sub(sess.gapStartTime))
					p.emitCallError(callID, "sequence_gap_timeout", fmt.Sprintf("stream stalled waiting for frame sequence %d for %v; missing frame never arrived", sess.nextExpectedSeq, p.gapTimeout))
					delete(sessions, callID)

					// Bounded tombstone insertion: evicts oldest tombstone if capacity (1024) is reached
					RecordAbortedCall(abortedCalls, callID, now)
				}
			}

		case job, ok := <-queue:
			if !ok {
				return
			}
			metrics.DefaultMetrics.SetWorkerQueueDepth(workerID, float64(len(queue)))
			if !job.EnqueuedAt.IsZero() {
				metrics.DefaultMetrics.ObservePacketQueueWait(time.Since(job.EnqueuedAt).Seconds())
			}
			p.dispatchJob(sessions, abortedCalls, job, workerID)
		}
	}
}

// RecordAbortedCall inserts a tombstone for an aborted callID.
// If the worker-local tombstone map reaches MaxAbortedTombstones (1024), it evicts
// the oldest recorded tombstone (FIFO/LRU timestamp eviction) so new aborts are never rejected.
func RecordAbortedCall(abortedCalls map[string]time.Time, callID string, now time.Time) {
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
			metrics.DefaultMetrics.IncLateFrameTombstoneDropped()
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
			metrics.DefaultMetrics.IncDuplicateFrameDropped()
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
				metrics.DefaultMetrics.IncDuplicateFrameDropped()
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
	procStart := time.Now()
	p.executeChunk(sess, chunk)
	metrics.DefaultMetrics.ObservePacketProcessing(time.Since(procStart).Seconds())

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
		bufProcStart := time.Now()
		p.executeChunk(sess, nextJob.Chunk)
		metrics.DefaultMetrics.ObservePacketProcessing(time.Since(bufProcStart).Seconds())
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
	// If chunk.Data provides explicit text (prefixed with "text:"), use it to allow testing arbitrary speech transcripts;
	// otherwise generate standard audio frame acknowledgment text.
	partialText := fmt.Sprintf("Speaker audio frame %d acknowledged (%d bytes)", chunk.Sequence, len(chunk.Data))
	if len(chunk.Data) > 5 && bytes.HasPrefix(chunk.Data, []byte("text:")) && utf8.Valid(chunk.Data[5:]) {
		partialText = string(chunk.Data[5:])
	}

	partialBytes, _ := json.Marshal(&PartialTranscriptEvent{
		Text:       partialText,
		Confidence: 0.96,
		IsSpeaking: !chunk.IsFinal,
	})

	seq := atomic.AddUint64(&p.eventSeq, 1)
	p.router.Publish(callID, &pb.CallEvent{
		CallId:    callID,
		Type:      pb.EventType_EVENT_TRANSCRIPT_PARTIAL,
		Payload:   string(partialBytes),
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
		if len(chunk.Data) > 11 && bytes.HasPrefix(chunk.Data, []byte("final_text:")) && utf8.Valid(chunk.Data[11:]) {
			finalText = string(chunk.Data[11:])
		}
		finalBytes, _ := json.Marshal(&FinalTranscriptEvent{
			Transcript: finalText,
			Confidence: 0.98,
			DurationMs: dur,
		})
		fSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_TRANSCRIPT_FINAL,
			Payload:   string(finalBytes),
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
