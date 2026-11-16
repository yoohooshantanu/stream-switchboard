package worker

import (
	"encoding/json"
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
)

type WorkerJob struct {
	CallID string
	Chunk  *pb.AudioChunk
}

type callSession struct {
	callID          string
	nextExpectedSeq uint64
	pendingChunks   map[uint64]*WorkerJob
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
	}
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	queue := p.workerQueues[workerID]

	for {
		select {
		case <-p.stopCh:
			return

		case <-ticker.C:
			// Evict abandoned sessions with no activity for > 15 minutes
			now := time.Now()
			for callID, sess := range sessions {
				if now.Sub(sess.lastActivity) > 15*time.Minute {
					delete(sessions, callID)
					logger.L.Debug("evicted_abandoned_session", "call_id", callID, "worker_id", workerID)
				}
			}

		case job, ok := <-queue:
			if !ok {
				return
			}
			p.dispatchJob(sessions, job, workerID)
		}
	}
}

func (p *AIWorkerPool) dispatchJob(sessions map[string]*callSession, job *WorkerJob, workerID int) {
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
			// Reject excessively large sequence gaps (protects against malformed attacks)
			if chunk.Sequence > sess.nextExpectedSeq+MaxSequenceGap {
				logger.L.Warn("dropping_chunk_excessive_sequence_gap", "call_id", callID, "seq", chunk.Sequence, "expected", sess.nextExpectedSeq)
				return
			}

			// Reject frame if per-call pending buffer is saturated (protects against memory exhaustion)
			if len(sess.pendingChunks) >= MaxPendingChunksPerCall {
				logger.L.Warn("dropping_chunk_pending_buffer_saturated", "call_id", callID, "seq", chunk.Sequence, "buffered", len(sess.pendingChunks))
				return
			}

			if _, alreadyBuffered := sess.pendingChunks[chunk.Sequence]; alreadyBuffered {
				logger.L.Debug("ignoring_duplicate_buffered_chunk", "call_id", callID, "seq", chunk.Sequence)
				return
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
	partialText := fmt.Sprintf("Speaker audio frame %d acknowledged (%d bytes)", chunk.Sequence, len(chunk.Data))
	partialPayload, _ := json.Marshal(map[string]interface{}{
		"text":        partialText,
		"confidence":  0.96,
		"is_speaking": !chunk.IsFinal,
	})

	seq := atomic.AddUint64(&p.eventSeq, 1)
	p.router.Publish(callID, &pb.CallEvent{
		CallId:    callID,
		Type:      pb.EventType_EVENT_TRANSCRIPT_PARTIAL,
		Payload:   string(partialPayload),
		Sequence:  seq,
		Timestamp: nowMs,
	})
	sess.eventCount++

	// 2. Real-time Sentiment every 3rd chunk
	if chunkCount%3 == 0 {
		sentiment := "positive"
		if chunkCount%2 == 0 {
			sentiment = "neutral"
		}
		sentPayload, _ := json.Marshal(map[string]interface{}{
			"sentiment": sentiment,
			"score":     0.88,
		})
		sSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_SENTIMENT,
			Payload:   string(sentPayload),
			Sequence:  sSeq,
			Timestamp: nowMs,
		})
		sess.eventCount++
	}

	// 3. Finalization events upon last chunk
	if chunk.IsFinal {
		// Final Transcript Event
		finalText := fmt.Sprintf("Full transcribed call session consisting of %d chunks.", chunkCount)
		finalPayload, _ := json.Marshal(map[string]interface{}{
			"transcript":  finalText,
			"confidence":  0.98,
			"duration_ms": time.Since(sess.startTime).Milliseconds(),
		})
		fSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_TRANSCRIPT_FINAL,
			Payload:   string(finalPayload),
			Sequence:  fSeq,
			Timestamp: nowMs,
		})
		sess.eventCount++

		// Intent Detection Event
		intentPayload, _ := json.Marshal(map[string]interface{}{
			"intent":     "customer_support_inquiry",
			"confidence": 0.94,
		})
		iSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_INTENT_DETECTED,
			Payload:   string(intentPayload),
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

		dur := time.Since(sess.startTime).Milliseconds()
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
