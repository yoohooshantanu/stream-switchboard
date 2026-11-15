package worker

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/pb"
	"github.com/yoohooshantanu/streambridge/internal/router"
)

type WorkerJob struct {
	CallID string
	Chunk  *pb.AudioChunk
}

type AIWorkerPool struct {
	inputQueue   chan *WorkerJob
	router       *router.EventRouter
	numWorkers   int
	wg           sync.WaitGroup
	stopCh       chan struct{}
	stopOnce     sync.Once
	eventSeq     uint64
	callChunks   map[string]int32
	callEvents   map[string]int32
	callStart    map[string]time.Time
	lastActivity map[string]time.Time
	callMu       sync.Mutex
}

func NewAIWorkerPool(router *router.EventRouter, numWorkers, queueCapacity int) *AIWorkerPool {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if queueCapacity <= 0 {
		queueCapacity = 1024
	}

	return &AIWorkerPool{
		inputQueue:   make(chan *WorkerJob, queueCapacity),
		router:       router,
		numWorkers:   numWorkers,
		stopCh:       make(chan struct{}),
		callChunks:   make(map[string]int32),
		callEvents:   make(map[string]int32),
		callStart:    make(map[string]time.Time),
		lastActivity: make(map[string]time.Time),
	}
}

func (p *AIWorkerPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
	p.wg.Add(1)
	go p.cleanupLoop()
	logger.L.Info("ai_worker_pool_started", "workers", p.numWorkers)
}

func (p *AIWorkerPool) cleanupLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.callMu.Lock()
			now := time.Now()
			for callID, last := range p.lastActivity {
				// Evict abandoned calls where client dropped connection without is_final
				if now.Sub(last) > 15*time.Minute {
					delete(p.callStart, callID)
					delete(p.callChunks, callID)
					delete(p.callEvents, callID)
					delete(p.lastActivity, callID)
					logger.L.Debug("evicted_abandoned_call_state", "call_id", callID)
				}
			}
			p.callMu.Unlock()
		}
	}
}

func (p *AIWorkerPool) Submit(job *WorkerJob) bool {
	select {
	case <-p.stopCh:
		return false
	case p.inputQueue <- job:
		return true
	}
}

func (p *AIWorkerPool) workerLoop(workerID int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case job, ok := <-p.inputQueue:
			if !ok {
				return
			}
			p.processChunk(job)
		}
	}
}

func (p *AIWorkerPool) processChunk(job *WorkerJob) {
	defer func() {
		if r := recover(); r != nil {
			logger.L.Error("worker_chunk_processing_panic_recovered", "call_id", job.CallID, "panic", r)
		}
	}()

	callID := job.CallID
	chunk := job.Chunk

	p.callMu.Lock()
	now := time.Now()
	p.lastActivity[callID] = now
	if _, ok := p.callStart[callID]; !ok {
		p.callStart[callID] = now
	}
	p.callChunks[callID]++
	chunkCount := p.callChunks[callID]
	p.callMu.Unlock()

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
	p.incCallEvents(callID)

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
		p.incCallEvents(callID)
	}

	// 3. Finalization events upon last chunk
	if chunk.IsFinal {
		// Final Transcript Event
		finalText := fmt.Sprintf("Full transcribed call session consisting of %d chunks.", chunkCount)
		finalPayload, _ := json.Marshal(map[string]interface{}{
			"transcript":  finalText,
			"confidence":  0.98,
			"duration_ms": time.Since(p.callStart[callID]).Milliseconds(),
		})
		fSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_TRANSCRIPT_FINAL,
			Payload:   string(finalPayload),
			Sequence:  fSeq,
			Timestamp: nowMs,
		})
		p.incCallEvents(callID)

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
		p.incCallEvents(callID)

		// Call Ended Event
		endSeq := atomic.AddUint64(&p.eventSeq, 1)
		p.router.Publish(callID, &pb.CallEvent{
			CallId:    callID,
			Type:      pb.EventType_EVENT_CALL_ENDED,
			Payload:   `{"status":"completed"}`,
			Sequence:  endSeq,
			Timestamp: nowMs,
		})
		p.incCallEvents(callID)

		// Aggregate and persist CallSummary
		p.callMu.Lock()
		dur := time.Since(p.callStart[callID]).Milliseconds()
		totEvents := p.callEvents[callID]
		// Clean up completed call state from in-memory tracking maps
		delete(p.callStart, callID)
		delete(p.callChunks, callID)
		delete(p.callEvents, callID)
		delete(p.lastActivity, callID)
		p.callMu.Unlock()

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

func (p *AIWorkerPool) incCallEvents(callID string) {
	p.callMu.Lock()
	defer p.callMu.Unlock()
	p.callEvents[callID]++
}

func (p *AIWorkerPool) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.wg.Wait()
	logger.L.Info("ai_worker_pool_stopped")
}
