package router

import (
	"sync"

	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/metrics"
	"github.com/yoohooshantanu/streambridge/internal/pb"
)

// EventRouter manages real-time event distribution and per-call subscription channels
type EventRouter struct {
	mu          sync.RWMutex
	subscribers map[string][]chan *pb.CallEvent
	summaries   map[string]*pb.CallSummary
	closed      bool
}

func NewEventRouter() *EventRouter {
	return &EventRouter{
		subscribers: make(map[string][]chan *pb.CallEvent),
		summaries:   make(map[string]*pb.CallSummary),
	}
}

// Subscribe attaches a new event listener channel for the given callID.
// It returns the receive channel and a thread-safe unsubscribe cancel func.
func (r *EventRouter) Subscribe(callID string, bufferSize int) (<-chan *pb.CallEvent, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bufferSize <= 0 {
		bufferSize = 256
	}
	ch := make(chan *pb.CallEvent, bufferSize)
	r.subscribers[callID] = append(r.subscribers[callID], ch)
	metrics.DefaultMetrics.IncWatcher()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()

			subs := r.subscribers[callID]
			for i, sub := range subs {
				if sub == ch {
					r.subscribers[callID] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
			if len(r.subscribers[callID]) == 0 {
				delete(r.subscribers, callID)
			}
			metrics.DefaultMetrics.DecWatcher()
			close(ch)
		})
	}

	return ch, unsubscribe
}

// Publish broadcasts an event to all active subscribers for the associated callID.
// Non-blocking backpressure drop preserves low latency if a consumer stalls.
func (r *EventRouter) Publish(callID string, event *pb.CallEvent) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return
	}

	subs := r.subscribers[callID]
	if len(subs) == 0 {
		return
	}

	metrics.DefaultMetrics.IncEvent()

	for _, ch := range subs {
		select {
		case ch <- event:
		default:
			// Non-blocking drop protects real-time pipeline latency
			metrics.DefaultMetrics.IncDropped()
			logger.L.Warn("event_dropped_backpressure", "call_id", callID, "event_seq", event.Sequence)
		}
	}
}

// SaveSummary records the aggregated post-call summary
func (r *EventRouter) SaveSummary(summary *pb.CallSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.summaries[summary.CallId] = summary
}

// GetSummary retrieves the aggregated summary for a completed call
func (r *EventRouter) GetSummary(callID string) (*pb.CallSummary, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.summaries[callID]
	return s, ok
}

// SubscriberCount returns active watcher channels for a call
func (r *EventRouter) SubscriberCount(callID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subscribers[callID])
}

// Close gracefully shuts down the router
func (r *EventRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for callID, subs := range r.subscribers {
		for _, ch := range subs {
			close(ch)
		}
		delete(r.subscribers, callID)
	}
	r.closed = true
}
