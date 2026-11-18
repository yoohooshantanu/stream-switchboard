package router

import (
	"sync"

	"github.com/yoohooshantanu/audio-gateway/internal/logger"
	"github.com/yoohooshantanu/audio-gateway/internal/metrics"
	"github.com/yoohooshantanu/audio-gateway/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EventRouter manages real-time event distribution, tenant isolation, and per-call subscription channels
type EventRouter struct {
	mu             sync.RWMutex
	subscribers    map[string][]chan *pb.CallEvent
	summaries      map[string]*pb.CallSummary
	callTenants    map[string]string
	summaryTenants map[string]string
	closed         bool
}

func NewEventRouter() *EventRouter {
	return &EventRouter{
		subscribers:    make(map[string][]chan *pb.CallEvent),
		summaries:      make(map[string]*pb.CallSummary),
		callTenants:    make(map[string]string),
		summaryTenants: make(map[string]string),
	}
}

// RegisterCall claims ownership of a call ID for a specific tenant.
// Rejects attempt if call is already claimed by a different tenant.
func (r *EventRouter) RegisterCall(callID, tenantID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.callTenants[callID]; ok && existing != tenantID && tenantID != "" {
		return status.Errorf(codes.PermissionDenied, "call %q is owned by tenant %q", callID, existing)
	}

	if tenantID != "" {
		r.callTenants[callID] = tenantID
	}
	return nil
}

// GetCallTenant returns the tenant ID that owns the specified call
func (r *EventRouter) GetCallTenant(callID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tenant, ok := r.callTenants[callID]
	return tenant, ok
}

// Subscribe attaches a new event listener channel for the given callID without tenant checks
func (r *EventRouter) Subscribe(callID string, bufferSize int) (<-chan *pb.CallEvent, func()) {
	ch, unsub, _ := r.SubscribeForTenant(callID, "", bufferSize)
	return ch, unsub
}

// SubscribeForTenant attaches an event listener channel with tenant isolation enforcement.
// If the call is owned by another tenant, returns codes.PermissionDenied.
func (r *EventRouter) SubscribeForTenant(callID, tenantID string, bufferSize int) (<-chan *pb.CallEvent, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if owner, ok := r.callTenants[callID]; ok && tenantID != "" && owner != tenantID {
		return nil, nil, status.Errorf(codes.PermissionDenied, "access denied: call belongs to another tenant")
	}

	if tenantID != "" {
		if _, ok := r.callTenants[callID]; !ok {
			r.callTenants[callID] = tenantID
		}
	}

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

	return ch, unsubscribe, nil
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

// SaveSummary records the aggregated post-call summary and binds its owning tenant
func (r *EventRouter) SaveSummary(summary *pb.CallSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Enforce maximum stored summaries to prevent unbounded memory growth
	if len(r.summaries) > 10000 {
		for k := range r.summaries {
			delete(r.summaries, k)
			delete(r.summaryTenants, k)
			break
		}
	}

	r.summaries[summary.CallId] = summary
	if tenant, ok := r.callTenants[summary.CallId]; ok {
		r.summaryTenants[summary.CallId] = tenant
	}
}

// GetSummary retrieves the aggregated summary for a completed call
func (r *EventRouter) GetSummary(callID string) (*pb.CallSummary, bool) {
	s, _, ok := r.GetSummaryWithTenant(callID)
	return s, ok
}

// GetSummaryWithTenant retrieves the summary and the owning tenant ID
func (r *EventRouter) GetSummaryWithTenant(callID string) (*pb.CallSummary, string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.summaries[callID]
	tenant := r.summaryTenants[callID]
	if !ok && tenant == "" {
		tenant = r.callTenants[callID]
	}
	return s, tenant, ok
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
