package interceptor

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const HeaderAPIKey = "x-api-key"

type tenantCtxKey struct{}

var tenantKey = tenantCtxKey{}

// TenantFromContext extracts the authenticated tenant ID from the context
func TenantFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(tenantKey).(string)
	return val, ok && val != ""
}

// ContextWithTenant injects the authenticated tenant ID into the context
func ContextWithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantKey, tenantID)
}

// KeyRegistry defines lookup and registration of API keys bound to tenant IDs
type KeyRegistry interface {
	LookupTenant(apiKey string) (string, bool)
	RegisterKey(apiKey, tenantID string)
}

// MemoryKeyRegistry is a thread-safe registry of valid API keys and tenant bindings
type MemoryKeyRegistry struct {
	mu   sync.RWMutex
	keys map[string]string
}

// NewKeyRegistry creates a KeyRegistry initialized with default keys and environment overrides
func NewKeyRegistry() *MemoryKeyRegistry {
	r := &MemoryKeyRegistry{
		keys: map[string]string{
			"sb_test_key_123":         "tenant_test",
			"sb_live_acme_corp_sec99": "tenant_acme_corp",
			"sb_live_growth_tier_key": "tenant_growth_tier",
		},
	}

	// Support explicit STREAMBRIDGE_API_KEY from environment
	if envKey := os.Getenv("STREAMBRIDGE_API_KEY"); envKey != "" {
		r.keys[envKey] = "tenant_production"
	}

	// Support comma-separated STREAMBRIDGE_API_KEYS (format: "key1:tenant1,key2:tenant2")
	if multiKeys := os.Getenv("STREAMBRIDGE_API_KEYS"); multiKeys != "" {
		pairs := strings.Split(multiKeys, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
				r.keys[parts[0]] = parts[1]
			}
		}
	}

	return r
}

func (r *MemoryKeyRegistry) LookupTenant(apiKey string) (string, bool) {
	if apiKey == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	tenant, ok := r.keys[apiKey]
	return tenant, ok
}

func (r *MemoryKeyRegistry) RegisterKey(apiKey, tenantID string) {
	if apiKey == "" || tenantID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[apiKey] = tenantID
}

// Global registry instance
var DefaultRegistry = NewKeyRegistry()

// ValidateAPIKey checks if the incoming API key is registered and returns its bound tenant ID.
// Prefix-based wildcards are strictly prohibited.
func ValidateAPIKey(key string) (string, bool) {
	return DefaultRegistry.LookupTenant(key)
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// UnaryAuthInterceptor verifies metadata headers on unary RPCs and binds tenant context
func UnaryAuthInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// Skip auth check for standard health probes
	if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") {
		return handler(ctx, req)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		metrics.DefaultMetrics.IncRPC(true)
		return nil, status.Error(codes.Unauthenticated, "missing request metadata headers")
	}

	keys := md.Get(HeaderAPIKey)
	if len(keys) == 0 {
		metrics.DefaultMetrics.IncRPC(true)
		return nil, status.Error(codes.Unauthenticated, "invalid or missing x-api-key header")
	}

	tenantID, valid := ValidateAPIKey(keys[0])
	if !valid {
		metrics.DefaultMetrics.IncRPC(true)
		return nil, status.Error(codes.Unauthenticated, "invalid or missing x-api-key header")
	}

	authedCtx := ContextWithTenant(ctx, tenantID)
	return handler(authedCtx, req)
}

// StreamAuthInterceptor enforces API key authentication before initializing bidirectional or server streams
func StreamAuthInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	// Skip auth check for health watch
	if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") {
		return handler(srv, ss)
	}

	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		metrics.DefaultMetrics.IncRPC(true)
		return status.Error(codes.Unauthenticated, "missing request metadata headers")
	}

	keys := md.Get(HeaderAPIKey)
	if len(keys) == 0 {
		metrics.DefaultMetrics.IncRPC(true)
		return status.Error(codes.Unauthenticated, "invalid or missing x-api-key header")
	}

	tenantID, valid := ValidateAPIKey(keys[0])
	if !valid {
		metrics.DefaultMetrics.IncRPC(true)
		return status.Error(codes.Unauthenticated, "invalid or missing x-api-key header")
	}

	wrappedStream := &wrappedServerStream{
		ServerStream: ss,
		ctx:          ContextWithTenant(ss.Context(), tenantID),
	}
	return handler(srv, wrappedStream)
}

// UnaryLoggingInterceptor logs method invocation duration and status codes
func UnaryLoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(start)

	statusCode := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			statusCode = st.Code()
		} else {
			statusCode = codes.Unknown
		}
	}

	metrics.DefaultMetrics.IncRPC(err != nil)
	logger.L.Info("grpc_unary_call",
		"method", info.FullMethod,
		"status", statusCode.String(),
		"duration_us", duration.Microseconds(),
	)

	return resp, err
}

// StreamLoggingInterceptor logs stream lifecycle events
func StreamLoggingInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	logger.L.Info("grpc_stream_opened", "method", info.FullMethod)

	err := handler(srv, ss)
	duration := time.Since(start)

	statusCode := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			statusCode = st.Code()
		} else {
			statusCode = codes.Unknown
		}
	}

	metrics.DefaultMetrics.IncRPC(err != nil)
	logger.L.Info("grpc_stream_closed",
		"method", info.FullMethod,
		"status", statusCode.String(),
		"duration_ms", duration.Milliseconds(),
	)

	return err
}
