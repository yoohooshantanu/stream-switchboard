package interceptor

import (
	"context"
	"strings"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const HeaderAPIKey = "x-api-key"

// ValidateAPIKey checks if the incoming API key meets security requirements
func ValidateAPIKey(key string) bool {
	if key == "" {
		return false
	}
	// Accept standard production format sb_live_* or valid development key
	if strings.HasPrefix(key, "sb_live_") || key == "sb_test_key_123" {
		return true
	}
	return false
}

// UnaryAuthInterceptor verifies metadata headers on unary RPCs
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
	if len(keys) == 0 || !ValidateAPIKey(keys[0]) {
		metrics.DefaultMetrics.IncRPC(true)
		return nil, status.Error(codes.Unauthenticated, "invalid or missing x-api-key header")
	}

	return handler(ctx, req)
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
	if len(keys) == 0 || !ValidateAPIKey(keys[0]) {
		metrics.DefaultMetrics.IncRPC(true)
		return status.Error(codes.Unauthenticated, "invalid or missing x-api-key header")
	}

	return handler(srv, ss)
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
