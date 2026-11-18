package tests

import (
	"context"
	"testing"
	"time"

	"github.com/yoohooshantanu/audio-gateway/internal/interceptor"
	"github.com/yoohooshantanu/audio-gateway/internal/pb"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthInterceptorRejections(t *testing.T) {
	h := setupTestHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Unary RPC without any metadata headers -> Must be Unauthenticated
	_, err := h.client.GetCallSummary(ctx, &pb.SummaryRequest{CallId: "any_call"})
	if err == nil {
		t.Fatalf("expected Unauthenticated error for request with no metadata")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Fatalf("expected status Unauthenticated, got: %v", err)
	}

	// 2. Unary RPC with invalid API key -> Must be Unauthenticated
	badAuthCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "invalid_random_token")
	_, err = h.client.GetCallSummary(badAuthCtx, &pb.SummaryRequest{CallId: "any_call"})
	if err == nil {
		t.Fatalf("expected Unauthenticated error for invalid api key")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Fatalf("expected status Unauthenticated, got: %v", err)
	}

	// 3. Unary RPC with attacker prefix "sb_live_attacker_unauthorized" -> Must be Unauthenticated!
	attackerAuthCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", "sb_live_attacker_unauthorized")
	_, err = h.client.GetCallSummary(attackerAuthCtx, &pb.SummaryRequest{CallId: "any_call"})
	if err == nil {
		t.Fatalf("expected Unauthenticated error for unregistered sb_live_ key")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Fatalf("expected status Unauthenticated on attacker sb_live_ prefix, got: %v", err)
	}

	// 4. Bidirectional Stream without API key -> Must be Unauthenticated
	stream, err := h.client.StreamCall(ctx)
	if err == nil {
		_, recvErr := stream.Recv()
		if recvErr == nil {
			t.Fatalf("expected stream to fail due to missing auth")
		}
		if st, ok := status.FromError(recvErr); !ok || st.Code() != codes.Unauthenticated {
			t.Fatalf("expected status Unauthenticated on unauthed stream, got: %v", recvErr)
		}
	} else {
		if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
			t.Fatalf("expected status Unauthenticated on stream dial, got: %v", err)
		}
	}

	// 5. Standard Health Check probe without API key -> Must succeed with SERVING
	healthResp, err := h.health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check probe failed unexpectedly: %v", err)
	}
	if healthResp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected health status SERVING, got: %v", healthResp.Status)
	}
}

func TestProductionRegistryStartsEmpty(t *testing.T) {
	// Verify that interceptor.NewKeyRegistry() contains zero hardcoded default credentials
	reg := interceptor.NewKeyRegistry()

	hardcodedKeys := []string{
		"sb_test_key_123",
		"sb_live_acme_corp_sec99",
		"sb_live_growth_tier_key",
	}

	for _, k := range hardcodedKeys {
		if tenant, found := reg.LookupTenant(k); found {
			t.Fatalf("production registry must not contain hardcoded default credential %q (mapped to %q)", k, tenant)
		}
	}
}
