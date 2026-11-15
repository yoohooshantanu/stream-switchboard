package health

import (
	"context"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// HealthServer implements standard grpc_health_v1 protocol
type HealthServer struct {
	healthpb.UnimplementedHealthServer
}

func NewHealthServer() *HealthServer {
	return &HealthServer{}
}

func (h *HealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}, nil
}

func (h *HealthServer) Watch(req *healthpb.HealthCheckRequest, server healthpb.Health_WatchServer) error {
	// Send initial SERVING status
	if err := server.Send(&healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}); err != nil {
		return err
	}
	<-server.Context().Done()
	return status.FromContextError(server.Context().Err()).Err()
}
