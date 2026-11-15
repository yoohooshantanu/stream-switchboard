package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/health"
	"github.com/yoohooshantanu/streambridge/internal/interceptor"
	"github.com/yoohooshantanu/streambridge/internal/logger"
	"github.com/yoohooshantanu/streambridge/internal/metrics"
	"github.com/yoohooshantanu/streambridge/internal/pb"
	"github.com/yoohooshantanu/streambridge/internal/router"
	"github.com/yoohooshantanu/streambridge/internal/server"
	"github.com/yoohooshantanu/streambridge/internal/worker"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	grpcPort := getEnv("GRPC_PORT", "50051")
	httpPort := getEnv("HTTP_PORT", "8083")

	logger.L.Info("starting_streambridge", "grpc_port", grpcPort, "http_port", httpPort)

	// 1. Initialize Event Router & AI Worker Pool
	eventRouter := router.NewEventRouter()
	workerPool := worker.NewAIWorkerPool(eventRouter, 4, 2048)
	workerPool.Start()

	// 2. Configure gRPC Server with Interceptor Chains
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryAuthInterceptor,
			interceptor.UnaryLoggingInterceptor,
		),
		grpc.ChainStreamInterceptor(
			interceptor.StreamAuthInterceptor,
			interceptor.StreamLoggingInterceptor,
		),
	)

	// Register Core StreamBridge and Standard Health Service
	bridgeServer := server.NewStreamBridgeServer(eventRouter, workerPool)
	pb.RegisterStreamBridgeServer(grpcServer, bridgeServer)

	healthServer := health.NewHealthServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// 3. Start gRPC Listener
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.L.Error("grpc_listen_failed", "error", err)
		os.Exit(1)
	}

	go func() {
		logger.L.Info("grpc_server_listening", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			logger.L.Error("grpc_server_error", "error", err)
		}
	}()

	// 4. Configure HTTP Sidecar for Metrics, Healthz & pprof
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "SERVING",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	httpMux.HandleFunc("/metrics", metrics.DefaultMetrics.Handler())

	// pprof
	httpMux.HandleFunc("/debug/pprof/", pprof.Index)
	httpMux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	httpServer := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      httpMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		logger.L.Info("http_sidecar_listening", "port", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("http_sidecar_error", "error", err)
		}
	}()

	// 5. Graceful Shutdown Coordinator
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_streambridge")

	// Gracefully drain active gRPC streams and RPCs
	grpcServer.GracefulStop()

	// Drain worker queue and stop background pool
	workerPool.Stop()
	eventRouter.Close()

	// Shutdown HTTP sidecar
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)

	logger.L.Info("streambridge_shutdown_complete")
}
