package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/yoohooshantanu/audio-gateway/internal/health"
	"github.com/yoohooshantanu/audio-gateway/internal/interceptor"
	"github.com/yoohooshantanu/audio-gateway/internal/logger"
	"github.com/yoohooshantanu/audio-gateway/internal/metrics"
	"github.com/yoohooshantanu/audio-gateway/internal/pb"
	"github.com/yoohooshantanu/audio-gateway/internal/router"
	"github.com/yoohooshantanu/audio-gateway/internal/server"
	"github.com/yoohooshantanu/audio-gateway/internal/worker"
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
	flagWorkers := flag.Int("workers", 0, "Number of AI worker goroutines")
	flag.Parse()

	grpcPort := getEnv("GRPC_PORT", "50051")
	httpPort := getEnv("HTTP_PORT", "8083")

	workerCount := 4
	if *flagWorkers > 0 {
		workerCount = *flagWorkers
	} else if envW := os.Getenv("WORKER_POOL_SIZE"); envW != "" {
		if n, err := strconv.Atoi(envW); err == nil && n > 0 {
			workerCount = n
		}
	}

	logger.L.Info("starting_audio-gateway", "grpc_port", grpcPort, "http_port", httpPort, "workers", workerCount)

	// 1. Initialize Event Router & AI Worker Pool
	eventRouter := router.NewEventRouter()
	workerPool := worker.NewAIWorkerPool(eventRouter, workerCount, 2048)
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

	// Register Core AudioGateway and Standard Health Service
	bridgeServer := server.NewAudioGatewayServer(eventRouter, workerPool)
	pb.RegisterAudioGatewayServer(grpcServer, bridgeServer)

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

	// pprof: registered strictly when PPROF_ENABLED is set, bound to localhost
	if os.Getenv("PPROF_ENABLED") == "true" || os.Getenv("PPROF_ENABLED") == "1" {
		runtime.SetBlockProfileRate(10000) // sample goroutine blocking events > 10µs
		runtime.SetMutexProfileFraction(5) // sample 1 in 5 mutex contention events
		httpMux.HandleFunc("/debug/pprof/", pprof.Index)
		httpMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		httpMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		httpMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		httpMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		logger.L.Info("pprof_endpoints_enabled", "binding", "127.0.0.1", "block_rate_ns", 10000, "mutex_fraction", 5)
	}

	httpBind := getEnv("HTTP_HOST", "127.0.0.1")
	httpServer := &http.Server{
		Addr:         net.JoinHostPort(httpBind, httpPort),
		Handler:      httpMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
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

	logger.L.Info("shutting_down_audio-gateway")

	// Gracefully drain active gRPC streams with fallback timeout
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.L.Info("grpc_server_stopped_gracefully")
	case <-time.After(5 * time.Second):
		logger.L.Warn("grpc_server_forced_stop_on_timeout")
		grpcServer.Stop()
	}

	// Drain worker queue and stop background pool
	workerPool.Stop()
	eventRouter.Close()

	// Shutdown HTTP sidecar
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)

	logger.L.Info("audio_gateway_shutdown_complete")
}
