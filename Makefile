.PHONY: all build test bench clean run docker-build docker-up proto

BINARY_NAME=streambridge
BIN_DIR=bin

all: test build

build:
	@echo "==> Building StreamBridge binary..."
	go build -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/server

test:
	@echo "==> Running hermetic gRPC unit and integration tests..."
	go test -v -race -cover ./tests/...

test-quick:
	@echo "==> Running tests without race detector..."
	go test -v ./tests/...

bench:
	@echo "==> Running gRPC bidirectional throughput benchmark..."
	go test -bench=BenchmarkStreamBridgeThroughput -benchmem -run=^$$ ./tests/...

run: build
	@echo "==> Running StreamBridge on :50051 (gRPC) and :8083 (HTTP metrics)..."
	./$(BIN_DIR)/$(BINARY_NAME)

docker-build:
	docker build -t streambridge:latest .

docker-up:
	docker compose up -d

clean:
	@rm -rf $(BIN_DIR)
