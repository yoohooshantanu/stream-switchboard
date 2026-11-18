.PHONY: all build test bench clean run docker-build docker-up proto

BINARY_NAME=audio-gateway
BIN_DIR=bin

all: test build

build:
	@echo "==> Building AudioGateway binary..."
	go build -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/server

test:
	@echo "==> Running hermetic gRPC unit and integration tests..."
	go test -v -race -cover ./tests/...

test-quick:
	@echo "==> Running tests without race detector..."
	go test -v ./tests/...

bench:
	@echo "==> Running gRPC bidirectional throughput benchmark..."
	go test -bench=BenchmarkAudioGatewayThroughput -benchmem -run=^$$ ./tests/...

run: build
	@echo "==> Running AudioGateway on :50051 (gRPC) and :8083 (HTTP metrics)..."
	./$(BIN_DIR)/$(BINARY_NAME)

docker-build:
	docker build -t audio-gateway:latest .

docker-up:
	docker compose up -d

clean:
	@rm -rf $(BIN_DIR)
