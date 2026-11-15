# Multi-stage Docker build for StreamBridge
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy application source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/streambridge ./cmd/server

# Distroless runtime image
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /bin/streambridge /streambridge

USER nonroot:nonroot

EXPOSE 50051 8083

ENTRYPOINT ["/streambridge"]
