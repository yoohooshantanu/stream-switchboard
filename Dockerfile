# Multi-stage Docker build for AudioGateway
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy application source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/audio-gateway ./cmd/server

# Distroless runtime image
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /bin/audio-gateway /audio-gateway

USER nonroot:nonroot

EXPOSE 50051 8083

ENTRYPOINT ["/audio-gateway"]
