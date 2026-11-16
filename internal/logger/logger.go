package logger

import (
	"io"
	"log/slog"
	"os"
)

var L *slog.Logger

func init() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	L = slog.New(handler)
}

// SetDiscard silences all logger output (used during benchmarks and high-volume performance tests)
func SetDiscard() {
	L = slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ResetStdout restores stdout JSON logging
func ResetStdout() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	L = slog.New(handler)
}
