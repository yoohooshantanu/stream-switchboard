//go:build !windows

package main

type hostCPUSnapshot struct {
	idle   uint64
	kernel uint64
	user   uint64
	valid  bool
}

func getHostCPUSnapshot() hostCPUSnapshot {
	return hostCPUSnapshot{valid: false}
}

func computeHostCPUPercent(start, end hostCPUSnapshot) float64 {
	return 0
}

func initTimerResolution() func() {
	return func() {}
}
