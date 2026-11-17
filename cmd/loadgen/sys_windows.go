//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

var (
	modwinmm            = syscall.NewLazyDLL("winmm.dll")
	procTimeBeginPeriod = modwinmm.NewProc("timeBeginPeriod")
	procTimeEndPeriod   = modwinmm.NewProc("timeEndPeriod")

	modkernel32        = syscall.NewLazyDLL("kernel32.dll")
	procGetSystemTimes = modkernel32.NewProc("GetSystemTimes")
)

type filetime struct {
	lowDateTime  uint32
	highDateTime uint32
}

type hostCPUSnapshot struct {
	idle   uint64
	kernel uint64
	user   uint64
	valid  bool
}

func getHostCPUSnapshot() hostCPUSnapshot {
	var fIdle, fKernel, fUser filetime
	r1, _, _ := procGetSystemTimes.Call(
		uintptr(unsafe.Pointer(&fIdle)),
		uintptr(unsafe.Pointer(&fKernel)),
		uintptr(unsafe.Pointer(&fUser)),
	)
	if r1 == 0 {
		return hostCPUSnapshot{valid: false}
	}
	idle := (uint64(fIdle.highDateTime) << 32) | uint64(fIdle.lowDateTime)
	kernel := (uint64(fKernel.highDateTime) << 32) | uint64(fKernel.lowDateTime)
	user := (uint64(fUser.highDateTime) << 32) | uint64(fUser.lowDateTime)
	return hostCPUSnapshot{idle: idle, kernel: kernel, user: user, valid: true}
}

func computeHostCPUPercent(start, end hostCPUSnapshot) float64 {
	if !start.valid || !end.valid {
		return 0
	}
	if end.kernel < start.kernel || end.user < start.user || end.idle < start.idle {
		return 0
	}
	kernelDelta := end.kernel - start.kernel
	userDelta := end.user - start.user
	idleDelta := end.idle - start.idle
	total := kernelDelta + userDelta
	if total == 0 || total < idleDelta {
		return 0
	}
	busy := total - idleDelta
	return (float64(busy) / float64(total)) * 100.0
}

func initTimerResolution() func() {
	_, _, _ = procTimeBeginPeriod.Call(1)
	return func() {
		_, _, _ = procTimeEndPeriod.Call(1)
	}
}
