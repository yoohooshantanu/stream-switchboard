package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yoohooshantanu/streambridge/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// MetricSnapshot stores parsed metrics scraped from the server's Prometheus endpoint
type MetricSnapshot struct {
	Timestamp          time.Time
	ActiveStreams      float64
	ActiveWatchers     float64
	AudioPacketsTotal  float64
	AudioBytesTotal    float64
	SequenceGapErrors  float64
	DuplicateDropped   float64
	TombstoneDropped   float64
	EventsDropped      float64
	Goroutines         float64
	RSSBytes           float64
	QueueDepthMax      float64
	QueueDepthAvg      float64
	QueueWaitBuckets   map[float64]uint64
	QueueWaitCount     uint64
	ProcessingBuckets  map[float64]uint64
	ProcessingCount    uint64
}

// StepResult stores all measured and derived metrics for a single concurrency step
type StepResult struct {
	StepIndex          int
	Concurrency        int
	Duration           time.Duration
	OfferedPPS         float64
	AchievedPPS        float64
	SendLateTickCount  int64
	GeneratorValid     bool

	ThroughputPPS      float64
	ThroughputBPS      float64

	QueueWaitP50       float64
	QueueWaitP95       float64
	QueueWaitP99       float64
	QueueWaitBuckets   map[float64]uint64

	ProcessingP50      float64
	ProcessingP95      float64
	ProcessingP99      float64
	ProcessingBuckets  map[float64]uint64

	ClientLatencyP50   float64
	ClientLatencyP95   float64
	ClientLatencyP99   float64

	SequenceGapErrors  float64
	DuplicateDropped   float64
	TombstoneDropped   float64
	EventsDropped      float64

	Goroutines         float64
	RSSBytes           float64
	QueueDepthAvg      float64
	QueueDepthMax      float64

	IsKnee             bool
	KneeReason         string
}

func parsePrometheusMetrics(body string) (*MetricSnapshot, error) {
	snap := &MetricSnapshot{
		Timestamp:         time.Now(),
		QueueWaitBuckets:  make(map[float64]uint64),
		ProcessingBuckets: make(map[float64]uint64),
	}

	queueDepths := []float64{}

	lines := strings.Split(body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		metricKey := parts[0]
		val, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		if strings.HasPrefix(metricKey, "streambridge_active_streams") {
			snap.ActiveStreams = val
		} else if strings.HasPrefix(metricKey, "streambridge_active_watchers") {
			snap.ActiveWatchers = val
		} else if strings.HasPrefix(metricKey, "streambridge_audio_packets_total{direction=\"inbound\"}") {
			snap.AudioPacketsTotal = val
		} else if strings.HasPrefix(metricKey, "streambridge_audio_bytes_total{direction=\"inbound\"}") {
			snap.AudioBytesTotal = val
		} else if strings.HasPrefix(metricKey, "streambridge_sequence_gap_timeouts_total") {
			snap.SequenceGapErrors = val
		} else if strings.HasPrefix(metricKey, "streambridge_duplicate_frames_dropped_total") {
			snap.DuplicateDropped = val
		} else if strings.HasPrefix(metricKey, "streambridge_late_frames_tombstone_dropped_total") {
			snap.TombstoneDropped = val
		} else if strings.HasPrefix(metricKey, "streambridge_events_dropped_total") {
			snap.EventsDropped = val
		} else if strings.HasPrefix(metricKey, "go_goroutines") {
			snap.Goroutines = val
		} else if strings.HasPrefix(metricKey, "process_resident_memory_bytes") {
			snap.RSSBytes = val
		} else if strings.HasPrefix(metricKey, "streambridge_worker_queue_depth{") {
			queueDepths = append(queueDepths, val)
		} else if strings.HasPrefix(metricKey, "streambridge_packet_queue_wait_seconds_bucket{") {
			le := extractLE(metricKey)
			snap.QueueWaitBuckets[le] = uint64(val)
		} else if strings.HasPrefix(metricKey, "streambridge_packet_queue_wait_seconds_count") {
			snap.QueueWaitCount = uint64(val)
		} else if strings.HasPrefix(metricKey, "streambridge_packet_processing_seconds_bucket{") {
			le := extractLE(metricKey)
			snap.ProcessingBuckets[le] = uint64(val)
		} else if strings.HasPrefix(metricKey, "streambridge_packet_processing_seconds_count") {
			snap.ProcessingCount = uint64(val)
		}
	}

	if len(queueDepths) > 0 {
		var sum float64
		maxD := queueDepths[0]
		for _, d := range queueDepths {
			sum += d
			if d > maxD {
				maxD = d
			}
		}
		snap.QueueDepthAvg = sum / float64(len(queueDepths))
		snap.QueueDepthMax = maxD
	}

	return snap, nil
}

func extractLE(s string) float64 {
	idx := strings.Index(s, "le=\"")
	if idx == -1 {
		return math.Inf(1)
	}
	sub := s[idx+4:]
	end := strings.Index(sub, "\"")
	if end == -1 {
		return math.Inf(1)
	}
	strVal := sub[:end]
	if strVal == "+Inf" {
		return math.Inf(1)
	}
	val, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		return math.Inf(1)
	}
	return val
}

func scrapeMetrics(url string) (*MetricSnapshot, error) {
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed scraping %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading metrics body: %w", err)
	}

	return parsePrometheusMetrics(string(body))
}

// computeQuantile calculates quantile (0.50, 0.95, 0.99) from delta histogram buckets via linear interpolation
func computeQuantile(bucketsDelta map[float64]uint64, totalCount uint64, q float64) float64 {
	if totalCount == 0 || len(bucketsDelta) == 0 {
		return 0
	}

	type bucketPair struct {
		upperBound float64
		count      uint64
	}

	sorted := make([]bucketPair, 0, len(bucketsDelta))
	for le, count := range bucketsDelta {
		sorted = append(sorted, bucketPair{upperBound: le, count: count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].upperBound < sorted[j].upperBound
	})

	rank := q * float64(totalCount)
	lowerBound := 0.0
	lowerCount := uint64(0)

	for _, b := range sorted {
		if float64(b.count) >= rank {
			if math.IsInf(b.upperBound, 1) {
				return lowerBound
			}
			countDelta := float64(b.count - lowerCount)
			if countDelta <= 0 {
				return lowerBound
			}
			fraction := (rank - float64(lowerCount)) / countDelta
			return lowerBound + fraction*(b.upperBound-lowerBound)
		}
		if !math.IsInf(b.upperBound, 1) {
			lowerBound = b.upperBound
		}
		lowerCount = b.count
	}

	return lowerBound
}

func computeBucketDeltas(startBuckets, endBuckets map[float64]uint64) (map[float64]uint64, uint64) {
	deltas := make(map[float64]uint64)
	for le, endVal := range endBuckets {
		startVal := startBuckets[le]
		if endVal >= startVal {
			deltas[le] = endVal - startVal
		} else {
			deltas[le] = endVal
		}
	}
	infDelta := deltas[math.Inf(1)]
	return deltas, infDelta
}

var (
	modwinmm            = syscall.NewLazyDLL("winmm.dll")
	procTimeBeginPeriod = modwinmm.NewProc("timeBeginPeriod")
	procTimeEndPeriod   = modwinmm.NewProc("timeEndPeriod")
)

func initTimerResolution() func() {
	if runtime.GOOS == "windows" {
		_, _, _ = procTimeBeginPeriod.Call(1)
		return func() {
			_, _, _ = procTimeEndPeriod.Call(1)
		}
	}
	return func() {}
}

func main() {
	cleanupTimer := initTimerResolution()
	defer cleanupTimer()

	targetAddr := flag.String("target", "127.0.0.1:50051", "Streambridge gRPC target address")
	metricsURL := flag.String("metrics-url", "http://127.0.0.1:8083/metrics", "Prometheus metrics URL")
	pprofURL := flag.String("pprof-url", "http://127.0.0.1:8083/debug/pprof", "pprof profiling base URL")
	apiKey := flag.String("api-key", "sb_loadtest_key", "API key for authentication")
	stepsFlag := flag.String("steps", "50,100,200,400,800", "Comma-separated active concurrent call steps")
	stepDuration := flag.Duration("step-duration", 15*time.Second, "Sustain duration per concurrency step")
	warmupDuration := flag.Duration("warmup-duration", 4*time.Second, "Warmup window to discard at start of each step")
	staggerWindow := flag.Duration("stagger-window", 1500*time.Millisecond, "Ramp duration over which call start times are staggered")
	frameInterval := flag.Duration("frame-interval", 20*time.Millisecond, "Audio chunk pacing interval (default: 20ms = 50 pps per call)")
	numRuns := flag.Int("runs", 3, "Number of repeated test runs to report min-max ranges")
	profileDir := flag.String("profile-dir", "./loadtest_profiles", "Directory to store pprof artifacts at the knee")
	serverCmdFlag := flag.String("server-bin", "./bin/streambridge.exe", "Path to streambridge server binary to launch/restart per run")
	autoRestart := flag.Bool("auto-restart", false, "Automatically start/restart the server binary between runs")
	flag.Parse()

	// Capture go version directly from runtime command
	goVersionOut, err := exec.Command("go", "version").Output()
	if err != nil {
		fmt.Printf("Warning: failed running 'go version': %v\n", err)
	}
	goVersionStr := strings.TrimSpace(string(goVersionOut))

	stepStrList := strings.Split(*stepsFlag, ",")
	steps := make([]int, 0, len(stepStrList))
	for _, s := range stepStrList {
		val, err := strconv.Atoi(strings.TrimSpace(s))
		if err == nil && val > 0 {
			steps = append(steps, val)
		}
	}

	_ = os.MkdirAll(*profileDir, 0755)

	fmt.Println("==========================================================================================")
	fmt.Printf(" STREAMBRIDGE HIGH-CONCURRENCY REAL-TIME VOIP LOAD TEST HARNESS\n")
	fmt.Println("==========================================================================================")
	fmt.Printf(" Target gRPC:         %s\n", *targetAddr)
	fmt.Printf(" Metrics URL:         %s\n", *metricsURL)
	fmt.Printf(" Concurrency Steps:   %v\n", steps)
	fmt.Printf(" Step Duration:       %v (warmup: %v, stagger: %v)\n", *stepDuration, *warmupDuration, *staggerWindow)
	fmt.Printf(" Frame Cadence:       %v (target rate: %.1f pps/call)\n", *frameInterval, float64(time.Second)/float64(*frameInterval))
	fmt.Printf(" Repeated Runs:       %d\n", *numRuns)
	fmt.Printf(" Runtime Toolchain:   %s\n", goVersionStr)
	fmt.Println("==========================================================================================")

	allRunsResults := make([][]StepResult, *numRuns)

	for run := 1; run <= *numRuns; run++ {
		fmt.Printf("\n>>> INITIATING TEST RUN %d OF %d <<<\n", run, *numRuns)

		var srvProc *exec.Cmd
		var srvLog *os.File
		if *autoRestart {
			srvProc, srvLog = startServerProcess(*serverCmdFlag)
			time.Sleep(2 * time.Second)
		}

		// Ensure server is up and active streams == 0
		waitForServerDrain(*metricsURL, 15*time.Second)

		runResults := make([]StepResult, 0, len(steps))

		for idx, concurrency := range steps {
			fmt.Printf("  -> Executing Step %d/%d: %d concurrent streams...\n", idx+1, len(steps), concurrency)

			res, kneeProfileTaken := executeStep(
				*targetAddr,
				*metricsURL,
				*pprofURL,
				*apiKey,
				concurrency,
				*stepDuration,
				*warmupDuration,
				*staggerWindow,
				*frameInterval,
				*profileDir,
				idx+1,
				run,
			)

			runResults = append(runResults, res)
			if kneeProfileTaken {
				fmt.Printf("     [!] KNEE CRITERIA TRIGGERED at %d concurrent calls: %s\n", concurrency, res.KneeReason)
			}

			// Wait for active streams to drain to 0 between steps
			waitForServerDrain(*metricsURL, 20*time.Second)
		}

		allRunsResults[run-1] = runResults

		if *autoRestart && srvProc != nil && srvProc.Process != nil {
			_ = srvProc.Process.Kill()
			_, _ = srvProc.Process.Wait()
			if srvLog != nil {
				_ = srvLog.Close()
			}
			time.Sleep(1 * time.Second)
		}
	}

	// Print Summary Table with [min-max] ranges across runs
	printSummaryTable(steps, allRunsResults, goVersionStr)
	printRawBucketAppendix(steps, allRunsResults)
}

func printRawBucketAppendix(steps []int, allRuns [][]StepResult) {
	fmt.Println("\n========================================================================================================================")
	fmt.Printf(" RESULTS APPENDIX: RAW QUEUE-WAIT & PROCESSING HISTOGRAM BUCKET COUNTS PER STEP (RUN 1)\n")
	fmt.Println("========================================================================================================================")
	if len(allRuns) == 0 {
		return
	}
	run1 := allRuns[0]
	for sIdx, step := range run1 {
		fmt.Printf("\n--- STEP %d: %d CONCURRENT CALLS ---\n", sIdx+1, step.Concurrency)
		fmt.Printf("Queue Wait Raw Buckets (Cumulative Counts):\n")
		var sortedQW []float64
		for le := range step.QueueWaitBuckets {
			sortedQW = append(sortedQW, le)
		}
		sort.Float64s(sortedQW)
		for _, le := range sortedQW {
			leStr := fmt.Sprintf("%.4fs", le)
			if math.IsInf(le, 1) {
				leStr = "+Inf"
			}
			fmt.Printf("  le <= %-8s : %d\n", leStr, step.QueueWaitBuckets[le])
		}

		fmt.Printf("Processing Raw Buckets (Cumulative Counts):\n")
		var sortedProc []float64
		for le := range step.ProcessingBuckets {
			sortedProc = append(sortedProc, le)
		}
		sort.Float64s(sortedProc)
		for _, le := range sortedProc {
			leStr := fmt.Sprintf("%.4fs", le)
			if math.IsInf(le, 1) {
				leStr = "+Inf"
			}
			fmt.Printf("  le <= %-8s : %d\n", leStr, step.ProcessingBuckets[le])
		}
	}
	fmt.Println("========================================================================================================================")
}

func startServerProcess(binPath string) (*exec.Cmd, *os.File) {
	absPath, err := filepath.Abs(binPath)
	if err != nil {
		absPath = binPath
	}
	cmd := exec.Command(absPath)
	cmd.Env = append(os.Environ(),
		"GRPC_PORT=50051",
		"HTTP_PORT=8083",
		"HTTP_HOST=127.0.0.1",
		"PPROF_ENABLED=1",
		"STREAMBRIDGE_API_KEY=sb_loadtest_key",
	)
	logFile, err := os.OpenFile("streambridge_server.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err == nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		fmt.Printf("Failed starting server binary at %s: %v\n", absPath, err)
		return nil, nil
	}
	return cmd, logFile
}

func waitForServerDrain(metricsURL string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snap, err := scrapeMetrics(metricsURL)
		if err == nil && snap.ActiveStreams == 0 {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Printf("     [!] Server not ready at %s within %v\n", metricsURL, timeout)
	return false
}

func executeStep(
	targetAddr, metricsURL, pprofURL, apiKey string,
	concurrency int,
	totalDuration, warmupDuration, staggerWindow, frameInterval time.Duration,
	profileDir string,
	stepIdx, runIdx int,
) (StepResult, bool) {
	result := StepResult{
		StepIndex:   stepIdx,
		Concurrency: concurrency,
		Duration:    totalDuration,
		OfferedPPS:  float64(concurrency) * (float64(time.Second) / float64(frameInterval)),
	}

	// Baseline snapshot before step starts
	preSnap, err := scrapeMetrics(metricsURL)
	if err != nil {
		fmt.Printf("     [!] Error scraping baseline metrics: %v\n", err)
		return result, false
	}

	var totalSentPackets int64
	var totalLateTicks int64
	var clientLatencies []float64
	var clientLatMu sync.Mutex

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Warmup timer: after warmupDuration, record intermediate metric baseline
	warmupDoneCh := make(chan struct{})
	var warmupSnap *MetricSnapshot
	go func() {
		time.Sleep(warmupDuration)
		ws, _ := scrapeMetrics(metricsURL)
		warmupSnap = ws
		close(warmupDoneCh)
	}()

	staggerInterval := time.Duration(0)
	if concurrency > 1 && staggerWindow > 0 {
		staggerInterval = staggerWindow / time.Duration(concurrency)
	}

	// Dial shared connection pool (or per-worker connections)
	// Using a bounded connection pool avoids single TCP socket bottlenecks
	numConns := 8
	if concurrency < numConns {
		numConns = concurrency
	}
	conns := make([]*grpc.ClientConn, numConns)
	for i := 0; i < numConns; i++ {
		c, err := grpc.NewClient(targetAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16*1024*1024), grpc.MaxCallSendMsgSize(16*1024*1024)),
		)
		if err != nil {
			fmt.Printf("     [!] Failed dialing gRPC target: %v\n", err)
			return result, false
		}
		conns[i] = c
		defer c.Close()
	}

	// Launch N concurrent streaming sessions
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		conn := conns[i%numConns]
		callID := fmt.Sprintf("load_r%d_s%d_call_%04d", runIdx, stepIdx, i+1)
		startDelay := time.Duration(i) * staggerInterval

		go func(cID string, delay time.Duration) {
			defer wg.Done()
			select {
			case <-time.After(delay):
			case <-stopCh:
				return
			}

			client := pb.NewStreamBridgeClient(conn)
			ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", apiKey)
			stream, err := client.StreamCall(ctx)
			if err != nil {
				fmt.Printf("     [!] Call %s StreamCall error: %v\n", cID, err)
				return
			}

			// Background receiver for client-side round-trip latency & response draining
			sendTimestamps := sync.Map{}
			recvDone := make(chan struct{})
			go func() {
				defer close(recvDone)
				for {
					event, err := stream.Recv()
					if err != nil {
						return
					}
					if event != nil && event.Type == pb.EventType_EVENT_TRANSCRIPT_PARTIAL {
						idx := strings.Index(event.Payload, "Speaker audio frame ")
						if idx != -1 {
							var frameSeq uint64
							_, _ = fmt.Sscanf(event.Payload[idx+len("Speaker audio frame "):], "%d acknowledged", &frameSeq)
							if frameSeq > 0 {
								if sentAtVal, ok := sendTimestamps.Load(frameSeq); ok {
									sentAt := sentAtVal.(time.Time)
									rtt := time.Since(sentAt).Seconds()
									clientLatMu.Lock()
									clientLatencies = append(clientLatencies, rtt)
									clientLatMu.Unlock()
									sendTimestamps.Delete(frameSeq)
								}
							}
						}
					}
				}
			}()

			// Transmission loop paced at frameInterval (strict drift-compensated timing)
			seq := uint64(1)
			ticker := time.NewTicker(frameInterval)
			defer ticker.Stop()

			pcmPayload := make([]byte, 320) // 20ms of 16-bit 8kHz PCM

			scheduledTime := time.Now().Add(frameInterval)
			for {
				select {
				case <-stopCh:
					// Send final chunk and close
					_ = stream.Send(&pb.AudioChunk{
						CallId:   cID,
						TenantId: "tenant_production",
						Sequence: seq,
						Data:     pcmPayload,
						IsFinal:  true,
					})
					_ = stream.CloseSend()
					<-recvDone
					return

				case tickTime := <-ticker.C:
					// Generator self-check: check tick lateness
					tickDelay := tickTime.Sub(scheduledTime)
					if tickDelay > 5*time.Millisecond {
						atomic.AddInt64(&totalLateTicks, 1)
						// No silent catch-up bursts: do NOT burst; advance scheduledTime
						scheduledTime = tickTime.Add(frameInterval)
					} else {
						scheduledTime = scheduledTime.Add(frameInterval)
					}

					now := time.Now()
					sendTimestamps.Store(seq, now)

					chunk := &pb.AudioChunk{
						CallId:    cID,
						TenantId:  "tenant_production",
						Sequence:  seq,
						Data:      pcmPayload,
						IsFinal:   false,
						Timestamp: now.UnixMilli(),
					}

					if err := stream.Send(chunk); err != nil {
						fmt.Printf("     [!] Call %s Send error: %v\n", cID, err)
						_ = stream.CloseSend()
						<-recvDone
						return
					}

					atomic.AddInt64(&totalSentPackets, 1)
					seq++
				}
			}
		}(callID, startDelay)
	}

	// Mid-step profiling trigger: capture pprof during active traffic window if high concurrency
	var pprofWg sync.WaitGroup
	var profileCaptured atomic.Bool
	if pprofURL != "" && profileDir != "" && concurrency >= 200 {
		pprofWg.Add(1)
		go func() {
			defer pprofWg.Done()
			select {
			case <-time.After(warmupDuration + 1*time.Second):
			case <-stopCh:
				return
			}
			ok := capturePprofProfiles(pprofURL, profileDir, concurrency, stepIdx, runIdx)
			profileCaptured.Store(ok)
		}()
	}

	// Sustain step for totalDuration
	time.Sleep(totalDuration)
	close(stopCh)
	wg.Wait()
	pprofWg.Wait()

	// Post-step metrics snapshot
	postSnap, err := scrapeMetrics(metricsURL)
	if err != nil {
		fmt.Printf("     [!] Error scraping post metrics: %v\n", err)
		return result, false
	}

	// Use delta from warmupSnap if available, otherwise from preSnap
	baseSnap := preSnap
	activeDuration := totalDuration
	if warmupSnap != nil {
		baseSnap = warmupSnap
		activeDuration = totalDuration - warmupDuration
	}

	deltaPackets := postSnap.AudioPacketsTotal - baseSnap.AudioPacketsTotal
	deltaBytes := postSnap.AudioBytesTotal - baseSnap.AudioBytesTotal
	sec := activeDuration.Seconds()
	if sec <= 0 {
		sec = totalDuration.Seconds()
	}

	result.ThroughputPPS = deltaPackets / sec
	result.ThroughputBPS = deltaBytes / sec
	result.SendLateTickCount = totalLateTicks
	result.AchievedPPS = float64(totalSentPackets) / totalDuration.Seconds()

	// Generator validation: invalid if late ticks exceed 2% of total packets sent
	if totalSentPackets > 0 && float64(totalLateTicks)/float64(totalSentPackets) > 0.02 {
		result.GeneratorValid = false
	} else {
		result.GeneratorValid = true
	}

	// Compute Queue Wait quantiles from Prometheus histogram delta
	qwDelta, qwCount := computeBucketDeltas(baseSnap.QueueWaitBuckets, postSnap.QueueWaitBuckets)
	result.QueueWaitP50 = computeQuantile(qwDelta, qwCount, 0.50)
	result.QueueWaitP95 = computeQuantile(qwDelta, qwCount, 0.95)
	result.QueueWaitP99 = computeQuantile(qwDelta, qwCount, 0.99)
	result.QueueWaitBuckets = qwDelta

	// Compute Processing quantiles from Prometheus histogram delta
	procDelta, procCount := computeBucketDeltas(baseSnap.ProcessingBuckets, postSnap.ProcessingBuckets)
	result.ProcessingP50 = computeQuantile(procDelta, procCount, 0.50)
	result.ProcessingP95 = computeQuantile(procDelta, procCount, 0.95)
	result.ProcessingP99 = computeQuantile(procDelta, procCount, 0.99)
	result.ProcessingBuckets = procDelta

	// Compute Client-side Roundtrip Latencies
	if len(clientLatencies) > 0 {
		sort.Float64s(clientLatencies)
		n := len(clientLatencies)
		result.ClientLatencyP50 = clientLatencies[int(float64(n)*0.50)]
		result.ClientLatencyP95 = clientLatencies[int(float64(n)*0.95)]
		result.ClientLatencyP99 = clientLatencies[int(float64(n)*0.99)]
	}

	result.SequenceGapErrors = postSnap.SequenceGapErrors - baseSnap.SequenceGapErrors
	result.DuplicateDropped = postSnap.DuplicateDropped - baseSnap.DuplicateDropped
	result.TombstoneDropped = postSnap.TombstoneDropped - baseSnap.TombstoneDropped
	result.EventsDropped = postSnap.EventsDropped - baseSnap.EventsDropped

	result.Goroutines = postSnap.Goroutines
	result.RSSBytes = postSnap.RSSBytes
	result.QueueDepthAvg = postSnap.QueueDepthAvg
	result.QueueDepthMax = postSnap.QueueDepthMax

	// Evaluate Knee Criteria:
	// 1. Achieved throughput < 95% of offered rate (concurrency * 50 pps)
	// 2. p99 queue wait > 10ms (half the 20ms frame interval)
	// 3. Worker queue depth growing throughout step (max queue depth > 100)
	isKnee := false
	var reasons []string
	if result.ThroughputPPS < 0.95*result.OfferedPPS {
		isKnee = true
		reasons = append(reasons, fmt.Sprintf("achieved pps (%.1f) < 95%% offered (%.1f)", result.ThroughputPPS, result.OfferedPPS))
	}
	if result.QueueWaitP99 > 0.010 { // 10ms
		isKnee = true
		reasons = append(reasons, fmt.Sprintf("p99 queue wait (%.2fms) > 10ms", result.QueueWaitP99*1000))
	}
	if result.QueueDepthMax > 100 {
		isKnee = true
		reasons = append(reasons, fmt.Sprintf("worker queue backlog growing (depth=%0.0f)", result.QueueDepthMax))
	}

	result.IsKnee = isKnee
	result.KneeReason = strings.Join(reasons, "; ")

	// If knee detected, report profile taken
	kneeProfileTaken := profileCaptured.Load()
	if isKnee && !kneeProfileTaken {
		kneeProfileTaken = capturePprofProfiles(pprofURL, profileDir, concurrency, stepIdx, runIdx)
	}

	return result, kneeProfileTaken
}

func capturePprofProfiles(pprofURL, profileDir string, concurrency, stepIdx, runIdx int) bool {
	_ = os.MkdirAll(profileDir, 0755)
	cpuFile := filepath.Join(profileDir, fmt.Sprintf("cpu_knee_%dcalls_r%d.pprof", concurrency, runIdx))
	heapFile := filepath.Join(profileDir, fmt.Sprintf("heap_knee_%dcalls_r%d.pprof", concurrency, runIdx))

	client := http.Client{Timeout: 15 * time.Second}

	// 1. Capture CPU profile (5 seconds)
	resp, err := client.Get(pprofURL + "/profile?seconds=5")
	if err == nil && resp.StatusCode == http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		_ = os.WriteFile(cpuFile, data, 0644)
		resp.Body.Close()
	} else if resp != nil {
		resp.Body.Close()
	}

	// 2. Capture Heap profile
	respHeap, errHeap := client.Get(pprofURL + "/heap")
	if errHeap == nil && respHeap.StatusCode == http.StatusOK {
		dataHeap, _ := io.ReadAll(respHeap.Body)
		_ = os.WriteFile(heapFile, dataHeap, 0644)
		respHeap.Body.Close()
	} else if respHeap != nil {
		respHeap.Body.Close()
	}

	return true
}

func printSummaryTable(steps []int, allRuns [][]StepResult, goVersion string) {
	fmt.Println("\n========================================================================================================================")
	fmt.Printf(" STREAMBRIDGE LOAD TEST RESULTS - 3-RUN [MIN - MAX] AGGREGATED SUMMARY\n")
	fmt.Printf(" Toolchain: %s\n", goVersion)
	fmt.Println("========================================================================================================================")
	fmt.Printf("| %-7s | %-15s | %-16s | %-16s | %-16s | %-10s | %-12s | %-10s |\n",
		"Calls", "Throughput (pps)", "Queue Wait (p99)", "Proc Time (p99)", "Client RTT (p99)", "Errors", "RSS Memory", "Goroutines")
	fmt.Println("|---------|-----------------|------------------|------------------|------------------|------------|--------------|------------|")

	for sIdx, c := range steps {
		var minPPS, maxPPS float64 = math.MaxFloat64, 0
		var minQW99, maxQW99 float64 = math.MaxFloat64, 0
		var minProc99, maxProc99 float64 = math.MaxFloat64, 0
		var minRTT99, maxRTT99 float64 = math.MaxFloat64, 0
		var minErr, maxErr float64 = math.MaxFloat64, 0
		var minRSS, maxRSS float64 = math.MaxFloat64, 0
		var minGoro, maxGoro float64 = math.MaxFloat64, 0

		allValid := true
		anyKnee := false

		for _, run := range allRuns {
			if sIdx >= len(run) {
				continue
			}
			r := run[sIdx]
			if !r.GeneratorValid {
				allValid = false
			}
			if r.IsKnee {
				anyKnee = true
			}

			if r.ThroughputPPS < minPPS {
				minPPS = r.ThroughputPPS
			}
			if r.ThroughputPPS > maxPPS {
				maxPPS = r.ThroughputPPS
			}

			qwMs := r.QueueWaitP99 * 1000
			if qwMs < minQW99 {
				minQW99 = qwMs
			}
			if qwMs > maxQW99 {
				maxQW99 = qwMs
			}

			procMs := r.ProcessingP99 * 1000
			if procMs < minProc99 {
				minProc99 = procMs
			}
			if procMs > maxProc99 {
				maxProc99 = procMs
			}

			rttMs := r.ClientLatencyP99 * 1000
			if rttMs < minRTT99 {
				minRTT99 = rttMs
			}
			if rttMs > maxRTT99 {
				maxRTT99 = rttMs
			}

			errs := r.SequenceGapErrors + r.DuplicateDropped
			if errs < minErr {
				minErr = errs
			}
			if errs > maxErr {
				maxErr = errs
			}

			rssMb := r.RSSBytes / (1024 * 1024)
			if rssMb < minRSS {
				minRSS = rssMb
			}
			if rssMb > maxRSS {
				maxRSS = rssMb
			}

			if r.Goroutines < minGoro {
				minGoro = r.Goroutines
			}
			if r.Goroutines > maxGoro {
				maxGoro = r.Goroutines
			}
		}

		kneeMarker := ""
		if anyKnee {
			kneeMarker = " *"
		}
		if !allValid {
			kneeMarker += " (generator throttled)"
		}

		fmt.Printf("| %-7d | %7.0f - %-7.0f | %6.2fms - %-6.2fms | %6.2fms - %-6.2fms | %6.2fms - %-6.2fms | %4.0f - %-4.0f | %5.1fMB - %-5.1fMB | %4.0f - %-4.0f |%s\n",
			c, minPPS, maxPPS, minQW99, maxQW99, minProc99, maxProc99, minRTT99, maxRTT99, minErr, maxErr, minRSS, maxRSS, minGoro, maxGoro, kneeMarker)
	}
	fmt.Println("========================================================================================================================")
	fmt.Println(" * Indicates knee of saturation curve detected.")
}
