package test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ocfl-archive/gocfl-grpc/pkg/client"
	"github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/bootstrap"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/stretchr/testify/require"
)

// loadMetrics collects latency and success/error statistics for a load test run.
type loadMetrics struct {
	mu           sync.Mutex
	durations    []time.Duration
	successCount int64
	errorCount   int64
	startTime    time.Time
	endTime      time.Time
}

func newLoadMetrics() *loadMetrics {
	return &loadMetrics{
		startTime: time.Now(),
		durations: make([]time.Duration, 0, 1000),
	}
}

func (m *loadMetrics) record(d time.Duration, success bool) {
	m.mu.Lock()
	m.durations = append(m.durations, d)
	m.mu.Unlock()

	if success {
		atomic.AddInt64(&m.successCount, 1)
	} else {
		atomic.AddInt64(&m.errorCount, 1)
	}
}

func (m *loadMetrics) finish() {
	m.endTime = time.Now()
}

func (m *loadMetrics) report(t *testing.T, title string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	total := len(m.durations)
	if total == 0 {
		t.Logf("[%s] No requests recorded.", title)
		return
	}

	totalDuration := m.endTime.Sub(m.startTime)
	if totalDuration <= 0 {
		totalDuration = time.Millisecond
	}

	sort.Slice(m.durations, func(i, j int) bool {
		return m.durations[i] < m.durations[j]
	})

	var sum time.Duration
	for _, d := range m.durations {
		sum += d
	}
	avg := sum / time.Duration(total)
	min := m.durations[0]
	max := m.durations[total-1]
	p50 := m.durations[total*50/100]
	p90 := m.durations[total*90/100]
	p95 := m.durations[total*95/100]
	p99 := m.durations[total*99/100]
	rps := float64(total) / totalDuration.Seconds()

	t.Logf("\n==================== LOAD TEST RESULTS: %s ====================", title)
	t.Logf("Total Requests:   %d", total)
	t.Logf("Successful:       %d (%.2f%%)", m.successCount, float64(m.successCount)/float64(total)*100)
	t.Logf("Errors:           %d (%.2f%%)", m.errorCount, float64(m.errorCount)/float64(total)*100)
	t.Logf("Total Time:       %v", totalDuration)
	t.Logf("Throughput:       %.2f req/s", rps)
	t.Logf("Latency Min:      %v", min)
	t.Logf("Latency Avg:      %v", avg)
	t.Logf("Latency Median:   %v", p50)
	t.Logf("Latency P90:      %v", p90)
	t.Logf("Latency P95:      %v", p95)
	t.Logf("Latency P99:      %v", p99)
	t.Logf("Latency Max:      %v", max)
	t.Logf("========================================================================\n")
}

// TestLoadHighConcurrency performs high concurrent load testing against a single running gRPC server.
func TestLoadHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high concurrency load test in short mode")
	}

	// 1. Start gRPC server ONCE
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	// 2. Connect client
	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "load_test_root"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Initialize Storage Root
	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	require.NoError(t, err)
	require.True(t, initResp.GetSuccess())

	// Create test source payload
	srcDir := filepath.Join(tempDir, "payload_src")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "data.txt"), []byte("Load test content payload with some bytes for checksumming"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "meta.json"), []byte(`{"service": "gocfl-grpc", "benchmark": true}`), 0644))

	// Test 1: Ingestion Load Test (Concurrent Add requests)
	t.Run("ConcurrentIngestLoad", func(t *testing.T) {
		const totalRequests = 100
		const concurrency = 10

		metrics := newLoadMetrics()
		workChan := make(chan int, totalRequests)
		for i := 0; i < totalRequests; i++ {
			workChan <- i
		}
		close(workChan)

		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for id := range workChan {
					objID := fmt.Sprintf("urn:load:object:%d", id)
					start := time.Now()

					resp, err := cl.Add(ctx, &pb.AddRequest{
						OcflPath: ocflRoot,
						ObjectId: objID,
						SrcPath:  filepath.ToSlash(srcDir),
						Message:  fmt.Sprintf("Load test add item %d", id),
						User: &pb.User{
							Name:    "Load Tester",
							Address: "mailto:loadtester@example.com",
						},
					})

					duration := time.Since(start)
					success := err == nil && resp != nil && resp.GetSuccess()
					metrics.record(duration, success)
				}
			}(w)
		}

		wg.Wait()
		metrics.finish()
		metrics.report(t, "Concurrent Add (Ingest)")

		require.Equal(t, int64(totalRequests), metrics.successCount, "All concurrent Add requests should succeed")
		require.Equal(t, int64(0), metrics.errorCount, "No Add errors expected")
	})

	// Test 2: Validation Load Test (Concurrent Validate requests on previously ingested objects)
	t.Run("ConcurrentValidateLoad", func(t *testing.T) {
		const totalRequests = 200
		const concurrency = 20

		metrics := newLoadMetrics()
		workChan := make(chan int, totalRequests)
		for i := 0; i < totalRequests; i++ {
			workChan <- i
		}
		close(workChan)

		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for id := range workChan {
					// Validate either specific objects or root
					targetObjID := fmt.Sprintf("urn:load:object:%d", id%100)
					start := time.Now()

					valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
						OcflPath: ocflRoot,
						ObjectId: targetObjID,
					})

					duration := time.Since(start)
					success := err == nil && valResp != nil && valResp.GetIsValid()
					metrics.record(duration, success)
				}
			}(w)
		}

		wg.Wait()
		metrics.finish()
		metrics.report(t, "Concurrent Validate")

		require.Equal(t, int64(totalRequests), metrics.successCount, "All concurrent Validate requests should succeed")
		require.Equal(t, int64(0), metrics.errorCount, "No Validate errors expected")
	})

	// Test 3: Concurrent Update Load Test
	t.Run("ConcurrentUpdateLoad", func(t *testing.T) {
		const numObjects = 20
		metrics := newLoadMetrics()

		var wg sync.WaitGroup
		for i := 0; i < numObjects; i++ {
			wg.Add(1)
			go func(objIndex int) {
				defer wg.Done()
				objID := fmt.Sprintf("urn:load:object:%d", objIndex)

				start := time.Now()
				resp, err := cl.Update(ctx, &pb.UpdateRequest{
					OcflPath: ocflRoot,
					ObjectId: objID,
					SrcPath:  filepath.ToSlash(srcDir),
					Message:  fmt.Sprintf("Update version 2 for %s", objID),
				})

				duration := time.Since(start)
				success := err == nil && resp != nil && resp.GetSuccess() && resp.GetVersion() == "v2"
				metrics.record(duration, success)
			}(i)
		}

		wg.Wait()
		metrics.finish()
		metrics.report(t, "Concurrent Update (v1 -> v2)")

		require.Equal(t, int64(numObjects), metrics.successCount)
		require.Equal(t, int64(0), metrics.errorCount)
	})

	// Test 4: Live Log Streaming under concurrent load
	t.Run("ConcurrentStreamingWithLogHandlers", func(t *testing.T) {
		const totalRequests = 30
		const concurrency = 5

		metrics := newLoadMetrics()
		var logCounter int64
		workChan := make(chan int, totalRequests)
		for i := 0; i < totalRequests; i++ {
			workChan <- i
		}
		close(workChan)

		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for id := range workChan {
					objID := fmt.Sprintf("urn:load:streamed:obj:%d", id)
					start := time.Now()

					resp, err := cl.Add(ctx, &pb.AddRequest{
						OcflPath: ocflRoot,
						ObjectId: objID,
						SrcPath:  filepath.ToSlash(srcDir),
						Message:  "Stream log load test",
					}, client.WithCallLogHandler(func(l *pb.LogEntry) {
						if l != nil {
							atomic.AddInt64(&logCounter, 1)
						}
					}))

					duration := time.Since(start)
					success := err == nil && resp != nil && resp.GetSuccess()
					metrics.record(duration, success)
				}
			}(w)
		}

		wg.Wait()
		metrics.finish()
		metrics.report(t, "Concurrent Add with Live Log Handlers")

		require.Equal(t, int64(totalRequests), metrics.successCount)
		require.True(t, atomic.LoadInt64(&logCounter) > 0, "Expected server log entries to be streamed and captured")
		t.Logf("Total live log messages captured across all streams: %d", atomic.LoadInt64(&logCounter))
	})
}

// BenchmarkGRPCService provides Go standard benchmarking for gRPC operations.
func BenchmarkGRPCService(b *testing.B) {
	// Start server once
	cfg, err := bootstrap.LoadConfig("")
	if err != nil {
		b.Fatalf("failed to load config: %v", err)
	}
	cfg.Addr = "127.0.0.1:0"

	server, err := bootstrap.NewServer(cfg)
	if err != nil {
		b.Fatalf("failed to create server: %v", err)
	}

	go func() {
		_ = server.Serve()
	}()
	time.Sleep(100 * time.Millisecond)
	defer server.GracefulStop()

	cl, err := client.NewClient(server.Addr(), client.WithInsecure())
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = cl.Close() }()

	tempDir := b.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "bench_root"))
	ctx := context.Background()

	// Init storage root
	_, err = cl.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	if err != nil {
		b.Fatalf("failed to init storage root: %v", err)
	}

	srcDir := filepath.Join(tempDir, "bench_src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		b.Fatalf("failed to create src dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "sample.txt"), []byte("benchmark content"), 0644); err != nil {
		b.Fatalf("failed to write sample file: %v", err)
	}

	// Ingest sample objects for validation benchmarks
	for i := 0; i < 20; i++ {
		_, err := cl.Add(ctx, &pb.AddRequest{
			OcflPath: ocflRoot,
			ObjectId: fmt.Sprintf("urn:bench:obj:%d", i),
			SrcPath:  filepath.ToSlash(srcDir),
			Message:  "Benchmark seed object",
			User: &pb.User{
				Name:    "Benchmark Runner",
				Address: "mailto:bench@example.org",
			},
		})
		if err != nil {
			b.Fatalf("failed to seed object: %v", err)
		}
	}

	b.Run("ValidateObject", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			objID := fmt.Sprintf("urn:bench:obj:%d", i%20)
			res, err := cl.Validate(ctx, &pb.ValidateRequest{
				OcflPath: ocflRoot,
				ObjectId: objID,
			})
			if err != nil || !res.GetIsValid() {
				b.Fatalf("validation failed: %v", err)
			}
		}
	})

	b.Run("ParallelValidateObject", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pbLoop *testing.PB) {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pbLoop.Next() {
				objID := fmt.Sprintf("urn:bench:obj:%d", r.Intn(20))
				res, err := cl.Validate(ctx, &pb.ValidateRequest{
					OcflPath: ocflRoot,
					ObjectId: objID,
				})
				if err != nil || !res.GetIsValid() {
					b.Fatalf("parallel validation failed: %v", err)
				}
			}
		})
	})
}
