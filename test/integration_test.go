package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ocfl-archive/gocfl-grpc/pkg/client"
	"github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/bootstrap"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startIntegrationServer starts a full gRPC server on an ephemeral TCP port.
func startIntegrationServer(t *testing.T) (*bootstrap.Server, string) {
	cfg, err := bootstrap.LoadConfig("")
	require.NoError(t, err)

	cfg.Addr = "127.0.0.1:0"

	server, err := bootstrap.NewServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	go func() {
		_ = server.Serve()
	}()

	time.Sleep(100 * time.Millisecond)

	addr := server.Addr()
	require.NotEmpty(t, addr)

	return server, addr
}

func TestFullLifecycleIntegration(t *testing.T) {
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() { _ = cl.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "storage_root"))

	// Step 1: Init Storage Root
	t.Run("InitStorageRoot", func(t *testing.T) {
		resp, err := cl.Init(ctx, &pb.InitRequest{
			OcflPath:    ocflRoot,
			OcflVersion: "1.1",
			Digest:      "sha512",
		})
		require.NoError(t, err)
		assert.True(t, resp.GetSuccess())
		assert.Contains(t, resp.GetMessage(), "initialized")
	})

	// Step 2: Validate Empty Storage Root
	t.Run("ValidateEmptyStorageRoot", func(t *testing.T) {
		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: ocflRoot,
		})
		require.NoError(t, err)
		assert.True(t, valResp.GetIsValid())
		assert.Empty(t, valResp.GetErrors())
	})

	// Step 3: Add Object v1
	srcDir := filepath.Join(tempDir, "content_v1")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("Integration test version 1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "data.json"), []byte(`{"key": "value1"}`), 0644))

	objectID := "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479"

	t.Run("AddObject_v1", func(t *testing.T) {
		addResp, err := cl.Add(ctx, &pb.AddRequest{
			OcflPath: ocflRoot,
			ObjectId: objectID,
			SrcPath:  filepath.ToSlash(srcDir),
			Message:  "Initial object ingest",
			User: &pb.User{
				Name:    "Integration Tester",
				Address: "mailto:integration@example.com",
			},
		})
		require.NoError(t, err)
		assert.True(t, addResp.GetSuccess())
		assert.Equal(t, objectID, addResp.GetObjectId())
		assert.Equal(t, "v1", addResp.GetVersion())
	})

	// Step 4: Validate Object v1
	t.Run("ValidateObject_v1", func(t *testing.T) {
		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: ocflRoot,
			ObjectId: objectID,
		})
		require.NoError(t, err)
		assert.True(t, valResp.GetIsValid())
		assert.Empty(t, valResp.GetErrors())
	})

	// Step 5: Update Object to v2
	srcDirV2 := filepath.Join(tempDir, "content_v2")
	require.NoError(t, os.MkdirAll(srcDirV2, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV2, "file1.txt"), []byte("Integration test version 2 - modified"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV2, "extra.txt"), []byte("additional content"), 0644))

	t.Run("UpdateObject_v2", func(t *testing.T) {
		updResp, err := cl.Update(ctx, &pb.UpdateRequest{
			OcflPath: ocflRoot,
			ObjectId: objectID,
			SrcPath:  filepath.ToSlash(srcDirV2),
			Message:  "Update with modified files",
			User: &pb.User{
				Name:    "Integration Tester",
				Address: "mailto:integration@example.com",
			},
		})
		require.NoError(t, err)
		assert.True(t, updResp.GetSuccess())
		assert.Equal(t, objectID, updResp.GetObjectId())
		assert.Equal(t, "v2", updResp.GetVersion())
	})

	// Step 6: Validate Object v2 and Storage Root
	t.Run("ValidateObject_v2_And_StorageRoot", func(t *testing.T) {
		valObjResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: ocflRoot,
			ObjectId: objectID,
		})
		require.NoError(t, err)
		assert.True(t, valObjResp.GetIsValid())

		valRootResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: ocflRoot,
		})
		require.NoError(t, err)
		assert.True(t, valRootResp.GetIsValid())
	})

	// Step 7: Create Combined Object in Fresh Storage Root
	t.Run("CreateCombinedObject", func(t *testing.T) {
		createRoot := filepath.ToSlash(filepath.Join(tempDir, "create_root"))
		createResp, err := cl.Create(ctx, &pb.CreateRequest{
			OcflPath:    createRoot,
			SrcPath:     filepath.ToSlash(srcDir),
			ObjectId:    "urn:gocfl:created:direct",
			Message:     "Created directly in one step",
			OcflVersion: "1.1",
			Digest:      "sha512",
			User: &pb.User{
				Name:    "Direct Creator",
				Address: "mailto:creator@example.com",
			},
		})
		require.NoError(t, err)
		assert.True(t, createResp.GetSuccess())
		assert.Equal(t, "urn:gocfl:created:direct", createResp.GetObjectId())
		assert.Equal(t, "v1", createResp.GetVersion())

		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: createRoot,
			ObjectId: "urn:gocfl:created:direct",
		})
		require.NoError(t, err)
		assert.True(t, valResp.GetIsValid())
	})
}

func TestConcurrentClientOperations(t *testing.T) {
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "concurrent_root"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize root first
	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	require.NoError(t, err)
	require.True(t, initResp.GetSuccess())

	const numWorkers = 5
	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			srcDir := filepath.Join(tempDir, fmt.Sprintf("worker_src_%d", workerID))
			if err := os.MkdirAll(srcDir, 0755); err != nil {
				errCh <- err
				return
			}
			if err := os.WriteFile(filepath.Join(srcDir, "payload.txt"), []byte(fmt.Sprintf("Worker payload %d", workerID)), 0644); err != nil {
				errCh <- err
				return
			}

			objID := fmt.Sprintf("urn:worker:%d", workerID)

			addResp, err := cl.Add(ctx, &pb.AddRequest{
				OcflPath: ocflRoot,
				ObjectId: objID,
				SrcPath:  filepath.ToSlash(srcDir),
				Message:  fmt.Sprintf("Worker %d ingestion", workerID),
				User: &pb.User{
					Name:    fmt.Sprintf("Worker %d", workerID),
					Address: fmt.Sprintf("mailto:worker%d@example.org", workerID),
				},
			})
			if err != nil {
				errCh <- err
				return
			}
			if !addResp.GetSuccess() {
				errCh <- fmt.Errorf("worker %d failed to add object", workerID)
				return
			}

			valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
				OcflPath: ocflRoot,
				ObjectId: objID,
			})
			if err != nil {
				errCh <- err
				return
			}
			if !valResp.GetIsValid() {
				errCh <- fmt.Errorf("worker %d object validation failed", workerID)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	// Validate the whole storage root after concurrent additions
	valRootResp, err := cl.Validate(ctx, &pb.ValidateRequest{
		OcflPath: ocflRoot,
	})
	require.NoError(t, err)
	assert.True(t, valRootResp.GetIsValid())
}

func TestMultiVersionWorkflow(t *testing.T) {
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "multi_version_root"))

	// Init
	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath: ocflRoot,
	})
	require.NoError(t, err)
	require.True(t, initResp.GetSuccess())

	objID := "urn:gocfl:object:multiversion"

	// v1
	srcDirV1 := filepath.Join(tempDir, "v1")
	require.NoError(t, os.MkdirAll(srcDirV1, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV1, "file1.txt"), []byte("v1 content"), 0644))

	addResp, err := cl.Add(ctx, &pb.AddRequest{
		OcflPath: ocflRoot,
		ObjectId: objID,
		SrcPath:  filepath.ToSlash(srcDirV1),
		Message:  "Version 1",
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", addResp.GetVersion())

	// v2
	srcDirV2 := filepath.Join(tempDir, "v2")
	require.NoError(t, os.MkdirAll(srcDirV2, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV2, "file1.txt"), []byte("v2 content"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV2, "file2.txt"), []byte("v2 file 2"), 0644))

	updResp2, err := cl.Update(ctx, &pb.UpdateRequest{
		OcflPath: ocflRoot,
		ObjectId: objID,
		SrcPath:  filepath.ToSlash(srcDirV2),
		Message:  "Version 2",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", updResp2.GetVersion())

	// v3
	srcDirV3 := filepath.Join(tempDir, "v3")
	require.NoError(t, os.MkdirAll(srcDirV3, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV3, "file1.txt"), []byte("v3 content"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV3, "file2.txt"), []byte("v3 file 2"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDirV3, "file3.txt"), []byte("v3 file 3"), 0644))

	updResp3, err := cl.Update(ctx, &pb.UpdateRequest{
		OcflPath: ocflRoot,
		ObjectId: objID,
		SrcPath:  filepath.ToSlash(srcDirV3),
		Message:  "Version 3",
	})
	require.NoError(t, err)
	assert.Equal(t, "v3", updResp3.GetVersion())

	// Validate after 3 versions
	valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
		OcflPath: ocflRoot,
		ObjectId: objID,
	})
	require.NoError(t, err)
	assert.True(t, valResp.GetIsValid())
}

func TestErrorScenarios(t *testing.T) {
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tempDir := t.TempDir()

	t.Run("UpdateNonExistentObject", func(t *testing.T) {
		ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "storage_root_errors"))
		initResp, err := cl.Init(ctx, &pb.InitRequest{
			OcflPath: ocflRoot,
		})
		require.NoError(t, err)
		require.True(t, initResp.GetSuccess())

		srcDir := filepath.Join(tempDir, "src_non_existent")
		require.NoError(t, os.MkdirAll(srcDir, 0755))

		resp, err := cl.Update(ctx, &pb.UpdateRequest{
			OcflPath: ocflRoot,
			ObjectId: "urn:gocfl:does:not:exist",
			SrcPath:  filepath.ToSlash(srcDir),
		})
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("ValidateNonExistentRoot", func(t *testing.T) {
		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: filepath.ToSlash(filepath.Join(tempDir, "completely_non_existent_path")),
		})
		// Validate returns isValid=false or an error
		if err == nil {
			assert.False(t, valResp.GetIsValid())
		} else {
			assert.Error(t, err)
		}
	})
}

func TestLiveLogStreamingIntegration(t *testing.T) {
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "streaming_logs_root"))

	var streamedLogs []*pb.LogEntry
	var logMu sync.Mutex
	logHandler := func(entry *pb.LogEntry) {
		logMu.Lock()
		defer logMu.Unlock()
		streamedLogs = append(streamedLogs, entry)
	}

	// 1. Init with log handler
	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	}, client.WithCallLogHandler(logHandler))
	require.NoError(t, err)
	assert.True(t, initResp.GetSuccess())

	// 2. Add with raw stream to inspect log entries directly
	srcDir := filepath.Join(tempDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("streaming log test content"), 0644))

	addStream, err := cl.AddStream(ctx, &pb.AddRequest{
		OcflPath: ocflRoot,
		ObjectId: "urn:gocfl:streamed:1",
		SrcPath:  filepath.ToSlash(srcDir),
		Message:  "Testing stream logs",
		User: &pb.User{
			Name:    "Stream Tester",
			Address: "mailto:stream@example.com",
		},
	})
	require.NoError(t, err)

	var addLogs []*pb.LogEntry
	var addResult *pb.AddResult
	for {
		resp, err := addStream.Recv()
		if err != nil {
			break
		}
		if l := resp.GetLog(); l != nil {
			addLogs = append(addLogs, l)
		}
		if r := resp.GetResult(); r != nil {
			addResult = r
		}
	}

	require.NotNil(t, addResult)
	assert.True(t, addResult.GetSuccess())
	assert.Equal(t, "urn:gocfl:streamed:1", addResult.GetObjectId())
	assert.Equal(t, "v1", addResult.GetVersion())

	// 3. Validate with log handler
	valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
		OcflPath: ocflRoot,
		ObjectId: "urn:gocfl:streamed:1",
	}, client.WithCallLogHandler(logHandler))
	require.NoError(t, err)
	assert.True(t, valResp.GetIsValid())
}
