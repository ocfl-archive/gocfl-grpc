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
	return startIntegrationServerWithConfig(t, cfg)
}

// startIntegrationServerWithConfig starts a full gRPC server with the given configuration.
func startIntegrationServerWithConfig(t *testing.T, cfg *bootstrap.Config) (*bootstrap.Server, string) {
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

// TestServerShutdownIntegration verifies that calling the Shutdown gRPC endpoint behaves correctly based on AllowAPIShutdown.
func TestServerShutdownIntegration(t *testing.T) {
	// 1. Verify that shutdown is rejected when AllowAPIShutdown is false (default)
	t.Run("ShutdownDisallowedByDefault", func(t *testing.T) {
		cfg, err := bootstrap.LoadConfig("")
		require.NoError(t, err)
		cfg.Addr = "127.0.0.1:0"
		cfg.AllowAPIShutdown = false

		server, addr := startIntegrationServerWithConfig(t, cfg)
		defer server.GracefulStop()

		cl, err := client.NewClient(addr, client.WithInsecure())
		require.NoError(t, err)
		defer func() { _ = cl.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = cl.Shutdown(ctx, &pb.ShutdownRequest{
			Reason: "unauthorized shutdown test",
		})
		require.Error(t, err, "Expected permission error when AllowAPIShutdown is false")
		assert.Contains(t, err.Error(), "gRPC API shutdown is not enabled")
	})

	// 2. Verify that shutdown terminates server when AllowAPIShutdown is true
	t.Run("ShutdownAllowed", func(t *testing.T) {
		cfg, err := bootstrap.LoadConfig("")
		require.NoError(t, err)
		cfg.Addr = "127.0.0.1:0"
		cfg.AllowAPIShutdown = true

		server, addr := startIntegrationServerWithConfig(t, cfg)

		cl, err := client.NewClient(addr, client.WithInsecure())
		require.NoError(t, err)
		defer func() { _ = cl.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Verify server is functional
		tempDir := t.TempDir()
		ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "shutdown_ocfl_root"))

		initResp, err := cl.Init(ctx, &pb.InitRequest{
			OcflPath:    ocflRoot,
			OcflVersion: "1.1",
			Digest:      "sha512",
		})
		require.NoError(t, err)
		require.True(t, initResp.GetSuccess())

		// Request server shutdown via gRPC
		shutResp, err := cl.Shutdown(ctx, &pb.ShutdownRequest{
			Reason: "authorized shutdown test",
			Force:  false,
		})
		require.NoError(t, err)
		require.True(t, shutResp.GetSuccess())
		assert.Contains(t, shutResp.GetMessage(), "shutdown initiated")

		// Assert server terminates within a short timeout
		select {
		case <-server.Done():
			t.Log("Server shut down cleanly via gRPC call")
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for server to shut down after gRPC Shutdown call")
		}

		// Verify subsequent gRPC calls fail because the server is stopped
		shortCtx, shortCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer shortCancel()

		_, err = cl.Init(shortCtx, &pb.InitRequest{
			OcflPath: filepath.ToSlash(filepath.Join(tempDir, "subsequent_root")),
		})
		require.Error(t, err, "Expected error on stopped server")
	})
}

// TestFineGrainedHandlesIntegration verifies interactive multi-step operations using the hybrid handle API.
func TestFineGrainedHandlesIntegration(t *testing.T) {
	server, addr := startIntegrationServer(t)
	defer server.GracefulStop()

	cl, err := client.NewClient(addr, client.WithInsecure())
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	srPath := filepath.ToSlash(filepath.Join(tempDir, "handle_ocfl_store"))

	// 1. Initialize StorageRoot via Handle
	srHandle, err := cl.InitStorageRootHandle(ctx, &pb.InitStorageRootHandleRequest{
		OcflPath:    srPath,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	require.NoError(t, err)
	require.NotEmpty(t, srHandle.GetId())

	// 2. Query StorageRoot details and verify initially empty object list
	srDetails, err := cl.GetStorageRootDetails(ctx, &pb.GetStorageRootDetailsRequest{
		StoragerootHandleId: srHandle.GetId(),
	})
	require.NoError(t, err)
	assert.Equal(t, "1.1", srDetails.GetOcflVersion())
	assert.Equal(t, "sha512", srDetails.GetDigestAlgorithm())

	listResp, err := cl.ListObjects(ctx, &pb.ListObjectsRequest{
		StoragerootHandleId: srHandle.GetId(),
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.GetObjectFolders())

	// 3. Initialize new Object within the StorageRoot Handle
	objID := "urn:hybrid:archive:1"
	objHandle, err := cl.InitObjectHandle(ctx, &pb.InitObjectHandleRequest{
		StoragerootHandleId: srHandle.GetId(),
		ObjectId:            objID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, objHandle.GetId())

	// 4. Begin Update v1
	updHandle1, err := cl.BeginUpdate(ctx, &pb.BeginUpdateRequest{
		ObjectHandleId: objHandle.GetId(),
		Message:        "Initial ingest v1 via handle API",
		User: &pb.User{
			Name:    "Alice Archivist",
			Address: "mailto:alice@example.org",
		},
	})
	require.NoError(t, err)

	// 5. Add files directly via VFS path and data bytes
	vfsReportFile := filepath.ToSlash(filepath.Join(tempDir, "report_from_vfs.txt"))
	require.NoError(t, os.WriteFile(vfsReportFile, []byte("Annual report data for 2026 via VFS stream."), 0644))

	addFileResp1, err := cl.AddFile(ctx, &pb.AddFileRequest{
		UpdaterHandleId: updHandle1.GetId(),
		SrcPath:         vfsReportFile,
		DestPath:        "documents/report.txt",
	})
	require.NoError(t, err)
	assert.True(t, addFileResp1.GetSuccess())

	addFileResp2, err := cl.AddFile(ctx, &pb.AddFileRequest{
		UpdaterHandleId: updHandle1.GetId(),
		Path:            "metadata.json",
		Content:         []byte(`{"title":"Annual Report","year":2026}`),
	})
	require.NoError(t, err)
	assert.True(t, addFileResp2.GetSuccess())

	// 6. Commit Update v1 and verify returned Inventory snapshot
	commitResp1, err := cl.CommitUpdate(ctx, &pb.CommitUpdateRequest{
		UpdaterHandleId: updHandle1.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, commitResp1.GetSuccess())
	assert.Equal(t, "v1", commitResp1.GetHeadVersion())
	require.NotNil(t, commitResp1.GetNewInventory())
	assert.Equal(t, objID, commitResp1.GetNewInventory().GetId())
	assert.Equal(t, "v1", commitResp1.GetNewInventory().GetHead())
	assert.Len(t, commitResp1.GetNewInventory().GetVersions(), 1)

	// 7. Inspect full Inventory snapshot via GetInventory RPC
	invResp1, err := cl.GetInventory(ctx, &pb.GetInventoryRequest{
		ObjectHandleId: objHandle.GetId(),
	})
	require.NoError(t, err)
	require.NotNil(t, invResp1.GetInventory())
	assert.Equal(t, objID, invResp1.GetInventory().GetId())
	assert.Equal(t, "v1", invResp1.GetInventory().GetHead())

	// 8. Validate Object via Handle
	valResp, err := cl.ValidateObjectHandle(ctx, &pb.ValidateObjectHandleRequest{
		ObjectHandleId: objHandle.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, valResp.GetIsValid())

	// 9. Begin Update v2: rename, modify, and add a directory from VFS
	updHandle2, err := cl.BeginUpdate(ctx, &pb.BeginUpdateRequest{
		ObjectHandleId: objHandle.GetId(),
		Message:        "Second update v2: rename report and add attachments",
		User: &pb.User{
			Name:    "Alice Archivist",
			Address: "mailto:alice@example.org",
		},
	})
	require.NoError(t, err)

	// 9a. Rename file within object state
	renameResp, err := cl.RenameFile(ctx, &pb.RenameFileRequest{
		UpdaterHandleId: updHandle2.GetId(),
		SourcePath:      "documents/report.txt",
		DestPath:        "documents/annual_report_2026.txt",
	})
	require.NoError(t, err)
	assert.True(t, renameResp.GetSuccess())

	// 9b. Add folder from VFS
	extraDir := filepath.Join(tempDir, "extra_attachments")
	require.NoError(t, os.MkdirAll(extraDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(extraDir, "appendix.txt"), []byte("Appendix data."), 0644))

	addFolderResp, err := cl.AddFolder(ctx, &pb.AddFolderRequest{
		UpdaterHandleId: updHandle2.GetId(),
		SrcPath:         filepath.ToSlash(extraDir),
		Area:            "",
	})
	require.NoError(t, err)
	assert.True(t, addFolderResp.GetSuccess())

	// 9c. Commit v2
	commitResp2, err := cl.CommitUpdate(ctx, &pb.CommitUpdateRequest{
		UpdaterHandleId: updHandle2.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, commitResp2.GetSuccess())
	assert.Equal(t, "v2", commitResp2.GetHeadVersion())
	require.NotNil(t, commitResp2.GetNewInventory())
	assert.Equal(t, "v2", commitResp2.GetNewInventory().GetHead())
	assert.Len(t, commitResp2.GetNewInventory().GetVersions(), 2)

	// 10. Verify StorageRoot list now lists the object folder
	listResp2, err := cl.ListObjects(ctx, &pb.ListObjectsRequest{
		StoragerootHandleId: srHandle.GetId(),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listResp2.GetObjectFolders())

	// 11. Test KeepAlive and CloseHandle
	keepResp, err := cl.KeepAlive(ctx, &pb.KeepAliveRequest{
		HandleId:      objHandle.GetId(),
		ExtendSeconds: 1800,
	})
	require.NoError(t, err)
	assert.True(t, keepResp.GetSuccess())

	closeObjResp, err := cl.CloseHandle(ctx, &pb.CloseHandleRequest{
		HandleId: objHandle.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, closeObjResp.GetSuccess())

	closeSrResp, err := cl.CloseHandle(ctx, &pb.CloseHandleRequest{
		HandleId: srHandle.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, closeSrResp.GetSuccess())

	// 12. Re-open StorageRoot and Object to test OpenStorageRoot and OpenObject
	srHandle2, err := cl.OpenStorageRoot(ctx, &pb.OpenStorageRootRequest{
		OcflPath: srPath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, srHandle2.GetId())
	defer func() { _, _ = cl.CloseHandle(ctx, &pb.CloseHandleRequest{HandleId: srHandle2.GetId()}) }()

	objHandle2, err := cl.OpenObject(ctx, &pb.OpenObjectRequest{
		StoragerootHandleId: srHandle2.GetId(),
		ObjectId:            objID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, objHandle2.GetId())
	defer func() { _, _ = cl.CloseHandle(ctx, &pb.CloseHandleRequest{HandleId: objHandle2.GetId()}) }()

	invResp2, err := cl.GetInventory(ctx, &pb.GetInventoryRequest{
		ObjectHandleId: objHandle2.GetId(),
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", invResp2.GetInventory().GetHead())

	// 13. Test GetMetadata via Handle
	metaHandleResp, err := cl.GetMetadata(ctx, &pb.GetMetadataRequest{
		ObjectHandleId: objHandle2.GetId(),
		Format:         "json",
	})
	require.NoError(t, err)
	assert.True(t, metaHandleResp.GetSuccess())
	require.NotNil(t, metaHandleResp.GetMetadata())
	assert.Equal(t, objID, metaHandleResp.GetMetadata().GetId())
	assert.Equal(t, "v2", metaHandleResp.GetMetadata().GetHead())
	assert.NotEmpty(t, metaHandleResp.GetJsonData())

	// 14. Test ExtractMetadata macro RPC
	macroMetaResp, err := cl.ExtractMetadata(ctx, &pb.ExtractMetadataRequest{
		OcflPath:  srPath,
		ObjectId:  objID,
		Format:    "human",
		Obfuscate: true,
	})
	require.NoError(t, err)
	assert.True(t, macroMetaResp.GetSuccess())
	assert.NotEmpty(t, macroMetaResp.GetHumanData())
	require.NotNil(t, macroMetaResp.GetMetadata())
	assert.Equal(t, objID, macroMetaResp.GetMetadata().GetId())
}
