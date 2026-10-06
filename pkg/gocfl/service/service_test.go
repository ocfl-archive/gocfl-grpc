package service_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/filesystem/pkg/vfsrw"
	"github.com/ocfl-archive/gocfl-grpc/pkg/client"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/service"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupTestGRPCServer(t *testing.T) (client.GocflClient, *service.GocflService, func()) {
	out := zerolog.Nop()
	logger := zLogger.ZLogger(&out)

	cfg := vfsrw.Config{}
	vfs, err := vfsrw.NewFS(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create VFS: %v", err)
	}
	if err := vfsrw.AddLocal(vfs, nil); err != nil {
		t.Fatalf("Failed to add local fs: %v", err)
	}

	svc, err := service.NewGocflService(
		service.WithLogger(logger),
		service.WithVFS(vfs),
	)
	if err != nil {
		t.Fatalf("Failed to create GocflService: %v", err)
	}

	bufferSize := 1024 * 1024
	lis := bufconn.Listen(bufferSize)

	server := grpc.NewServer()
	pb.RegisterGocflServiceServer(server, svc)

	go func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	cl := client.NewClientFromConn(conn, logger)

	cleanup := func() {
		_ = cl.Close()
		server.Stop()
	}

	return cl, svc, cleanup
}

func TestGocflServiceLifecycle(t *testing.T) {
	cl, svc, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	tempDir := t.TempDir()

	storageRootPath := filepath.Join(tempDir, "ocfl_root")
	srcDirV1 := filepath.Join(tempDir, "src_v1")
	srcDirV2 := filepath.Join(tempDir, "src_v2")

	if err := os.MkdirAll(srcDirV1, 0755); err != nil {
		t.Fatalf("Failed to create srcDirV1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDirV1, "sample.txt"), []byte("Hello OCFL v1"), 0644); err != nil {
		t.Fatalf("Failed to write sample.txt in src_v1: %v", err)
	}

	if err := os.MkdirAll(srcDirV2, 0755); err != nil {
		t.Fatalf("Failed to create srcDirV2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDirV2, "sample.txt"), []byte("Hello OCFL v2"), 0644); err != nil {
		t.Fatalf("Failed to write sample.txt in src_v2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDirV2, "new_file.txt"), []byte("Added in v2"), 0644); err != nil {
		t.Fatalf("Failed to write new_file.txt in src_v2: %v", err)
	}

	var logCount int64
	logHandler := func(entry *pb.LogEntry) {
		atomic.AddInt64(&logCount, 1)
	}

	// 1. Test Init
	t.Run("Init", func(t *testing.T) {
		initResp, err := cl.Init(ctx, &pb.InitRequest{
			OcflPath:    storageRootPath,
			OcflVersion: "1.1",
			Digest:      "sha512",
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}
		if !initResp.GetSuccess() {
			t.Errorf("Expected Init success true, got false")
		}
	})

	// 2. Test Add
	objectID := "test_object_1"
	t.Run("Add", func(t *testing.T) {
		addResp, err := cl.Add(ctx, &pb.AddRequest{
			OcflPath: storageRootPath,
			SrcPath:  srcDirV1,
			ObjectId: objectID,
			Message:  "Initial commit",
			User: &pb.User{
				Name:    "Alice",
				Address: "mailto:alice@example.org",
			},
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if !addResp.GetSuccess() {
			t.Errorf("Expected Add success true, got false")
		}
		if addResp.GetObjectId() != objectID {
			t.Errorf("Expected object ID %s, got %s", objectID, addResp.GetObjectId())
		}
		if addResp.GetVersion() != "v1" {
			t.Errorf("Expected version v1, got %s", addResp.GetVersion())
		}
	})

	// 3. Test Add duplicate (should fail)
	t.Run("Add Duplicate", func(t *testing.T) {
		_, err := cl.Add(ctx, &pb.AddRequest{
			OcflPath: storageRootPath,
			SrcPath:  srcDirV1,
			ObjectId: objectID,
		})
		if err == nil {
			t.Errorf("Expected error when adding existing object, got nil")
		}
	})

	// 4. Test Update
	t.Run("Update", func(t *testing.T) {
		updResp, err := cl.Update(ctx, &pb.UpdateRequest{
			OcflPath: storageRootPath,
			SrcPath:  srcDirV2,
			ObjectId: objectID,
			Message:  "Updated to v2",
			User: &pb.User{
				Name:    "Alice",
				Address: "mailto:alice@example.org",
			},
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		if !updResp.GetSuccess() {
			t.Errorf("Expected Update success true, got false")
		}
		if updResp.GetVersion() != "v2" {
			t.Errorf("Expected version v2, got %s", updResp.GetVersion())
		}
	})

	// 5. Test Validate Object
	t.Run("Validate Object", func(t *testing.T) {
		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: storageRootPath,
			ObjectId: objectID,
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("Validate failed: %v", err)
		}
		if !valResp.GetIsValid() {
			t.Errorf("Expected valid object, got invalid: %v, errors: %v", valResp.GetMessage(), valResp.GetErrors())
		}
	})

	// 6. Test Validate Storage Root
	t.Run("Validate Storage Root", func(t *testing.T) {
		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: storageRootPath,
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("Validate storage root failed: %v", err)
		}
		if !valResp.GetIsValid() {
			t.Errorf("Expected valid storage root, got invalid: %v, errors: %v", valResp.GetMessage(), valResp.GetErrors())
		}
	})

	// 7. Test Create
	t.Run("Create", func(t *testing.T) {
		storageRootPath2 := filepath.Join(tempDir, "ocfl_root_create")
		createObjectID := "test_object_create"
		createResp, err := cl.Create(ctx, &pb.CreateRequest{
			OcflPath:    storageRootPath2,
			SrcPath:     srcDirV1,
			ObjectId:    createObjectID,
			Message:     "Created root with initial object",
			OcflVersion: "1.1",
			User: &pb.User{
				Name:    "Bob",
				Address: "mailto:bob@example.org",
			},
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if !createResp.GetSuccess() {
			t.Errorf("Expected Create success true, got false")
		}
		if createResp.GetVersion() != "v1" {
			t.Errorf("Expected version v1, got %s", createResp.GetVersion())
		}

		// Validate created object
		valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
			OcflPath: storageRootPath2,
			ObjectId: createObjectID,
		})
		if err != nil {
			t.Fatalf("Validate created object failed: %v", err)
		}
		if !valResp.GetIsValid() {
			t.Errorf("Expected valid created object, got invalid: %v, errors: %v", valResp.GetMessage(), valResp.GetErrors())
		}
	})

	// 8. Test ExtractMetadata
	t.Run("ExtractMetadata", func(t *testing.T) {
		// 8a. JSON format
		metaResp, err := cl.ExtractMetadata(ctx, &pb.ExtractMetadataRequest{
			OcflPath: storageRootPath,
			ObjectId: objectID,
			Format:   "json",
		}, client.WithCallLogHandler(logHandler))
		if err != nil {
			t.Fatalf("ExtractMetadata failed: %v", err)
		}
		if !metaResp.GetSuccess() {
			t.Errorf("Expected ExtractMetadata success true, got false")
		}
		if metaResp.GetMetadata() == nil {
			t.Fatalf("Expected non-nil metadata")
		}
		if metaResp.GetMetadata().GetId() != objectID {
			t.Errorf("Expected object ID %s, got %s", objectID, metaResp.GetMetadata().GetId())
		}
		if metaResp.GetJsonData() == "" {
			t.Errorf("Expected non-empty JsonData")
		}

		// 8b. Human format and Obfuscated
		metaHumanResp, err := cl.ExtractMetadata(ctx, &pb.ExtractMetadataRequest{
			OcflPath:  storageRootPath,
			ObjectId:  objectID,
			Format:    "human",
			Obfuscate: true,
		})
		if err != nil {
			t.Fatalf("ExtractMetadata human/obfuscate failed: %v", err)
		}
		if !metaHumanResp.GetSuccess() {
			t.Errorf("Expected ExtractMetadata human/obfuscate success true, got false")
		}
		if metaHumanResp.GetHumanData() == "" {
			t.Errorf("Expected non-empty HumanData")
		}
	})

	// 9. Test Shutdown
	t.Run("Shutdown Disallowed and Allowed", func(t *testing.T) {
		shutdownCalled := false
		svc.SetShutdownFunc(func(force bool) {
			shutdownCalled = true
		})

		// 8a. Test Shutdown when disallowed (default)
		svc.SetAllowAPIShutdown(false)
		_, err := cl.Shutdown(ctx, &pb.ShutdownRequest{
			Reason: "service unit test disallowed",
			Force:  false,
		})
		if err == nil {
			t.Errorf("Expected error when shutdown is disallowed, got nil")
		}

		// 8b. Test Shutdown when allowed
		svc.SetAllowAPIShutdown(true)
		shutResp, err := cl.Shutdown(ctx, &pb.ShutdownRequest{
			Reason: "service unit test allowed",
			Force:  false,
		})
		if err != nil {
			t.Fatalf("Shutdown failed when allowed: %v", err)
		}
		if !shutResp.GetSuccess() {
			t.Errorf("Expected Shutdown success true, got false")
		}

		time.Sleep(150 * time.Millisecond)
		if !shutdownCalled {
			t.Errorf("Expected shutdown callback to be called")
		}
	})
}
