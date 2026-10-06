package client

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupBufconnServer(t *testing.T) (*grpc.Server, *bufconn.Listener, *service.GocflService) {
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()

	svc, err := service.NewGocflService(service.WithAllowAPIShutdown(true))
	require.NoError(t, err)

	pb.RegisterGocflServiceServer(srv, svc)

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("Server error: %v", err)
		}
	}()

	return srv, lis, svc
}

func TestClientWithBufconn(t *testing.T) {
	srv, lis, _ := setupBufconnServer(t)
	defer srv.Stop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	cl, err := NewClient("passthrough://bufnet",
		WithDialOptions(
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() { _ = cl.Close() }()

	assert.NotNil(t, cl.Conn())
	assert.NotNil(t, cl.GRPCClient())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "ocfl_root"))

	// 1. Init
	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	require.NoError(t, err)
	assert.True(t, initResp.GetSuccess())

	// 2. Add
	srcDir := filepath.Join(tempDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("client test content"), 0644))

	addResp, err := cl.Add(ctx, &pb.AddRequest{
		OcflPath: ocflRoot,
		ObjectId: "urn:test:client:1",
		SrcPath:  filepath.ToSlash(srcDir),
		Message:  "initial version",
		User: &pb.User{
			Name:    "Client Tester",
			Address: "mailto:tester@example.com",
		},
	})
	require.NoError(t, err)
	assert.True(t, addResp.GetSuccess())
	assert.Equal(t, "urn:test:client:1", addResp.GetObjectId())
	assert.Equal(t, "v1", addResp.GetVersion())

	// 3. Update
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("client updated content"), 0644))
	updResp, err := cl.Update(ctx, &pb.UpdateRequest{
		OcflPath: ocflRoot,
		ObjectId: "urn:test:client:1",
		SrcPath:  filepath.ToSlash(srcDir),
		Message:  "second version",
		User: &pb.User{
			Name:    "Client Tester",
			Address: "mailto:tester@example.com",
		},
	})
	require.NoError(t, err)
	assert.True(t, updResp.GetSuccess())
	assert.Equal(t, "urn:test:client:1", updResp.GetObjectId())
	assert.Equal(t, "v2", updResp.GetVersion())

	// 4. Validate Object
	valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
		OcflPath: ocflRoot,
		ObjectId: "urn:test:client:1",
	})
	require.NoError(t, err)
	assert.True(t, valResp.GetIsValid())

	// 5. Create
	createRoot := filepath.ToSlash(filepath.Join(tempDir, "ocfl_create_root"))
	createResp, err := cl.Create(ctx, &pb.CreateRequest{
		OcflPath:    createRoot,
		SrcPath:     filepath.ToSlash(srcDir),
		ObjectId:    "urn:test:client:created",
		Message:     "created directly",
		OcflVersion: "1.1",
		Digest:      "sha512",
		User: &pb.User{
			Name:    "Client Tester",
			Address: "mailto:tester@example.com",
		},
	})
	require.NoError(t, err)
	assert.True(t, createResp.GetSuccess())
	assert.Equal(t, "urn:test:client:created", createResp.GetObjectId())
	assert.Equal(t, "v1", createResp.GetVersion())

	// 6. Test Handle-Based Hybrid Operations
	// 6a. Init StorageRoot Handle
	handleRoot := filepath.ToSlash(filepath.Join(tempDir, "ocfl_handle_root"))
	srHandle, err := cl.InitStorageRootHandle(ctx, &pb.InitStorageRootHandleRequest{
		OcflPath:    handleRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	require.NoError(t, err)
	require.NotEmpty(t, srHandle.GetId())

	// 6b. Get StorageRoot Details & List Objects
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

	// 6c. Init Object Handle
	objHandle, err := cl.InitObjectHandle(ctx, &pb.InitObjectHandleRequest{
		StoragerootHandleId: srHandle.GetId(),
		ObjectId:            "urn:handle:obj1",
	})
	require.NoError(t, err)
	require.NotEmpty(t, objHandle.GetId())

	// 6d. Begin Update & Add File directly via Handle
	updHandle, err := cl.BeginUpdate(ctx, &pb.BeginUpdateRequest{
		ObjectHandleId: objHandle.GetId(),
		Message:        "first version via handle",
		User: &pb.User{
			Name:    "Handle Tester",
			Address: "mailto:handle@example.com",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, updHandle.GetId())

	// 6d. Add File to Updater using direct VFS path (no bytes needed)
	vfsSampleFile := filepath.ToSlash(filepath.Join(tempDir, "sample_vfs_file.txt"))
	require.NoError(t, os.WriteFile(vfsSampleFile, []byte("direct vfs stream content"), 0644))

	addFileResp, err := cl.AddFile(ctx, &pb.AddFileRequest{
		UpdaterHandleId: updHandle.GetId(),
		SrcPath:         vfsSampleFile,
		DestPath:        "data/file1.txt",
	})
	require.NoError(t, err)
	assert.True(t, addFileResp.GetSuccess())

	// 6e. Commit Update
	commitResp, err := cl.CommitUpdate(ctx, &pb.CommitUpdateRequest{
		UpdaterHandleId: updHandle.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, commitResp.GetSuccess())
	assert.Equal(t, "v1", commitResp.GetHeadVersion())
	require.NotNil(t, commitResp.GetNewInventory())
	assert.Equal(t, "urn:handle:obj1", commitResp.GetNewInventory().GetId())
	assert.Equal(t, "v1", commitResp.GetNewInventory().GetHead())

	// 6f. Get Inventory Snapshot
	invResp, err := cl.GetInventory(ctx, &pb.GetInventoryRequest{
		ObjectHandleId: objHandle.GetId(),
	})
	require.NoError(t, err)
	require.NotNil(t, invResp.GetInventory())
	assert.Equal(t, "urn:handle:obj1", invResp.GetInventory().GetId())
	assert.Equal(t, "v1", invResp.GetInventory().GetHead())
	assert.NotEmpty(t, invResp.GetInventory().GetManifest())
	assert.NotEmpty(t, invResp.GetInventory().GetVersions())

	// 6g. Validate Object via Handle
	valObjResp, err := cl.ValidateObjectHandle(ctx, &pb.ValidateObjectHandleRequest{
		ObjectHandleId: objHandle.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, valObjResp.GetIsValid())

	// 6h. Incremental Update: Add v2 via Handle
	updHandle2, err := cl.BeginUpdate(ctx, &pb.BeginUpdateRequest{
		ObjectHandleId: objHandle.GetId(),
		Message:        "second version via handle",
	})
	require.NoError(t, err)

	addFileResp2, err := cl.AddFile(ctx, &pb.AddFileRequest{
		UpdaterHandleId: updHandle2.GetId(),
		Path:            "data/file2.txt",
		Content:         []byte("second file content"),
	})
	require.NoError(t, err)
	assert.True(t, addFileResp2.GetSuccess())

	commitResp2, err := cl.CommitUpdate(ctx, &pb.CommitUpdateRequest{
		UpdaterHandleId: updHandle2.GetId(),
	})
	require.NoError(t, err)
	assert.True(t, commitResp2.GetSuccess())
	assert.Equal(t, "v2", commitResp2.GetHeadVersion())
	require.NotNil(t, commitResp2.GetNewInventory())
	assert.Equal(t, "v2", commitResp2.GetNewInventory().GetHead())

	// 6i. KeepAlive and Close Handles
	keepResp, err := cl.KeepAlive(ctx, &pb.KeepAliveRequest{
		HandleId:      srHandle.GetId(),
		ExtendSeconds: 600,
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

	// 7. Shutdown
	shutResp, err := cl.Shutdown(ctx, &pb.ShutdownRequest{
		Reason: "client test shutdown",
		Force:  false,
	})
	require.NoError(t, err)
	assert.True(t, shutResp.GetSuccess())
}

func TestNewClientFromConn(t *testing.T) {
	srv, lis, _ := setupBufconnServer(t)
	defer srv.Stop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	cl := NewClientFromConn(conn, nil)
	require.NotNil(t, cl)
	assert.Equal(t, conn, cl.Conn())
	assert.NotNil(t, cl.GRPCClient())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "ocfl_from_conn"))

	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath: ocflRoot,
	})
	require.NoError(t, err)
	assert.True(t, initResp.GetSuccess())
}

func TestClientOptions(t *testing.T) {
	// Test WithInsecure
	cl, err := NewClient("127.0.0.1:0", WithInsecure())
	require.NoError(t, err)
	require.NotNil(t, cl)
	_ = cl.Close()
}
