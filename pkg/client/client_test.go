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

	svc, err := service.NewGocflService()
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
