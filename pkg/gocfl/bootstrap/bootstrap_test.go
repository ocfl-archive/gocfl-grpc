package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBootstrapServer(t *testing.T) {
	tempDir := t.TempDir()

	cfg, err := LoadConfig("")
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Addr)

	// Bind to an arbitrary available port for testing
	cfg.Addr = "127.0.0.1:0"

	server, err := NewServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	go func() {
		_ = server.Serve()
	}()
	defer server.GracefulStop()

	// Wait briefly for server listener to be ready
	time.Sleep(100 * time.Millisecond)

	serverAddr := server.Addr()
	assert.NotEmpty(t, serverAddr)

	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := pb.NewGocflServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test Init via bootstrapped server
	ocflRoot := filepath.ToSlash(filepath.Join(tempDir, "ocfl_storage"))
	initResp, err := client.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	require.NoError(t, err)
	assert.True(t, initResp.GetSuccess())

	// Create test source content
	srcDir := filepath.Join(tempDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("hello bootstrap ocfl"), 0644))

	// Test Add
	addResp, err := client.Add(ctx, &pb.AddRequest{
		OcflPath: ocflRoot,
		ObjectId: "object:1",
		SrcPath:  filepath.ToSlash(srcDir),
		Message:  "first version",
		User: &pb.User{
			Name:    "Alice",
			Address: "mailto:alice@example.org",
		},
	})
	require.NoError(t, err)
	assert.True(t, addResp.GetSuccess())
	assert.Equal(t, "object:1", addResp.GetObjectId())
	assert.Equal(t, "v1", addResp.GetVersion())

	// Test Validate
	valResp, err := client.Validate(ctx, &pb.ValidateRequest{
		OcflPath: ocflRoot,
		ObjectId: "object:1",
	})
	require.NoError(t, err)
	assert.True(t, valResp.GetIsValid())
}
