package proto_test

import (
	"context"
	"net"
	"testing"

	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type mockGocflServer struct {
	pb.UnimplementedGocflServiceServer
}

func (s *mockGocflServer) Init(ctx context.Context, req *pb.InitRequest) (*pb.InitResponse, error) {
	return &pb.InitResponse{
		Success: true,
		Message: "initialized " + req.GetOcflPath(),
	}, nil
}

func (s *mockGocflServer) Add(ctx context.Context, req *pb.AddRequest) (*pb.AddResponse, error) {
	return &pb.AddResponse{
		Success:  true,
		Message:  "added object",
		ObjectId: req.GetObjectId(),
		Version:  "v1",
	}, nil
}

func (s *mockGocflServer) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	return &pb.UpdateResponse{
		Success:  true,
		Message:  "updated object",
		ObjectId: req.GetObjectId(),
		Version:  "v2",
	}, nil
}

func (s *mockGocflServer) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateResponse, error) {
	return &pb.CreateResponse{
		Success:  true,
		Message:  "created ocfl structure and initial object",
		ObjectId: req.GetObjectId(),
		Version:  "v1",
	}, nil
}

func (s *mockGocflServer) Validate(ctx context.Context, req *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	return &pb.ValidateResponse{
		IsValid: true,
		Message: "valid ocfl structure",
	}, nil
}

func TestGocflGRPCService(t *testing.T) {
	bufferSize := 1024 * 1024
	lis := bufconn.Listen(bufferSize)

	server := grpc.NewServer()
	pb.RegisterGocflServiceServer(server, &mockGocflServer{})

	go func() {
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Server error: %v", err)
		}
	}()
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewGocflServiceClient(conn)
	ctx := context.Background()

	// Test Init
	initReq := &pb.InitRequest{
		OcflPath:    "/tmp/test_ocfl",
		OcflVersion: "1.1",
		Digest:      "sha512",
	}
	initResp, err := client.Init(ctx, initReq)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if !initResp.GetSuccess() {
		t.Errorf("Expected Init success, got false")
	}

	// Test Add
	addReq := &pb.AddRequest{
		OcflPath: "/tmp/test_ocfl",
		SrcPath:  "/tmp/src",
		ObjectId: "urn:test:1",
		Message:  "initial add",
		User: &pb.User{
			Name:    "Jane Doe",
			Address: "mailto:jane@example.com",
		},
	}
	addResp, err := client.Add(ctx, addReq)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if addResp.GetObjectId() != "urn:test:1" {
		t.Errorf("Expected object ID urn:test:1, got %s", addResp.GetObjectId())
	}

	// Test Update
	updateReq := &pb.UpdateRequest{
		OcflPath: "/tmp/test_ocfl",
		SrcPath:  "/tmp/src_v2",
		ObjectId: "urn:test:1",
		Message:  "update version",
		User: &pb.User{
			Name:    "Jane Doe",
			Address: "mailto:jane@example.com",
		},
	}
	updateResp, err := client.Update(ctx, updateReq)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updateResp.GetVersion() != "v2" {
		t.Errorf("Expected version v2, got %s", updateResp.GetVersion())
	}

	// Test Create
	createReq := &pb.CreateRequest{
		OcflPath:    "/tmp/test_ocfl2",
		SrcPath:     "/tmp/src",
		ObjectId:    "urn:test:2",
		Message:     "create new ocfl",
		OcflVersion: "1.1",
		User: &pb.User{
			Name:    "Jane Doe",
			Address: "mailto:jane@example.com",
		},
	}
	createResp, err := client.Create(ctx, createReq)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !createResp.GetSuccess() {
		t.Errorf("Expected Create success, got false")
	}

	// Test Validate
	valReq := &pb.ValidateRequest{
		OcflPath: "/tmp/test_ocfl",
	}
	valResp, err := client.Validate(ctx, valReq)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if !valResp.GetIsValid() {
		t.Errorf("Expected IsValid true, got false")
	}

	// Verify Protobuf Marshalling
	data, err := proto.Marshal(createReq)
	if err != nil {
		t.Fatalf("proto.Marshal failed: %v", err)
	}
	var unmarshaled pb.CreateRequest
	if err := proto.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}
	if unmarshaled.GetObjectId() != createReq.GetObjectId() {
		t.Errorf("Unmarshaled object id mismatch: got %s, want %s", unmarshaled.GetObjectId(), createReq.GetObjectId())
	}
}
