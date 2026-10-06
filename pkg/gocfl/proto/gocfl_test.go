package proto_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type mockGocflServer struct {
	pb.UnimplementedGocflServiceServer
}

func (s *mockGocflServer) Init(req *pb.InitRequest, stream pb.GocflService_InitServer) error {
	_ = stream.Send(&pb.InitResponse{
		Payload: &pb.InitResponse_Log{
			Log: &pb.LogEntry{
				Timestamp: time.Now().UnixNano(),
				Level:     "info",
				Message:   "initializing storage root",
				JsonRaw:   `{"level":"info","message":"initializing storage root"}`,
			},
		},
	})
	return stream.Send(&pb.InitResponse{
		Payload: &pb.InitResponse_Result{
			Result: &pb.InitResult{
				Success: true,
				Message: "initialized " + req.GetOcflPath(),
			},
		},
	})
}

func (s *mockGocflServer) Add(req *pb.AddRequest, stream pb.GocflService_AddServer) error {
	_ = stream.Send(&pb.AddResponse{
		Payload: &pb.AddResponse_Log{
			Log: &pb.LogEntry{
				Timestamp: time.Now().UnixNano(),
				Level:     "info",
				Message:   "adding object " + req.GetObjectId(),
				JsonRaw:   `{"level":"info","message":"adding object"}`,
			},
		},
	})
	return stream.Send(&pb.AddResponse{
		Payload: &pb.AddResponse_Result{
			Result: &pb.AddResult{
				Success:  true,
				Message:  "added object",
				ObjectId: req.GetObjectId(),
				Version:  "v1",
			},
		},
	})
}

func (s *mockGocflServer) Update(req *pb.UpdateRequest, stream pb.GocflService_UpdateServer) error {
	_ = stream.Send(&pb.UpdateResponse{
		Payload: &pb.UpdateResponse_Log{
			Log: &pb.LogEntry{
				Timestamp: time.Now().UnixNano(),
				Level:     "info",
				Message:   "updating object " + req.GetObjectId(),
				JsonRaw:   `{"level":"info","message":"updating object"}`,
			},
		},
	})
	return stream.Send(&pb.UpdateResponse{
		Payload: &pb.UpdateResponse_Result{
			Result: &pb.UpdateResult{
				Success:  true,
				Message:  "updated object",
				ObjectId: req.GetObjectId(),
				Version:  "v2",
			},
		},
	})
}

func (s *mockGocflServer) Create(req *pb.CreateRequest, stream pb.GocflService_CreateServer) error {
	_ = stream.Send(&pb.CreateResponse{
		Payload: &pb.CreateResponse_Log{
			Log: &pb.LogEntry{
				Timestamp: time.Now().UnixNano(),
				Level:     "info",
				Message:   "creating root and object " + req.GetObjectId(),
				JsonRaw:   `{"level":"info","message":"creating root and object"}`,
			},
		},
	})
	return stream.Send(&pb.CreateResponse{
		Payload: &pb.CreateResponse_Result{
			Result: &pb.CreateResult{
				Success:  true,
				Message:  "created ocfl structure and initial object",
				ObjectId: req.GetObjectId(),
				Version:  "v1",
			},
		},
	})
}

func (s *mockGocflServer) Validate(req *pb.ValidateRequest, stream pb.GocflService_ValidateServer) error {
	_ = stream.Send(&pb.ValidateResponse{
		Payload: &pb.ValidateResponse_Log{
			Log: &pb.LogEntry{
				Timestamp: time.Now().UnixNano(),
				Level:     "info",
				Message:   "validating structure",
				JsonRaw:   `{"level":"info","message":"validating structure"}`,
			},
		},
	})
	return stream.Send(&pb.ValidateResponse{
		Payload: &pb.ValidateResponse_Result{
			Result: &pb.ValidateResult{
				IsValid: true,
				Message: "valid ocfl structure",
			},
		},
	})
}

func (s *mockGocflServer) Shutdown(ctx context.Context, req *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	return &pb.ShutdownResponse{
		Success: true,
		Message: "shutdown initiated",
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
	initStream, err := client.Init(ctx, initReq)
	if err != nil {
		t.Fatalf("Init stream failed: %v", err)
	}
	var initLogs []*pb.LogEntry
	var initResult *pb.InitResult
	for {
		resp, err := initStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Init Recv failed: %v", err)
		}
		if l := resp.GetLog(); l != nil {
			initLogs = append(initLogs, l)
		}
		if r := resp.GetResult(); r != nil {
			initResult = r
		}
	}
	if len(initLogs) == 0 {
		t.Errorf("Expected at least 1 log entry from Init")
	}
	if initResult == nil || !initResult.GetSuccess() {
		t.Errorf("Expected Init success, got %+v", initResult)
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
	addStream, err := client.Add(ctx, addReq)
	if err != nil {
		t.Fatalf("Add stream failed: %v", err)
	}
	var addResult *pb.AddResult
	for {
		resp, err := addStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Add Recv failed: %v", err)
		}
		if r := resp.GetResult(); r != nil {
			addResult = r
		}
	}
	if addResult == nil || addResult.GetObjectId() != "urn:test:1" {
		t.Errorf("Expected object ID urn:test:1, got %+v", addResult)
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
	updateStream, err := client.Update(ctx, updateReq)
	if err != nil {
		t.Fatalf("Update stream failed: %v", err)
	}
	var updateResult *pb.UpdateResult
	for {
		resp, err := updateStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Update Recv failed: %v", err)
		}
		if r := resp.GetResult(); r != nil {
			updateResult = r
		}
	}
	if updateResult == nil || updateResult.GetVersion() != "v2" {
		t.Errorf("Expected version v2, got %+v", updateResult)
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
	createStream, err := client.Create(ctx, createReq)
	if err != nil {
		t.Fatalf("Create stream failed: %v", err)
	}
	var createResult *pb.CreateResult
	for {
		resp, err := createStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Create Recv failed: %v", err)
		}
		if r := resp.GetResult(); r != nil {
			createResult = r
		}
	}
	if createResult == nil || !createResult.GetSuccess() {
		t.Errorf("Expected Create success, got %+v", createResult)
	}

	// Test Validate
	valReq := &pb.ValidateRequest{
		OcflPath: "/tmp/test_ocfl",
	}
	valStream, err := client.Validate(ctx, valReq)
	if err != nil {
		t.Fatalf("Validate stream failed: %v", err)
	}
	var valResult *pb.ValidateResult
	for {
		resp, err := valStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Validate Recv failed: %v", err)
		}
		if r := resp.GetResult(); r != nil {
			valResult = r
		}
	}
	if valResult == nil || !valResult.GetIsValid() {
		t.Errorf("Expected IsValid true, got %+v", valResult)
	}

	// Test Shutdown
	shutResp, err := client.Shutdown(ctx, &pb.ShutdownRequest{
		Reason: "test shutdown",
		Force:  false,
	})
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
	if shutResp == nil || !shutResp.GetSuccess() {
		t.Errorf("Expected Shutdown success, got %+v", shutResp)
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
