// Package client provides an idiomatic Go client for interacting with the gocfl gRPC service.
//
// The client supports connecting to a gocfl gRPC server over plain TCP or TLS (including
// dynamic TLS certificate management via certloader). It provides convenient wrapper methods
// for all OCFL operations (Init, Add, Update, Create, Validate, and Shutdown) with optional live log streaming
// callbacks, as well as direct access to the underlying gRPC client streams and connection.
//
// # Quickstart
//
//	cl, err := client.NewClient("localhost:50051", client.WithInsecure())
//	if err != nil {
//	    log.Fatalf("failed to connect: %v", err)
//	}
//	defer cl.Close()
//
//	// Initialize an OCFL storage root with live log callback
//	resp, err := cl.Init(ctx, &pb.InitRequest{
//	    OcflPath:    "/path/to/storage_root",
//	    OcflVersion: "1.1",
//	    Digest:      "sha512",
//	}, client.WithCallLogHandler(func(log *pb.LogEntry) {
//	    fmt.Printf("[%s] %s\n", log.Level, log.Message)
//	}))
//
//	// Add a new object
//	addResp, err := cl.Add(ctx, &pb.AddRequest{
//	    OcflPath: "/path/to/storage_root",
//	    ObjectId: "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479",
//	    SrcPath:  "/path/to/source_folder",
//	    Message:  "Initial object version",
//	})
//
//	// Validate the storage root or object
//	valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
//	    OcflPath: "/path/to/storage_root",
//	})
package client
