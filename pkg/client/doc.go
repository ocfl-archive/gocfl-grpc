// Package client provides an idiomatic Go client for interacting with the gocfl gRPC service.
//
// The client supports connecting to a gocfl gRPC server over plain TCP or TLS (including
// dynamic TLS certificate management via certloader). It provides convenient wrapper methods
// for all OCFL operations (Init, Add, Update, Create, Validate, and Shutdown) with optional live log streaming
// callbacks, as well as fine-grained handle operations (OpenStorageRoot, OpenObject, GetInventory, BeginUpdate,
// AddFile, AddFolder, CommitUpdate, KeepAlive, CloseHandle).
//
// # Quickstart (Macro Actions)
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
// # Quickstart (Fine-Grained Handle API)
//
//	srHandle, _ := cl.OpenStorageRoot(ctx, &pb.OpenStorageRootRequest{OcflPath: "/path/to/storage_root"})
//	defer cl.CloseHandle(ctx, &pb.CloseHandleRequest{HandleId: srHandle.Id})
//
//	objHandle, _ := cl.OpenObject(ctx, &pb.OpenObjectRequest{StoragerootHandleId: srHandle.Id, ObjectId: "urn:obj:1"})
//	defer cl.CloseHandle(ctx, &pb.CloseHandleRequest{HandleId: objHandle.Id})
//
//	invResp, _ := cl.GetInventory(ctx, &pb.GetInventoryRequest{ObjectHandleId: objHandle.Id})
//	fmt.Println("Head Version:", invResp.Inventory.Head)
package client
