# gocfl gRPC Client (`pkg/client`)

`pkg/client` provides an idiomatic Go client library for communicating with the `gocfl-grpc` service. It simplifies connecting to the gRPC server (supporting plain TCP, standard TLS, and dynamic certificate rotation via `certloader`) and provides both convenient synchronous wrappers with real-time log callbacks and low-level streaming interfaces.

---

## Features

- **Full OCFL Command Coverage**: Supports `Init`, `Add`, `Update`, `Create`, and `Validate`.
- **Live Log Streaming**: Receives structured server-side log events in real time during long-running operations.
- **Multiple Consumption Models**:
  - High-level methods (`cl.Init`, `cl.Add`, ...) with optional `WithCallLogHandler` callbacks.
  - Low-level streaming methods (`cl.InitStream`, `cl.AddStream`, ...) for fine-grained stream and flow control.
- **Flexible Security & Connectivity**: Supports insecure connections, static `tls.Config`, and Basel cloud `certloader`.

---

## Installation & Imports

```go
import (
    "github.com/ocfl-archive/gocfl-grpc/pkg/client"
    pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
)
```

---

## Client Initialization

### 1. Insecure Connection (Local Development)

```go
cl, err := client.NewClient("localhost:50051", client.WithInsecure())
if err != nil {
    log.Fatalf("failed to connect: %v", err)
}
defer cl.Close()
```

### 2. TLS with Static Configuration

```go
tlsConfig := &tls.Config{
    // Custom CA / ServerName / Certificates
}

cl, err := client.NewClient("gocfl.example.com:50051", client.WithTLS(tlsConfig))
if err != nil {
    log.Fatalf("failed to connect: %v", err)
}
defer cl.Close()
```

### 3. Dynamic TLS with `certloader`

```go
certConf := &loader.Config{
    // certloader configuration
}

cl, err := client.NewClient("gocfl.example.com:50051", client.WithCertLoader(certConf))
if err != nil {
    log.Fatalf("failed to connect: %v", err)
}
defer cl.Close()
```

### 4. Custom Client Logger & Dial Options

```go
cl, err := client.NewClient(
    "localhost:50051",
    client.WithInsecure(),
    client.WithLogger(myZLogger),
    client.WithDialOptions(grpc.WithBlock()),
)
```

---

## Logging Architecture & How Logging Works

The server executes OCFL operations (checksum computation, indexing, VFS file operations, validation) and streams all execution logs concurrently to the client.

### Protobuf `LogEntry` Structure

Each log event sent by the server contains:

```protobuf
message LogEntry {
  int64 timestamp = 1; // Unix timestamp in nanoseconds
  string level = 2;    // Log level: "trace", "debug", "info", "warn", "error"
  string message = 3;  // Human-readable message
  string json_raw = 4; // Complete serialized JSON payload from zerolog
}
```

### Logging Option A: High-Level Wrapper with `WithCallLogHandler`

The simplest and recommended approach is to use the synchronous wrapper methods (`Init`, `Add`, `Update`, `Create`, `Validate`) and supply a `WithCallLogHandler` option:

```go
logHandler := func(logEntry *pb.LogEntry) {
    // 1. Simple formatted output
    fmt.Printf("[%s] %s\n", strings.ToUpper(logEntry.Level), logEntry.Message)

    // 2. Or access raw JSON from zerolog directly
    // fmt.Println(logEntry.JsonRaw)
}

result, err := cl.Add(ctx, &pb.AddRequest{
    OcflPath: "/path/to/storage_root",
    ObjectId: "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479",
    SrcPath:  "/path/to/data",
    Message:  "Ingest v1",
}, client.WithCallLogHandler(logHandler))

if err != nil {
    log.Fatalf("Add failed: %v", err)
}

fmt.Printf("Created version: %s for object: %s\n", result.Version, result.ObjectId)
```

### Logging Option B: Direct Stream Processing (Low-Level)

If you need granular control over backpressure, cancellation, or concurrent event handling, use the `*Stream` methods:

```go
stream, err := cl.AddStream(ctx, &pb.AddRequest{
    OcflPath: "/path/to/storage_root",
    ObjectId: "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479",
    SrcPath:  "/path/to/data",
})
if err != nil {
    log.Fatalf("failed to initiate stream: %v", err)
}

var finalResult *pb.AddResult

for {
    resp, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatalf("error receiving from stream: %v", err)
    }

    switch payload := resp.Payload.(type) {
    case *pb.AddResponse_Log:
        logEntry := payload.Log
        fmt.Printf("[%s] (ts=%d) %s\n", logEntry.Level, logEntry.Timestamp, logEntry.Message)
    case *pb.AddResponse_Result:
        finalResult = payload.Result
    }
}

if finalResult != nil {
    fmt.Printf("Success: %v, version: %s\n", finalResult.Success, finalResult.Version)
}
```

---

## Action Reference & Examples

### 1. `Init` — Initialize Storage Root

```go
initResult, err := cl.Init(ctx, &pb.InitRequest{
    OcflPath:    "/path/to/storage_root",
    OcflVersion: "1.1",
    Digest:      "sha512",
}, client.WithCallLogHandler(func(l *pb.LogEntry) {
    log.Printf("[SERVER-LOG] %s", l.Message)
}))
```

### 2. `Add` — Ingest New Object

```go
addResult, err := cl.Add(ctx, &pb.AddRequest{
    OcflPath: "/path/to/storage_root",
    ObjectId: "urn:uuid:4f2081d5-b0b9-4f7b-99d8-9df24fca1a38",
    SrcPath:  "/path/to/content",
    Message:  "Initial ingest",
    User: &pb.User{
        Name:    "Alice Example",
        Address: "alice@example.com",
    },
})
```

### 3. `Update` — Add New Object Version

```go
updateResult, err := cl.Update(ctx, &pb.UpdateRequest{
    OcflPath: "/path/to/storage_root",
    ObjectId: "urn:uuid:4f2081d5-b0b9-4f7b-99d8-9df24fca1a38",
    SrcPath:  "/path/to/updated_content",
    Message:  "Update metadata and contents",
})
```

### 4. `Create` — Initialize Storage Root and Ingest Initial Object

```go
createResult, err := cl.Create(ctx, &pb.CreateRequest{
    OcflPath:    "/path/to/storage_root",
    OcflVersion: "1.1",
    Digest:      "sha512",
    ObjectId:    "urn:uuid:4f2081d5-b0b9-4f7b-99d8-9df24fca1a38",
    SrcPath:     "/path/to/content",
    Message:     "Initial creation",
})
```

### 5. `Validate` — Validate Storage Root or Specific Object

```go
valResult, err := cl.Validate(ctx, &pb.ValidateRequest{
    OcflPath: "/path/to/storage_root",
    ObjectId: "", // Leave empty to validate entire storage root, or specify object ID
}, client.WithCallLogHandler(func(l *pb.LogEntry) {
    if l.Level == "error" || l.Level == "warn" {
        fmt.Printf("[VALIDATION %s] %s\n", strings.ToUpper(l.Level), l.Message)
    }
}))

if !valResult.IsValid {
    fmt.Printf("Validation failed with %d errors:\n", len(valResult.Errors))
    for _, e := range valResult.Errors {
        fmt.Printf(" - [%s] %s (path: %s)\n", e.Code, e.Description, e.Path)
    }
} else {
    fmt.Println("OCFL structure is valid.")
}
```

### 6. `Shutdown` — Gracefully Terminate Server

```go
shutResp, err := cl.Shutdown(ctx, &pb.ShutdownRequest{
    Reason: "remote administrative shutdown",
    Force:  false, // true for immediate termination without waiting for active RPCs
})
if err != nil {
    log.Fatalf("shutdown failed: %v", err)
}
fmt.Printf("Server shutdown response: %s (success=%v)\n", shutResp.Message, shutResp.Success)
```

---

## Fine-Grained Handle Operations (Hybrid Architecture)

In addition to the 1-call macro actions, `gocfl-grpc` supports fine-grained handle-based workflows for stateful, interactive OCFL management without reparsing the storage root or object on every request.

### 1. Opening Storage Roots and Objects

```go
// 1. Open StorageRoot Handle
srHandle, err := cl.OpenStorageRoot(ctx, &pb.OpenStorageRootRequest{
    OcflPath: "/path/to/storage_root",
})
defer cl.CloseHandle(ctx, &pb.CloseHandleRequest{HandleId: srHandle.Id})

// 2. Open Object Handle within the StorageRoot
objHandle, err := cl.OpenObject(ctx, &pb.OpenObjectRequest{
    StoragerootHandleId: srHandle.Id,
    ObjectId:            "urn:archive:my-object-1",
})
defer cl.CloseHandle(ctx, &pb.CloseHandleRequest{HandleId: objHandle.Id})
```

### 2. Inspecting the Inventory (Protobuf Data Object)

The full inventory is retrieved as an immutable protobuf snapshot without roundtrip latency:

```go
invResp, err := cl.GetInventory(ctx, &pb.GetInventoryRequest{
    ObjectHandleId: objHandle.Id,
})
if err != nil {
    log.Fatalf("failed to get inventory: %v", err)
}

inv := invResp.Inventory
fmt.Printf("Object: %s, Head: %s, Spec: %s\n", inv.Id, inv.Head, inv.Type)

// Iterate manifest files
for digest, paths := range inv.Manifest {
    fmt.Printf("Digest %s -> %v\n", digest, paths.Paths)
}

// Iterate versions history
for verNum, ver := range inv.Versions {
    fmt.Printf("Version %s: created at %s, message: %q, author: %s\n",
        verNum, ver.Created, ver.Message, ver.User.GetName())
}
```

### 3. Interactive Version Updating (`UpdaterHandle`)

```go
// 1. Begin version update
updHandle, err := cl.BeginUpdate(ctx, &pb.BeginUpdateRequest{
    ObjectHandleId: objHandle.Id,
    Message:        "Add quarterly data and document",
    User: &pb.User{
        Name:    "Alice Archivist",
        Address: "mailto:alice@example.org",
    },
})

// 2. Add files directly via VFS path (zero bytes in payload, fully streamed by server)
_, err = cl.AddFile(ctx, &pb.AddFileRequest{
    UpdaterHandleId: updHandle.Id,
    SrcPath:         "source_data/quarterly.csv", // path in VFS
    DestPath:        "data/quarterly.csv",        // logical path in OCFL version
})

// Or add file content directly via in-memory bytes if needed
_, err = cl.AddFile(ctx, &pb.AddFileRequest{
    UpdaterHandleId: updHandle.Id,
    Path:            "data/notes.txt",
    Content:         []byte("Additional notes\n"),
})

// 3. Add whole folders from server VFS if needed
_, err = cl.AddFolder(ctx, &pb.AddFolderRequest{
    UpdaterHandleId: updHandle.Id,
    SrcPath:         "/mnt/staging/extra_docs",
})

// 4. Rename or delete existing files in state
_, err = cl.RenameFile(ctx, &pb.RenameFileRequest{
    UpdaterHandleId: updHandle.Id,
    SourcePath:      "data/old_name.csv",
    DestPath:        "data/new_name.csv",
})

// 5. Commit the update (atomically writes new version and updates inventory)
commitResp, err := cl.CommitUpdate(ctx, &pb.CommitUpdateRequest{
    UpdaterHandleId: updHandle.Id,
})
fmt.Printf("Committed version %s. New head: %s\n", commitResp.HeadVersion, commitResp.NewInventory.Head)
```

### 4. Lease & TTL Management (`KeepAlive`)

Handles are automatically evicted on the server after the TTL (default 15 minutes) if unused. Long-running clients can refresh their leases at any time:

```go
keepResp, err := cl.KeepAlive(ctx, &pb.KeepAliveRequest{
    HandleId:      srHandle.Id,
    ExtendSeconds: 1800, // extend by 30 minutes
})
```

---

## Connection Lifecycle

Always ensure client connections are properly closed when shutting down:

```go
cl, err := client.NewClient("localhost:50051", client.WithInsecure())
if err != nil {
    // handle error
}
defer cl.Close()
```

If you need direct access to the underlying `*grpc.ClientConn` or the raw protobuf service client:

```go
conn := cl.Conn()
rawClient := cl.GRPCClient()
```
