# gocfl-grpc

`gocfl-grpc` is a high-performance gRPC microservice and Go client library for the [Oxford Common File Layout (OCFL)](https://ocfl.io/) specification (v1.0 and v1.1). Built on top of [`gocfl`](https://github.com/ocfl-archive/gocfl), it provides remote API access for creating, updating, and validating digital preservation storage roots and objects.

---

## Features

- **Standard OCFL Actions over gRPC with Live Log Streaming**:
  - `Init`: Initialize a new OCFL storage root with configurable layout, digest algorithm, and extensions.
  - `Add`: Ingest an initial object version (`v1`) with metadata and user tracking.
  - `Update`: Append subsequent versions (`v2`, `v3`, ...) to an existing object with deduplication and area mapping.
  - `Create`: Initialize a storage root and ingest an initial object in a single operation.
  - `Validate`: Perform full conformance validation of storage roots or individual objects according to OCFL specs.
  - **Real-Time Log Streaming**: All RPCs stream structured log entries (`LogEntry` with timestamp, level, message, raw JSON) directly to the client as events occur on the server.
- **Virtual Filesystem (VFS) Abstraction**:
  - Supports local filesystems, zip containers as folders, and remote S3 object stores.
  - KeePass2 and KMS secret resolution for cloud credentials.
- **Built-in OCFL Extensions**:
  - Siegfried / PRONOM format indexer (`ext_NNNN_indexer`).
  - Thumbnail generation (`ext_NNNN_thumbnail`).
  - Content migration (`ext_NNNN_migration`).
  - Metafile management (`ext_NNNN_metafile`).
- **Idiomatic Go Client Library (`pkg/client`)**:
  - Synchronous convenience methods with optional live log callbacks (`client.WithCallLogHandler(...)`).
  - Raw streaming methods (`InitStream`, `AddStream`, `UpdateStream`, `CreateStream`, `ValidateStream`).
  - Fluent connection options (`WithInsecure()`, `WithTLS()`, `WithCertLoader()`, `WithLogger()`).
- **Production-Ready Server (`cmd/gocflgrpc`)**:
  - CLI flags, TOML configuration loading, gRPC reflection, and graceful shutdown handling.
  - Structured multi-target logging (Console, File, Logstash with TLS).

---

## Package Architecture

```
gocfl-grpc/
├── cmd/
│   └── gocflgrpc/         # Server executable entry point (CLI & daemon)
├── pkg/
│   ├── client/            # Go gRPC client library (GocflClient interface)
│   └── gocfl/
│       ├── bootstrap/     # Server setup, VFS mounting, logger & config loading
│       ├── proto/         # Protobuf definitions (gocfl.proto) & generated stubs
│       └── service/       # Core gRPC service implementation (GocflService)
└── test/                  # End-to-end integration tests (Client <-> Server over TCP)
```

| Package | Description |
|---|---|
| `cmd/gocflgrpc` | Main binary that loads configuration, boots up the VFS and extensions, and starts the gRPC server. |
| `pkg/client` | High-level Go client providing method wrappers for `Init`, `Add`, `Update`, `Create`, and `Validate`. |
| `pkg/gocfl/bootstrap` | Config decoding (TOML), logger initialization, VFS setup, extension registration, and server lifecycle management. |
| `pkg/gocfl/proto` | Protobuf schemas (`gocfl.proto`), generated Go types (`gocfl.pb.go`), and gRPC service definitions (`gocfl_grpc.pb.go`). |
| `pkg/gocfl/service` | gRPC service implementation binding `pb.GocflServiceServer` with `gocfl` core functions. |
| `test` | End-to-end integration tests verifying concurrent execution, multi-version workflows, and error handling. |

---

## Installation & Build

### Prerequisites
- Go 1.23 or higher
- Protoc compiler and plugins (only if modifying `.proto` files):
  - `protoc`
  - `protoc-gen-go`
  - `protoc-gen-go-grpc`

### Build Binary
To build the server binary:

```bash
go build -o gocflgrpc ./cmd/gocflgrpc
```

---

## Running the Server

### Command-Line Flags

```bash
./gocflgrpc -addr :50051 -log-level INFO
```

Available flags:
- `-addr`: gRPC server address to listen on (e.g. `:50051`, `127.0.0.1:50051`). Default is `:50051`.
- `-config`: Path to a TOML configuration file.
- `-log-level`: Logging level (`CRITICAL`, `ERROR`, `WARNING`, `NOTICE`, `INFO`, `DEBUG`).
- `-log-file`: Path to write log output (default is console/stderr).
- `-allow-api-shutdown`: Enable remote server termination via the gRPC `Shutdown` API (default is `false`).


### Configuration File (TOML)

You can provide a TOML configuration file via `-config`:

```toml
addr = ":50051"
allow_api_shutdown = false


[log]
level = "INFO"
file = ""

[vfs]
# Additional virtual filesystem mappings
```

---

## Using the Go Client Library

Add the module to your Go project:

```bash
go get github.com/ocfl-archive/gocfl-grpc
```

### Quickstart Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ocfl-archive/gocfl-grpc/pkg/client"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
)

func main() {
	// Connect to gRPC server (insecure or with TLS)
	cl, err := client.NewClient("127.0.0.1:50051", client.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ocflRoot := "/data/ocfl_storage"
	objectID := "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479"

	// 1. Initialize Storage Root
	initResp, err := cl.Init(ctx, &pb.InitRequest{
		OcflPath:    ocflRoot,
		OcflVersion: "1.1",
		Digest:      "sha512",
	})
	if err != nil {
		log.Fatalf("Init failed: %v", err)
	}
	fmt.Printf("Init: %s\n", initResp.GetMessage())

	// 2. Add Initial Version (v1) of an Object
	addResp, err := cl.Add(ctx, &pb.AddRequest{
		OcflPath: ocflRoot,
		ObjectId: objectID,
		SrcPath:  "/data/source_content_v1",
		Message:  "Initial ingestion of object",
		User: &pb.User{
			Name:    "Alice Doe",
			Address: "mailto:alice@example.org",
		},
	})
	if err != nil {
		log.Fatalf("Add failed: %v", err)
	}
	fmt.Printf("Added: %s (version: %s)\n", addResp.GetObjectId(), addResp.GetVersion())

	// 3. Update Object to Version (v2)
	updResp, err := cl.Update(ctx, &pb.UpdateRequest{
		OcflPath: ocflRoot,
		ObjectId: objectID,
		SrcPath:  "/data/source_content_v2",
		Message:  "Updated files",
		User: &pb.User{
			Name:    "Alice Doe",
			Address: "mailto:alice@example.org",
		},
	})
	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Printf("Updated: %s (new version: %s)\n", updResp.GetObjectId(), updResp.GetVersion())

	// 4. Validate Object
	valResp, err := cl.Validate(ctx, &pb.ValidateRequest{
		OcflPath: ocflRoot,
		ObjectId: objectID,
	})
	if err != nil {
		log.Fatalf("Validate failed: %v", err)
	}
	fmt.Printf("Validation result: isValid=%v, message=%s\n", valResp.GetIsValid(), valResp.GetMessage())
}
```

### Client Options

| Option | Description |
|---|---|
| `client.WithInsecure()` | Disables transport security (useful for local development or within trusted private networks). |
| `client.WithTLS(tlsConfig)` | Uses custom `*tls.Config` for secure gRPC communication. |
| `client.WithCertLoader(config)` | Uses dynamic certificate reloading via `go.ub.unibas.ch/cloud/certloader/v2`. |
| `client.WithLogger(logger)` | Attaches a `zLogger.ZLogger` instance for client-side logging. |
| `client.WithDialOptions(opts...)` | Passes custom `grpc.DialOption` instances to the underlying gRPC connection. |

---

## gRPC Protocol Reference

The gRPC service `GocflService` provides five server-streaming RPC methods:

```protobuf
service GocflService {
  rpc Init (InitRequest) returns (stream InitResponse);
  rpc Add (AddRequest) returns (stream AddResponse);
  rpc Update (UpdateRequest) returns (stream UpdateResponse);
  rpc Create (CreateRequest) returns (stream CreateResponse);
  rpc Validate (ValidateRequest) returns (stream ValidateResponse);
}
```

### Log Streaming & Message Structure

Each streamed response message is a `oneof` payload containing either a real-time `LogEntry` or the final operation result:

```protobuf
message LogEntry {
  int64 timestamp = 1;
  string level = 2;
  string message = 3;
  string json_raw = 4;
}
```

### Methods Summary

#### Macro Operations (1-Call Actions)
- `Init`: Initializes a new OCFL storage root with live log streaming.
- `Add`: Ingests an initial object version (`v1`) into an existing storage root.
- `Update`: Creates a subsequent version for an existing object with deduplication and area support.
- `Create`: Initializes a storage root and adds an initial object in one operation.
- `Validate`: Conformance validation of storage roots or objects according to OCFL specs.
- `Shutdown`: Remote graceful or immediate shutdown of the gRPC server process.

#### Fine-Grained Handle Operations
- `OpenStorageRoot` / `InitStorageRootHandle`: Opens/Initializes a storage root and returns an opaque `StorageRootHandle`.
- `ListObjects` / `GetStorageRootDetails`: Queries object folders and storage root layout configuration.
- `OpenObject` / `InitObjectHandle`: Opens/Initializes an OCFL object within a storage root or direct path and returns an `ObjectHandle`.
- `GetInventory`: Retrieves the full immutable `Inventory` protobuf snapshot (`manifest`, `versions`, `state`, `fixity`, `head`, `spec`).
- `ValidateObjectHandle`: Validates an open object handle without re-opening filesystems.
- `BeginUpdate`: Starts a new staging version update session returning an `UpdaterHandle`.
- `AddFile` / `AddFolder`: Adds files (direct byte payload or VFS path) to the active version.
- `RenameFile` / `DeleteFile`: Modifies the logical file state of the active version.
- `CommitUpdate`: Atomically writes the new version and updates the object inventory.
- `RollbackUpdate`: Cancels the staging update and cleans up resources.
- `KeepAlive` / `CloseHandle`: Leases management, TTL renewal, and resource release.

---

## Development & Testing

### Running Tests

Run all unit and integration tests across the repository:

```bash
go test -v ./...
```

Run only end-to-end integration and load tests:

```bash
go test -v ./test/...
```

### Running Load Tests & Benchmarks

Run high-concurrency load tests (starts the gRPC server once and executes concurrent batches of `Add`, `Validate`, `Update`, and log streaming calls):

```bash
go test -v -run TestLoadHighConcurrency ./test/...
```

Run Go performance benchmarks:

```bash
go test -bench=BenchmarkGRPCService -benchmem -run=^$ ./test/...
```

### Regenerating Protobuf Stubs

If you update `pkg/gocfl/proto/gocfl.proto`, recompile the Go stubs with:

```bash
cd pkg/gocfl/proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       gocfl.proto
```

---

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
