// Package service implements the pb.GocflServiceServer gRPC service interface.
//
// It provides server-streaming handlers with live log multiplexing for the five core OCFL actions:
//   - Init: Initializes a new OCFL storage root with specified OCFL version, digest algorithm, and extensions.
//   - Add: Ingests an initial version (v1) of an object into an existing storage root.
//   - Update: Ingests a subsequent version (v2, v3, ...) into an existing object.
//   - Create: Combines storage root initialization and initial object ingestion in a single call.
//   - Validate: Validates the conformance of an OCFL storage root or a specific OCFL object according to the OCFL specification.
//
// All operations stream structured Zerolog log entries (as LogEntry payloads) directly to the client
// during execution, concluding with the final operation result message.
//
// The service operates on a virtual filesystem (VFS) abstraction supporting local filesystems,
// zip containers as folders, and remote object storage (S3).
package service
