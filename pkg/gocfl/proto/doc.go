// Package proto contains the generated Protocol Buffers and gRPC stubs for gocfl.
//
// The service definition and message types are generated from gocfl.proto:
//   - GocflService: gRPC server-streaming service definition containing Init, Add, Update, Create, and Validate RPC methods.
//   - LogEntry: Structure describing real-time structured log events with timestamp, level, message, and json_raw.
//   - InitRequest / InitResponse / InitResult: Messages for storage root initialization with log streaming.
//   - AddRequest / AddResponse / AddResult: Messages for adding an object with log streaming.
//   - UpdateRequest / UpdateResponse / UpdateResult: Messages for adding a new version with log streaming.
//   - CreateRequest / CreateResponse / CreateResult: Messages for combined root and object creation with log streaming.
//   - ValidateRequest / ValidateResponse / ValidateResult: Messages for validating storage roots and objects with log streaming.
//   - User: Structure describing author/user information (name and address) in OCFL version manifests.
//   - ValidationError: Structure describing individual validation errors/warnings with code, path, and message.
//
// To regenerate the Go code from the proto file, see build.md in this directory.
package proto
