// Package proto contains the generated Protocol Buffers and gRPC stubs for gocfl.
//
// The service definition and message types are generated from gocfl.proto:
//   - GocflService: gRPC service definition containing Init, Add, Update, Create, and Validate RPC methods.
//   - InitRequest / InitResponse: Request and response messages for storage root initialization.
//   - AddRequest / AddResponse: Request and response messages for adding an object.
//   - UpdateRequest / UpdateResponse: Request and response messages for adding a new version to an object.
//   - CreateRequest / CreateResponse: Request and response messages for combined storage root and object creation.
//   - ValidateRequest / ValidateResponse: Request and response messages for validating storage roots and objects.
//   - User: Structure describing author/user information (name and address) in OCFL version manifests.
//   - ValidationError: Structure describing individual validation errors/warnings with code, path, and message.
//
// To regenerate the Go code from the proto file, see build.md in this directory.
package proto
