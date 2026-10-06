package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/rs/zerolog"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// LogHandler is a callback function invoked when a log entry is streamed from the server.
type LogHandler func(*pb.LogEntry)

// CallOption configures individual client operation calls.
type CallOption func(*callOptions)

type callOptions struct {
	logHandler LogHandler
	grpcOpts   []grpc.CallOption
}

// WithCallLogHandler sets a log handler for receiving live server log messages during the call.
func WithCallLogHandler(handler LogHandler) CallOption {
	return func(o *callOptions) {
		o.logHandler = handler
	}
}

// WithGRPCOptions adds grpc.CallOptions to the gRPC stream.
func WithGRPCOptions(opts ...grpc.CallOption) CallOption {
	return func(o *callOptions) {
		o.grpcOpts = append(o.grpcOpts, opts...)
	}
}

// GocflClient defines the interface for interacting with the gocfl gRPC service.
type GocflClient interface {
	Init(ctx context.Context, req *pb.InitRequest, opts ...CallOption) (*pb.InitResult, error)
	Add(ctx context.Context, req *pb.AddRequest, opts ...CallOption) (*pb.AddResult, error)
	Update(ctx context.Context, req *pb.UpdateRequest, opts ...CallOption) (*pb.UpdateResult, error)
	Create(ctx context.Context, req *pb.CreateRequest, opts ...CallOption) (*pb.CreateResult, error)
	Validate(ctx context.Context, req *pb.ValidateRequest, opts ...CallOption) (*pb.ValidateResult, error)
	Shutdown(ctx context.Context, req *pb.ShutdownRequest, opts ...CallOption) (*pb.ShutdownResponse, error)

	InitStream(ctx context.Context, req *pb.InitRequest, opts ...grpc.CallOption) (pb.GocflService_InitClient, error)
	AddStream(ctx context.Context, req *pb.AddRequest, opts ...grpc.CallOption) (pb.GocflService_AddClient, error)
	UpdateStream(ctx context.Context, req *pb.UpdateRequest, opts ...grpc.CallOption) (pb.GocflService_UpdateClient, error)
	CreateStream(ctx context.Context, req *pb.CreateRequest, opts ...grpc.CallOption) (pb.GocflService_CreateClient, error)
	ValidateStream(ctx context.Context, req *pb.ValidateRequest, opts ...grpc.CallOption) (pb.GocflService_ValidateClient, error)

	// Fine-grained Handle Operations
	CloseHandle(ctx context.Context, req *pb.CloseHandleRequest, opts ...CallOption) (*pb.CloseHandleResponse, error)
	KeepAlive(ctx context.Context, req *pb.KeepAliveRequest, opts ...CallOption) (*pb.KeepAliveResponse, error)
	OpenStorageRoot(ctx context.Context, req *pb.OpenStorageRootRequest, opts ...CallOption) (*pb.StorageRootHandle, error)
	InitStorageRootHandle(ctx context.Context, req *pb.InitStorageRootHandleRequest, opts ...CallOption) (*pb.StorageRootHandle, error)
	ListObjects(ctx context.Context, req *pb.ListObjectsRequest, opts ...CallOption) (*pb.ListObjectsResponse, error)
	GetStorageRootDetails(ctx context.Context, req *pb.GetStorageRootDetailsRequest, opts ...CallOption) (*pb.StorageRootDetailsResponse, error)
	OpenObject(ctx context.Context, req *pb.OpenObjectRequest, opts ...CallOption) (*pb.ObjectHandle, error)
	InitObjectHandle(ctx context.Context, req *pb.InitObjectHandleRequest, opts ...CallOption) (*pb.ObjectHandle, error)
	GetInventory(ctx context.Context, req *pb.GetInventoryRequest, opts ...CallOption) (*pb.InventoryResponse, error)
	ValidateObjectHandle(ctx context.Context, req *pb.ValidateObjectHandleRequest, opts ...CallOption) (*pb.ValidateObjectHandleResponse, error)
	BeginUpdate(ctx context.Context, req *pb.BeginUpdateRequest, opts ...CallOption) (*pb.UpdaterHandle, error)
	AddFile(ctx context.Context, req *pb.AddFileRequest, opts ...CallOption) (*pb.AddFileResponse, error)
	AddFolder(ctx context.Context, req *pb.AddFolderRequest, opts ...CallOption) (*pb.AddFolderResponse, error)
	DeleteFile(ctx context.Context, req *pb.DeleteFileRequest, opts ...CallOption) (*pb.DeleteFileResponse, error)
	RenameFile(ctx context.Context, req *pb.RenameFileRequest, opts ...CallOption) (*pb.RenameFileResponse, error)
	CommitUpdate(ctx context.Context, req *pb.CommitUpdateRequest, opts ...CallOption) (*pb.CommitUpdateResponse, error)
	RollbackUpdate(ctx context.Context, req *pb.RollbackUpdateRequest, opts ...CallOption) (*pb.RollbackUpdateResponse, error)

	GRPCClient() pb.GocflServiceClient
	Conn() *grpc.ClientConn
	Close() error
}

// Client represents a gRPC client connection to the gocfl service.
type Client struct {
	conn       *grpc.ClientConn
	grpcClient pb.GocflServiceClient
	logger     zLogger.ZLogger
	closers    []io.Closer
}

// Option configures a Client instance.
type Option func(*options)

type options struct {
	dialOpts []grpc.DialOption
	logger   zLogger.ZLogger
	tlsConf  *tls.Config
	certConf *loader.Config
	insecure bool
}

// WithDialOptions adds custom gRPC dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithLogger sets the logger for the client.
func WithLogger(logger zLogger.ZLogger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// WithTLS configures TLS credentials with the provided TLS configuration.
func WithTLS(tlsConfig *tls.Config) Option {
	return func(o *options) {
		o.tlsConf = tlsConfig
	}
}

// WithCertLoader configures TLS credentials using a certloader Config.
func WithCertLoader(certConf *loader.Config) Option {
	return func(o *options) {
		o.certConf = certConf
	}
}

// WithInsecure specifies that the connection should use insecure transport credentials.
func WithInsecure() Option {
	return func(o *options) {
		o.insecure = true
	}
}

// NewClient creates and connects a new gocfl gRPC Client to the given target address.
func NewClient(target string, opts ...Option) (*Client, error) {
	opt := &options{}
	for _, o := range opts {
		o(opt)
	}

	var closers []io.Closer
	if opt.logger == nil {
		out := zerolog.ConsoleWriter{Out: os.Stderr}
		zlogger := zerolog.New(out).With().Timestamp().Logger()
		opt.logger = &zlogger
	}

	var dialOptions []grpc.DialOption
	dialOptions = append(dialOptions, opt.dialOpts...)

	if opt.certConf != nil {
		tlsConfig, clientLoader, err := loader.CreateClientLoader(opt.certConf, nil)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create certloader client")
		}
		if clientLoader != nil {
			closers = append(closers, clientLoader)
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else if opt.tlsConf != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(opt.tlsConf)))
	} else if opt.insecure {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		for _, c := range closers {
			_ = c.Close()
		}
		return nil, fmt.Errorf("failed to dial target %s: %w", target, err)
	}

	return &Client{
		conn:       conn,
		grpcClient: pb.NewGocflServiceClient(conn),
		logger:     opt.logger,
		closers:    closers,
	}, nil
}

// NewClientFromConn wraps an existing grpc.ClientConn into a Client.
func NewClientFromConn(conn *grpc.ClientConn, logger zLogger.ZLogger) *Client {
	if logger == nil {
		out := zerolog.ConsoleWriter{Out: os.Stderr}
		zlogger := zerolog.New(out).With().Timestamp().Logger()
		logger = &zlogger
	}
	return &Client{
		conn:       conn,
		grpcClient: pb.NewGocflServiceClient(conn),
		logger:     logger,
	}
}

// GRPCClient returns the underlying generated protobuf client.
func (c *Client) GRPCClient() pb.GocflServiceClient {
	return c.grpcClient
}

// Conn returns the underlying grpc.ClientConn.
func (c *Client) Conn() *grpc.ClientConn {
	return c.conn
}

// Close closes the gRPC connection and any associated closers.
func (c *Client) Close() error {
	var errs []error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, cl := range c.closers {
		if err := cl.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Combine(errs...)
	}
	return nil
}

// InitStream initiates the Init stream.
func (c *Client) InitStream(ctx context.Context, req *pb.InitRequest, opts ...grpc.CallOption) (pb.GocflService_InitClient, error) {
	return c.grpcClient.Init(ctx, req, opts...)
}

// Init initializes an empty OCFL storage root, draining logs and returning the final result.
func (c *Client) Init(ctx context.Context, req *pb.InitRequest, opts ...CallOption) (*pb.InitResult, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}

	stream, err := c.grpcClient.Init(ctx, req, co.grpcOpts...)
	if err != nil {
		return nil, err
	}

	var result *pb.InitResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if logEntry := resp.GetLog(); logEntry != nil {
			if co.logHandler != nil {
				co.logHandler(logEntry)
			}
		}
		if res := resp.GetResult(); res != nil {
			result = res
		}
	}
	if result == nil {
		return nil, errors.New("server closed stream without returning a result")
	}
	return result, nil
}

// AddStream initiates the Add stream.
func (c *Client) AddStream(ctx context.Context, req *pb.AddRequest, opts ...grpc.CallOption) (pb.GocflService_AddClient, error) {
	return c.grpcClient.Add(ctx, req, opts...)
}

// Add adds a new object into an existing OCFL structure, draining logs and returning the final result.
func (c *Client) Add(ctx context.Context, req *pb.AddRequest, opts ...CallOption) (*pb.AddResult, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}

	stream, err := c.grpcClient.Add(ctx, req, co.grpcOpts...)
	if err != nil {
		return nil, err
	}

	var result *pb.AddResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if logEntry := resp.GetLog(); logEntry != nil {
			if co.logHandler != nil {
				co.logHandler(logEntry)
			}
		}
		if res := resp.GetResult(); res != nil {
			result = res
		}
	}
	if result == nil {
		return nil, errors.New("server closed stream without returning a result")
	}
	return result, nil
}

// UpdateStream initiates the Update stream.
func (c *Client) UpdateStream(ctx context.Context, req *pb.UpdateRequest, opts ...grpc.CallOption) (pb.GocflService_UpdateClient, error) {
	return c.grpcClient.Update(ctx, req, opts...)
}

// Update adds a new version to an existing object in an OCFL structure, draining logs and returning the final result.
func (c *Client) Update(ctx context.Context, req *pb.UpdateRequest, opts ...CallOption) (*pb.UpdateResult, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}

	stream, err := c.grpcClient.Update(ctx, req, co.grpcOpts...)
	if err != nil {
		return nil, err
	}

	var result *pb.UpdateResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if logEntry := resp.GetLog(); logEntry != nil {
			if co.logHandler != nil {
				co.logHandler(logEntry)
			}
		}
		if res := resp.GetResult(); res != nil {
			result = res
		}
	}
	if result == nil {
		return nil, errors.New("server closed stream without returning a result")
	}
	return result, nil
}

// CreateStream initiates the Create stream.
func (c *Client) CreateStream(ctx context.Context, req *pb.CreateRequest, opts ...grpc.CallOption) (pb.GocflService_CreateClient, error) {
	return c.grpcClient.Create(ctx, req, opts...)
}

// Create initializes an OCFL structure and adds an initial object, draining logs and returning the final result.
func (c *Client) Create(ctx context.Context, req *pb.CreateRequest, opts ...CallOption) (*pb.CreateResult, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}

	stream, err := c.grpcClient.Create(ctx, req, co.grpcOpts...)
	if err != nil {
		return nil, err
	}

	var result *pb.CreateResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if logEntry := resp.GetLog(); logEntry != nil {
			if co.logHandler != nil {
				co.logHandler(logEntry)
			}
		}
		if res := resp.GetResult(); res != nil {
			result = res
		}
	}
	if result == nil {
		return nil, errors.New("server closed stream without returning a result")
	}
	return result, nil
}

// ValidateStream initiates the Validate stream.
func (c *Client) ValidateStream(ctx context.Context, req *pb.ValidateRequest, opts ...grpc.CallOption) (pb.GocflService_ValidateClient, error) {
	return c.grpcClient.Validate(ctx, req, opts...)
}

// Validate validates an OCFL storage root or a specific object, draining logs and returning the final result.
func (c *Client) Validate(ctx context.Context, req *pb.ValidateRequest, opts ...CallOption) (*pb.ValidateResult, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}

	stream, err := c.grpcClient.Validate(ctx, req, co.grpcOpts...)
	if err != nil {
		return nil, err
	}

	var result *pb.ValidateResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if logEntry := resp.GetLog(); logEntry != nil {
			if co.logHandler != nil {
				co.logHandler(logEntry)
			}
		}
		if res := resp.GetResult(); res != nil {
			result = res
		}
	}
	if result == nil {
		return nil, errors.New("server closed stream without returning a result")
	}
	return result, nil
}

// Shutdown initiates a server shutdown over gRPC.
func (c *Client) Shutdown(ctx context.Context, req *pb.ShutdownRequest, opts ...CallOption) (*pb.ShutdownResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}

	return c.grpcClient.Shutdown(ctx, req, co.grpcOpts...)
}

// CloseHandle releases an active StorageRoot, Object, or Updater handle.
func (c *Client) CloseHandle(ctx context.Context, req *pb.CloseHandleRequest, opts ...CallOption) (*pb.CloseHandleResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.CloseHandle(ctx, req, co.grpcOpts...)
}

// KeepAlive renews the lease/TTL of an open handle.
func (c *Client) KeepAlive(ctx context.Context, req *pb.KeepAliveRequest, opts ...CallOption) (*pb.KeepAliveResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.KeepAlive(ctx, req, co.grpcOpts...)
}

// OpenStorageRoot opens an existing OCFL storage root and returns a handle.
func (c *Client) OpenStorageRoot(ctx context.Context, req *pb.OpenStorageRootRequest, opts ...CallOption) (*pb.StorageRootHandle, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.OpenStorageRoot(ctx, req, co.grpcOpts...)
}

// InitStorageRootHandle initializes a storage root and returns an open handle.
func (c *Client) InitStorageRootHandle(ctx context.Context, req *pb.InitStorageRootHandleRequest, opts ...CallOption) (*pb.StorageRootHandle, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.InitStorageRootHandle(ctx, req, co.grpcOpts...)
}

// ListObjects lists object folders within an open storage root.
func (c *Client) ListObjects(ctx context.Context, req *pb.ListObjectsRequest, opts ...CallOption) (*pb.ListObjectsResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.ListObjects(ctx, req, co.grpcOpts...)
}

// GetStorageRootDetails retrieves version and layout details of an open storage root.
func (c *Client) GetStorageRootDetails(ctx context.Context, req *pb.GetStorageRootDetailsRequest, opts ...CallOption) (*pb.StorageRootDetailsResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.GetStorageRootDetails(ctx, req, co.grpcOpts...)
}

// OpenObject opens an existing OCFL object within a storage root or direct path.
func (c *Client) OpenObject(ctx context.Context, req *pb.OpenObjectRequest, opts ...CallOption) (*pb.ObjectHandle, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.OpenObject(ctx, req, co.grpcOpts...)
}

// InitObjectHandle initializes a new OCFL object and returns an open handle.
func (c *Client) InitObjectHandle(ctx context.Context, req *pb.InitObjectHandleRequest, opts ...CallOption) (*pb.ObjectHandle, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.InitObjectHandle(ctx, req, co.grpcOpts...)
}

// GetInventory retrieves the full Inventory snapshot as a Protobuf message.
func (c *Client) GetInventory(ctx context.Context, req *pb.GetInventoryRequest, opts ...CallOption) (*pb.InventoryResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.GetInventory(ctx, req, co.grpcOpts...)
}

// ValidateObjectHandle validates an open OCFL object.
func (c *Client) ValidateObjectHandle(ctx context.Context, req *pb.ValidateObjectHandleRequest, opts ...CallOption) (*pb.ValidateObjectHandleResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.ValidateObjectHandle(ctx, req, co.grpcOpts...)
}

// BeginUpdate starts a new version update session on an open object.
func (c *Client) BeginUpdate(ctx context.Context, req *pb.BeginUpdateRequest, opts ...CallOption) (*pb.UpdaterHandle, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.BeginUpdate(ctx, req, co.grpcOpts...)
}

// AddFile adds a file with direct content bytes into the active version.
func (c *Client) AddFile(ctx context.Context, req *pb.AddFileRequest, opts ...CallOption) (*pb.AddFileResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.AddFile(ctx, req, co.grpcOpts...)
}

// AddFolder adds all files from a VFS path into the active version.
func (c *Client) AddFolder(ctx context.Context, req *pb.AddFolderRequest, opts ...CallOption) (*pb.AddFolderResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.AddFolder(ctx, req, co.grpcOpts...)
}

// DeleteFile removes a file from the active version state.
func (c *Client) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest, opts ...CallOption) (*pb.DeleteFileResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.DeleteFile(ctx, req, co.grpcOpts...)
}

// RenameFile renames a file within the active version state.
func (c *Client) RenameFile(ctx context.Context, req *pb.RenameFileRequest, opts ...CallOption) (*pb.RenameFileResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.RenameFile(ctx, req, co.grpcOpts...)
}

// CommitUpdate commits the version update and updates the object's inventory.
func (c *Client) CommitUpdate(ctx context.Context, req *pb.CommitUpdateRequest, opts ...CallOption) (*pb.CommitUpdateResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.CommitUpdate(ctx, req, co.grpcOpts...)
}

// RollbackUpdate cancels the version update and discards uncommitted changes.
func (c *Client) RollbackUpdate(ctx context.Context, req *pb.RollbackUpdateRequest, opts ...CallOption) (*pb.RollbackUpdateResponse, error) {
	co := &callOptions{}
	for _, o := range opts {
		o(co)
	}
	return c.grpcClient.RollbackUpdate(ctx, req, co.grpcOpts...)
}
