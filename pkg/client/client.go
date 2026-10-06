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

// GocflClient defines the interface for interacting with the gocfl gRPC service.
type GocflClient interface {
	Init(ctx context.Context, req *pb.InitRequest, opts ...grpc.CallOption) (*pb.InitResponse, error)
	Add(ctx context.Context, req *pb.AddRequest, opts ...grpc.CallOption) (*pb.AddResponse, error)
	Update(ctx context.Context, req *pb.UpdateRequest, opts ...grpc.CallOption) (*pb.UpdateResponse, error)
	Create(ctx context.Context, req *pb.CreateRequest, opts ...grpc.CallOption) (*pb.CreateResponse, error)
	Validate(ctx context.Context, req *pb.ValidateRequest, opts ...grpc.CallOption) (*pb.ValidateResponse, error)
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

// Init initializes an empty OCFL storage root.
func (c *Client) Init(ctx context.Context, req *pb.InitRequest, opts ...grpc.CallOption) (*pb.InitResponse, error) {
	return c.grpcClient.Init(ctx, req, opts...)
}

// Add adds a new object into an existing OCFL structure.
func (c *Client) Add(ctx context.Context, req *pb.AddRequest, opts ...grpc.CallOption) (*pb.AddResponse, error) {
	return c.grpcClient.Add(ctx, req, opts...)
}

// Update adds a new version to an existing object in an OCFL structure.
func (c *Client) Update(ctx context.Context, req *pb.UpdateRequest, opts ...grpc.CallOption) (*pb.UpdateResponse, error) {
	return c.grpcClient.Update(ctx, req, opts...)
}

// Create initializes an OCFL structure and adds an initial object.
func (c *Client) Create(ctx context.Context, req *pb.CreateRequest, opts ...grpc.CallOption) (*pb.CreateResponse, error) {
	return c.grpcClient.Create(ctx, req, opts...)
}

// Validate validates an OCFL storage root or a specific object.
func (c *Client) Validate(ctx context.Context, req *pb.ValidateRequest, opts ...grpc.CallOption) (*pb.ValidateResponse, error) {
	return c.grpcClient.Validate(ctx, req, opts...)
}
