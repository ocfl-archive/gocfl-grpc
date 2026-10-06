package bootstrap

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/keepass2kms"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/filesystem/pkg/vfsrw"
	"github.com/ocfl-archive/gocfl-cli/config"
	defaultObjectExtensions "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultStorageRootExtensions "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/service"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	indexerutil "github.com/ocfl-archive/indexer/v3/pkg/util"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// DefaultServerAddr is the default address the gRPC server listens on.
const DefaultServerAddr = ":50051"

// Config extends the base gocfl configuration with gRPC server settings.
type Config struct {
	config.GOCFLConfig
	Addr             string `toml:"addr"`
	AllowAPIShutdown bool   `toml:"allow_api_shutdown"`
}

// LoadConfig loads the configuration from a TOML file or falls back to embedded defaults.
func LoadConfig(configFile string) (*Config, error) {
	baseConf, err := config.LoadGOCFLConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load base gocfl config: %w", err)
	}

	cfg := &Config{
		GOCFLConfig: *baseConf,
		Addr:        DefaultServerAddr,
	}

	if configFile != "" && configFile != "internal" {
		if _, err := toml.DecodeFile(configFile, cfg); err != nil {
			return nil, fmt.Errorf("failed to decode config file '%s': %w", configFile, err)
		}
	}

	if cfg.Addr == "" {
		cfg.Addr = DefaultServerAddr
	}

	return cfg, nil
}

// SetupLogger initializes the zerolog logger with optional UbMultiLogger, logfile, and logstash targets.
func SetupLogger(conf *config.GOCFLConfig) (zLogger.ZLogger, []io.Closer, error) {
	var closers []io.Closer
	hostname, err := os.Hostname()
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot get hostname")
	}

	var loggerTLSConfig *tls.Config
	if conf.Log.Stash.TLS != nil {
		var loggerLoader io.Closer
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			return nil, nil, errors.Wrap(err, "cannot create client loader")
		}
		closers = append(closers, loggerLoader)
	}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create logger")
	}
	if _logstash != nil {
		closers = append(closers, _logstash)
	}
	if _logfile != nil {
		closers = append(closers, _logfile)
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Logger()
	return &l2, closers, nil
}

// GetLocalFSConfig returns the platform-specific partition mappings for the VFS.
func GetLocalFSConfig() map[string]*vfsrw.VFS {
	var result = map[string]*vfsrw.VFS{}
	if runtime.GOOS == "windows" {
		partitions, _ := disk.Partitions(false)
		for _, partition := range partitions {
			if len(partition.Mountpoint) < 2 || partition.Mountpoint[1] != ':' {
				continue
			}
			result[strings.ToLower(partition.Mountpoint[:1])] = &vfsrw.VFS{
				Name:        strings.ToLower(partition.Mountpoint[:1]),
				Type:        "os",
				ReadOnly:    false,
				ZipAsFolder: nil,
				OS: &vfsrw.OS{
					BaseDir: partition.Mountpoint + "/",
				},
			}
		}
	} else {
		result["root"] = &vfsrw.VFS{
			Name:        "root",
			Type:        "os",
			ReadOnly:    false,
			ZipAsFolder: nil,
			OS: &vfsrw.OS{
				BaseDir: "/",
			},
		}
	}
	return result
}

// SetupVFS initializes the virtual filesystem (VFS) with local and cloud storage mappings.
func SetupVFS(conf *config.GOCFLConfig, logger zLogger.ZLogger) (vfsrw.VFSRW, error) {
	if conf.VFS == nil {
		conf.VFS = vfsrw.Config{}
	}
	for name, val := range GetLocalFSConfig() {
		conf.VFS[name] = val
	}

	if conf.AES.Enable {
		db, err := keepass2kms.LoadKeePassDBFromFile(string(conf.AES.KeepassFile), string(conf.AES.KeepassKey))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot load keepass file '%s'", conf.AES.KeepassFile)
		}
		client, err := keepass2kms.NewClient(db, filepath.Base(string(conf.AES.KeepassFile)))
		if err != nil {
			return nil, errors.Wrap(err, "cannot create keepass2kms client")
		}
		registry.RegisterKMSClient(client)
	}

	vfs, err := vfsrw.NewFS(conf.VFS, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create VFS")
	}

	if err := vfsrw.AddLocal(vfs, &vfsrw.ZipAsFolder{
		Enabled:   true,
		Digests:   []checksum.DigestAlgorithm{checksum.DigestSHA512},
		CacheSize: 3,
		Compress:  false,
		ReadOnly:  false,
	}); err != nil {
		return nil, errors.Wrap(err, "cannot add local VFS")
	}

	return vfs, nil
}

// InitExtensions initializes and registers all OCFL extensions with runtime configuration.
func InitExtensions(conf *config.GOCFLConfig, vfs vfsrw.VFSRW, logger zLogger.ZLogger) error {
	if conf.Autoconfig && conf.Thumbnail != nil {
		if _, err := ext_NNNN_thumbnail.Autoconfig(conf.Thumbnail, map[string]string{}, logger); err != nil {
			logger.Warn().Err(err).Msg("failed to autoconfigure thumbnail extension")
		}
	}

	if conf.Indexer != nil && conf.Indexer.Optimize {
		if _, err := indexerutil.OptimizeConfig(conf.Indexer, logger); err != nil {
			logger.Warn().Err(err).Msg("failed to optimize indexer config")
		}
	}

	ocflLogger := ocfl.NewOCFLLogger(context.Background(), logger, nil, version.Default, nil)
	ext_NNNN_migration.Init(&conf.Migration, nil, ocflLogger)
	ext_NNNN_thumbnail.Init(conf.Thumbnail, nil, ocflLogger)
	ext_NNNN_indexer.Init(conf.Indexer, false, ocflLogger)
	ext_NNNN_metafile.Init(vfs, ocflLogger)

	return nil
}

// Server encapsulates the running gRPC server and its resources.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	service    *service.GocflService
	closers    []io.Closer
	logger     zLogger.ZLogger
	config     *Config
	stopOnce   sync.Once
	doneChan   chan struct{}
}

// NewServer bootstraps all components and constructs a ready-to-run gRPC Server.
func NewServer(cfg *Config, serverOpts ...grpc.ServerOption) (*Server, error) {
	if cfg == nil {
		var err error
		cfg, err = LoadConfig("")
		if err != nil {
			return nil, err
		}
	}

	logger, closers, err := SetupLogger(&cfg.GOCFLConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to setup logger: %w", err)
	}

	vfs, err := SetupVFS(&cfg.GOCFLConfig, logger)
	if err != nil {
		for _, c := range closers {
			_ = c.Close()
		}
		return nil, fmt.Errorf("failed to setup vfs: %w", err)
	}

	if err := InitExtensions(&cfg.GOCFLConfig, vfs, logger); err != nil {
		for _, c := range closers {
			_ = c.Close()
		}
		return nil, fmt.Errorf("failed to init extensions: %w", err)
	}

	svc, err := service.NewGocflService(
		service.WithConfig(&cfg.GOCFLConfig),
		service.WithVFS(vfs),
		service.WithLogger(logger),
		service.WithAllowAPIShutdown(cfg.AllowAPIShutdown),
		service.WithDefaultStorageRootExtensionFS(defaultStorageRootExtensions.DefaultStorageRootExtensionFS),
		service.WithDefaultObjectExtensionFS(defaultObjectExtensions.DefaultObjectExtensionFS),
	)
	if err != nil {
		for _, c := range closers {
			_ = c.Close()
		}
		return nil, fmt.Errorf("failed to create gocfl service: %w", err)
	}

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		for _, c := range closers {
			_ = c.Close()
		}
		return nil, fmt.Errorf("failed to listen on %s: %w", cfg.Addr, err)
	}

	grpcServer := grpc.NewServer(serverOpts...)
	pb.RegisterGocflServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	srv := &Server{
		grpcServer: grpcServer,
		listener:   lis,
		service:    svc,
		closers:    closers,
		logger:     logger,
		config:     cfg,
		doneChan:   make(chan struct{}),
	}

	svc.SetShutdownFunc(func(force bool) {
		if force {
			srv.Stop()
		} else {
			srv.GracefulStop()
		}
	})

	return srv, nil
}

// Addr returns the actual listening network address.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.Addr
}

// Done returns a channel that is closed when the server has stopped.
func (s *Server) Done() <-chan struct{} {
	return s.doneChan
}

// Serve starts accepting incoming gRPC connections.
func (s *Server) Serve() error {
	s.logger.Info().Msgf("gocfl gRPC server listening on %s", s.Addr())
	err := s.grpcServer.Serve(s.listener)
	s.stopOnce.Do(func() {
		for _, c := range s.closers {
			_ = c.Close()
		}
		close(s.doneChan)
	})
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// GracefulStop gracefully stops the gRPC server and cleans up resources.
func (s *Server) GracefulStop() {
	s.stopOnce.Do(func() {
		s.logger.Info().Msg("shutting down gocfl gRPC server...")
		s.grpcServer.GracefulStop()
		for _, c := range s.closers {
			_ = c.Close()
		}
		close(s.doneChan)
	})
}

// Stop immediately stops the gRPC server and cleans up resources.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.logger.Info().Msg("immediately stopping gocfl gRPC server...")
		s.grpcServer.Stop()
		for _, c := range s.closers {
			_ = c.Close()
		}
		close(s.doneChan)
	})
}
