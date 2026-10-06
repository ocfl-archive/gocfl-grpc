package service

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/vfsrw"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-cli/config"
	defaultObjectExtensions "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultStorageRootExtensions "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/util"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	indexerutil "github.com/ocfl-archive/indexer/v3/pkg/util"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GocflService implements the pb.GocflServiceServer gRPC interface.
type GocflService struct {
	pb.UnimplementedGocflServiceServer
	vfs                           vfsrw.VFSRW
	logger                        zLogger.ZLogger
	conf                          *config.GOCFLConfig
	defaultStorageRootExtensionFS fs.FS
	defaultObjectExtensionFS      fs.FS
}

// Option configures a GocflService instance.
type Option func(*GocflService)

// WithVFS sets a custom virtual filesystem.
func WithVFS(vfs vfsrw.VFSRW) Option {
	return func(s *GocflService) {
		s.vfs = vfs
	}
}

// WithLogger sets a custom logger.
func WithLogger(logger zLogger.ZLogger) Option {
	return func(s *GocflService) {
		s.logger = logger
	}
}

// WithConfig sets the GOCFLConfig configuration.
func WithConfig(conf *config.GOCFLConfig) Option {
	return func(s *GocflService) {
		s.conf = conf
	}
}

// WithDefaultStorageRootExtensionFS sets a custom default storage root extension filesystem.
func WithDefaultStorageRootExtensionFS(fsys fs.FS) Option {
	return func(s *GocflService) {
		s.defaultStorageRootExtensionFS = fsys
	}
}

// WithDefaultObjectExtensionFS sets a custom default object extension filesystem.
func WithDefaultObjectExtensionFS(fsys fs.FS) Option {
	return func(s *GocflService) {
		s.defaultObjectExtensionFS = fsys
	}
}

// NewGocflService creates a new GocflService instance with the provided options.
func NewGocflService(opts ...Option) (*GocflService, error) {
	s := &GocflService{}
	for _, opt := range opts {
		opt(s)
	}

	if s.defaultStorageRootExtensionFS == nil {
		s.defaultStorageRootExtensionFS = defaultStorageRootExtensions.DefaultStorageRootExtensionFS
	}

	if s.defaultObjectExtensionFS == nil {
		s.defaultObjectExtensionFS = defaultObjectExtensions.DefaultObjectExtensionFS
	}

	if s.logger == nil {
		out := zerolog.ConsoleWriter{Out: os.Stderr}
		zlogger := zerolog.New(out).With().Timestamp().Logger()
		s.logger = &zlogger
	}

	if s.conf == nil {
		var err error
		s.conf, err = config.LoadGOCFLConfig("")
		if err != nil {
			return nil, fmt.Errorf("failed to load default config: %w", err)
		}
	}

	if s.vfs == nil {
		cfg := s.conf.VFS
		if cfg == nil {
			cfg = vfsrw.Config{}
		}
		vfs, err := vfsrw.NewFS(cfg, s.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create vfs: %w", err)
		}
		if err := vfsrw.AddLocal(vfs, &vfsrw.ZipAsFolder{
			Enabled:   true,
			Digests:   []checksum.DigestAlgorithm{checksum.DigestSHA512},
			CacheSize: 3,
			Compress:  false,
			ReadOnly:  false,
		}); err != nil {
			return nil, fmt.Errorf("failed to add local filesystem to vfs: %w", err)
		}
		s.vfs = vfs
	}

	if s.conf.Autoconfig && s.conf.Thumbnail != nil {
		_, _ = ext_NNNN_thumbnail.Autoconfig(s.conf.Thumbnail, map[string]string{}, s.logger)
	}

	if s.conf.Indexer != nil && s.conf.Indexer.Optimize {
		_, _ = indexerutil.OptimizeConfig(s.conf.Indexer, s.logger)
	}

	ocflLogger := s.newLogger(context.Background(), version.Default)
	ext_NNNN_migration.Init(&s.conf.Migration, nil, ocflLogger)
	ext_NNNN_thumbnail.Init(s.conf.Thumbnail, nil, ocflLogger)
	ext_NNNN_indexer.Init(s.conf.Indexer, false, ocflLogger)
	ext_NNNN_metafile.Init(s.vfs, ocflLogger)

	return s, nil
}

func (s *GocflService) newLogger(ctx context.Context, ver version.OCFLVersion) ocfllogger.OCFLLogger {
	return ocfl.NewOCFLLogger(ctx, s.logger, nil, ver, nil)
}

func (s *GocflService) newStreamLogger(ctx context.Context, ver version.OCFLVersion, sendLog func(*pb.LogEntry) error) ocfllogger.OCFLLogger {
	if sendLog == nil {
		return s.newLogger(ctx, ver)
	}

	hook := zerolog.HookFunc(func(e *zerolog.Event, level zerolog.Level, message string) {
		entry := &pb.LogEntry{
			Timestamp: time.Now().UnixNano(),
			Level:     level.String(),
			Message:   message,
			JsonRaw:   fmt.Sprintf(`{"level":"%s","time":"%s","message":%q}`, level.String(), time.Now().Format(time.RFC3339Nano), message),
		}
		_ = sendLog(entry)
	})

	if zl, ok := s.logger.(*zerolog.Logger); ok {
		reqLogger := zl.Hook(hook)
		return ocfl.NewOCFLLogger(ctx, &reqLogger, nil, ver, nil)
	}

	return s.newLogger(ctx, ver)
}

// Init initializes an empty OCFL storage root.
func (s *GocflService) Init(req *pb.InitRequest, stream pb.GocflService_InitServer) error {
	if req.GetOcflPath() == "" {
		return status.Error(codes.InvalidArgument, "ocfl_path is required")
	}

	ctx := stream.Context()
	var streamMu sync.Mutex
	sendLog := func(entry *pb.LogEntry) error {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream.Send(&pb.InitResponse{
			Payload: &pb.InitResponse_Log{
				Log: entry,
			},
		})
	}

	ocflVer := version.Default
	if req.GetOcflVersion() != "" {
		ocflVer = version.OCFLVersion(req.GetOcflVersion())
	}

	digest := checksum.DigestSHA512
	if req.GetDigest() != "" {
		digest = checksum.DigestAlgorithm(req.GetDigest())
	}

	logger := s.newStreamLogger(ctx, ocflVer, sendLog)

	srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
	storageRootFS, closer, err := appendfs.Sub(s.vfs, srPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
	}
	defer func() { _ = closer.Close() }()

	extFS := s.defaultStorageRootExtensionFS
	if req.GetDefaultStoragerootExtensions() != "" {
		extFS = os.DirFS(req.GetDefaultStoragerootExtensions())
	}

	sr, err := ocfl.InitStorageRoot(ctx, storageRootFS, extFS, ocflVer, digest, req.GetExtensionParams(), logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to initialize storage root: %v", err)
	}
	defer func() { _ = sr.Close() }()

	streamMu.Lock()
	defer streamMu.Unlock()
	return stream.Send(&pb.InitResponse{
		Payload: &pb.InitResponse_Result{
			Result: &pb.InitResult{
				Success: true,
				Message: fmt.Sprintf("storage root initialized at '%s'", req.GetOcflPath()),
			},
		},
	})
}

// Add adds a new object into an existing OCFL structure.
func (s *GocflService) Add(req *pb.AddRequest, stream pb.GocflService_AddServer) error {
	if req.GetOcflPath() == "" {
		return status.Error(codes.InvalidArgument, "ocfl_path is required")
	}
	if req.GetObjectId() == "" {
		return status.Error(codes.InvalidArgument, "object_id is required")
	}

	ctx := stream.Context()
	var streamMu sync.Mutex
	sendLog := func(entry *pb.LogEntry) error {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream.Send(&pb.AddResponse{
			Payload: &pb.AddResponse_Log{
				Log: entry,
			},
		})
	}

	srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
	storageRootFS, closer, err := appendfs.Sub(s.vfs, srPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
	}
	defer func() { _ = closer.Close() }()

	ocflVer, err := util.GetStorageRootVersion(storageRootFS)
	if err != nil {
		ocflVer = version.Default
	}

	logger := s.newStreamLogger(ctx, ocflVer, sendLog)

	sr, err := ocfl.LoadStorageRoot(ctx, storageRootFS, req.GetExtensionParams(), nil, logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to load storage root: %v", err)
	}
	defer func() { _ = sr.Close() }()

	exists, err := sr.ObjectExists(req.GetObjectId())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check if object exists: %v", err)
	}
	if exists {
		return status.Errorf(codes.AlreadyExists, "object '%s' already exists in storage root", req.GetObjectId())
	}

	objFolder, err := sr.IdToFolder(req.GetObjectId())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to map id to folder: %v", err)
	}

	objFS, objCloser, err := appendfs.Sub(storageRootFS, objFolder)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for object folder '%s': %v", objFolder, err)
	}
	defer func() { _ = objCloser.Close() }()

	extFS := s.defaultObjectExtensionFS
	if req.GetDefaultObjectExtensions() != "" {
		extFS = os.DirFS(req.GetDefaultObjectExtensions())
	}

	digest := sr.GetDigest()
	if req.GetDigest() != "" {
		digest = checksum.DigestAlgorithm(req.GetDigest())
	}
	if digest == "" {
		digest = checksum.DigestSHA512
	}

	obj, err := ocfl.InitObject(ctx, objFS, extFS, sr.GetOCFLVersion(), req.GetObjectId(), digest, req.GetExtensionParams(), logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to initialize object '%s': %v", req.GetObjectId(), err)
	}
	defer func() { _ = obj.Close() }()

	userName := ""
	userAddress := ""
	if req.GetUser() != nil {
		userName = req.GetUser().GetName()
		userAddress = req.GetUser().GetAddress()
	}
	msg := req.GetMessage()
	if msg == "" {
		msg = "initial version"
	}

	vw, err := obj.StartUpdate(msg, userName, userAddress, false)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to start update: %v", err)
	}
	defer func() {
		if vw != nil {
			_ = vw.Close()
		}
	}()

	if req.GetSrcPath() != "" {
		srcFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, req.GetSrcPath()))
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to open src_path '%s': %v", req.GetSrcPath(), err)
		}
		if err := vw.AddFolder(srcFS, req.GetDeduplicate(), req.GetDefaultArea()); err != nil {
			return status.Errorf(codes.Internal, "failed to add source folder: %v", err)
		}
	}

	if len(req.GetAreaPaths()) > 0 {
		for areaName, areaPath := range req.GetAreaPaths() {
			areaFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, areaPath))
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "failed to open area path '%s': %v", areaPath, err)
			}
			if err := vw.AddFolder(areaFS, req.GetDeduplicate(), areaName); err != nil {
				return status.Errorf(codes.Internal, "failed to add area '%s' folder: %v", areaName, err)
			}
		}
	}

	if err := vw.Close(); err != nil {
		return status.Errorf(codes.Internal, "failed to close version writer: %v", err)
	}
	vw = nil

	versionStr := ""
	if obj.GetInventory() != nil && obj.GetInventory().GetHead() != nil {
		versionStr = obj.GetInventory().GetHead().String()
	}

	streamMu.Lock()
	defer streamMu.Unlock()
	return stream.Send(&pb.AddResponse{
		Payload: &pb.AddResponse_Result{
			Result: &pb.AddResult{
				Success:  true,
				Message:  fmt.Sprintf("object '%s' added successfully", req.GetObjectId()),
				ObjectId: req.GetObjectId(),
				Version:  versionStr,
			},
		},
	})
}

// Update adds a new version to an existing object in an OCFL structure.
func (s *GocflService) Update(req *pb.UpdateRequest, stream pb.GocflService_UpdateServer) error {
	if req.GetOcflPath() == "" {
		return status.Error(codes.InvalidArgument, "ocfl_path is required")
	}
	if req.GetObjectId() == "" {
		return status.Error(codes.InvalidArgument, "object_id is required")
	}

	ctx := stream.Context()
	var streamMu sync.Mutex
	sendLog := func(entry *pb.LogEntry) error {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream.Send(&pb.UpdateResponse{
			Payload: &pb.UpdateResponse_Log{
				Log: entry,
			},
		})
	}

	srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
	storageRootFS, closer, err := appendfs.Sub(s.vfs, srPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
	}
	defer func() { _ = closer.Close() }()

	ocflVer, err := util.GetStorageRootVersion(storageRootFS)
	if err != nil {
		ocflVer = version.Default
	}

	logger := s.newStreamLogger(ctx, ocflVer, sendLog)

	sr, err := ocfl.LoadStorageRoot(ctx, storageRootFS, req.GetExtensionParams(), nil, logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to load storage root: %v", err)
	}
	defer func() { _ = sr.Close() }()

	objFolder, err := sr.IdToFolder(req.GetObjectId())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to map id to folder: %v", err)
	}

	objFS, objCloser, err := appendfs.Sub(storageRootFS, objFolder)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for object folder '%s': %v", objFolder, err)
	}
	defer func() { _ = objCloser.Close() }()

	obj, err := ocfl.LoadObject(ctx, objFS, req.GetExtensionParams(), logger)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to load object '%s': %v", req.GetObjectId(), err)
	}
	defer func() { _ = obj.Close() }()

	userName := ""
	userAddress := ""
	if req.GetUser() != nil {
		userName = req.GetUser().GetName()
		userAddress = req.GetUser().GetAddress()
	}
	msg := req.GetMessage()
	if msg == "" {
		msg = "update version"
	}

	vw, err := obj.StartUpdate(msg, userName, userAddress, req.GetEcho())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to start update: %v", err)
	}
	defer func() {
		if vw != nil {
			_ = vw.Close()
		}
	}()

	if req.GetSrcPath() != "" {
		srcFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, req.GetSrcPath()))
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to open src_path '%s': %v", req.GetSrcPath(), err)
		}
		if err := vw.AddFolder(srcFS, req.GetDeduplicate(), ""); err != nil {
			return status.Errorf(codes.Internal, "failed to add source folder: %v", err)
		}
	}

	if len(req.GetAreaPaths()) > 0 {
		for areaName, areaPath := range req.GetAreaPaths() {
			areaFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, areaPath))
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "failed to open area path '%s': %v", areaPath, err)
			}
			if err := vw.AddFolder(areaFS, req.GetDeduplicate(), areaName); err != nil {
				return status.Errorf(codes.Internal, "failed to add area '%s' folder: %v", areaName, err)
			}
		}
	}

	if err := vw.Close(); err != nil {
		return status.Errorf(codes.Internal, "failed to close version writer: %v", err)
	}
	vw = nil

	versionStr := ""
	if obj.GetInventory() != nil && obj.GetInventory().GetHead() != nil {
		versionStr = obj.GetInventory().GetHead().String()
	}

	streamMu.Lock()
	defer streamMu.Unlock()
	return stream.Send(&pb.UpdateResponse{
		Payload: &pb.UpdateResponse_Result{
			Result: &pb.UpdateResult{
				Success:  true,
				Message:  fmt.Sprintf("object '%s' updated successfully", req.GetObjectId()),
				ObjectId: req.GetObjectId(),
				Version:  versionStr,
			},
		},
	})
}

// Create initializes an OCFL structure and adds an initial object.
func (s *GocflService) Create(req *pb.CreateRequest, stream pb.GocflService_CreateServer) error {
	if req.GetOcflPath() == "" {
		return status.Error(codes.InvalidArgument, "ocfl_path is required")
	}
	if req.GetObjectId() == "" {
		return status.Error(codes.InvalidArgument, "object_id is required")
	}

	ctx := stream.Context()
	var streamMu sync.Mutex
	sendLog := func(entry *pb.LogEntry) error {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream.Send(&pb.CreateResponse{
			Payload: &pb.CreateResponse_Log{
				Log: entry,
			},
		})
	}

	ocflVer := version.Default
	if req.GetOcflVersion() != "" {
		ocflVer = version.OCFLVersion(req.GetOcflVersion())
	}

	digest := checksum.DigestSHA512
	if req.GetDigest() != "" {
		digest = checksum.DigestAlgorithm(req.GetDigest())
	}

	logger := s.newStreamLogger(ctx, ocflVer, sendLog)

	srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
	storageRootFS, closer, err := appendfs.Sub(s.vfs, srPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
	}
	defer func() { _ = closer.Close() }()

	srExtFS := s.defaultStorageRootExtensionFS
	if req.GetDefaultStoragerootExtensions() != "" {
		srExtFS = os.DirFS(req.GetDefaultStoragerootExtensions())
	}

	sr, err := ocfl.InitStorageRoot(ctx, storageRootFS, srExtFS, ocflVer, digest, req.GetExtensionParams(), logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to initialize storage root: %v", err)
	}
	defer func() { _ = sr.Close() }()

	objFolder, err := sr.IdToFolder(req.GetObjectId())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to map id to folder: %v", err)
	}

	objFS, objCloser, err := appendfs.Sub(storageRootFS, objFolder)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subfs for object folder '%s': %v", objFolder, err)
	}
	defer func() { _ = objCloser.Close() }()

	objExtFS := s.defaultObjectExtensionFS
	if req.GetDefaultObjectExtensions() != "" {
		objExtFS = os.DirFS(req.GetDefaultObjectExtensions())
	}

	obj, err := ocfl.InitObject(ctx, objFS, objExtFS, ocflVer, req.GetObjectId(), digest, req.GetExtensionParams(), logger)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to initialize object '%s': %v", req.GetObjectId(), err)
	}
	defer func() { _ = obj.Close() }()

	userName := ""
	userAddress := ""
	if req.GetUser() != nil {
		userName = req.GetUser().GetName()
		userAddress = req.GetUser().GetAddress()
	}
	msg := req.GetMessage()
	if msg == "" {
		msg = "initial version"
	}

	vw, err := obj.StartUpdate(msg, userName, userAddress, false)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to start update: %v", err)
	}
	defer func() {
		if vw != nil {
			_ = vw.Close()
		}
	}()

	if req.GetSrcPath() != "" {
		srcFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, req.GetSrcPath()))
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to open src_path '%s': %v", req.GetSrcPath(), err)
		}
		if err := vw.AddFolder(srcFS, req.GetDeduplicate(), req.GetDefaultArea()); err != nil {
			return status.Errorf(codes.Internal, "failed to add source folder: %v", err)
		}
	}

	if len(req.GetAreaPaths()) > 0 {
		for areaName, areaPath := range req.GetAreaPaths() {
			areaFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, areaPath))
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "failed to open area path '%s': %v", areaPath, err)
			}
			if err := vw.AddFolder(areaFS, req.GetDeduplicate(), areaName); err != nil {
				return status.Errorf(codes.Internal, "failed to add area '%s' folder: %v", areaName, err)
			}
		}
	}

	if err := vw.Close(); err != nil {
		return status.Errorf(codes.Internal, "failed to close version writer: %v", err)
	}
	vw = nil

	versionStr := ""
	if obj.GetInventory() != nil && obj.GetInventory().GetHead() != nil {
		versionStr = obj.GetInventory().GetHead().String()
	}

	streamMu.Lock()
	defer streamMu.Unlock()
	return stream.Send(&pb.CreateResponse{
		Payload: &pb.CreateResponse_Result{
			Result: &pb.CreateResult{
				Success:  true,
				Message:  fmt.Sprintf("storage root and object '%s' created successfully", req.GetObjectId()),
				ObjectId: req.GetObjectId(),
				Version:  versionStr,
			},
		},
	})
}

// Validate validates an OCFL storage root or a specific object.
func (s *GocflService) Validate(req *pb.ValidateRequest, stream pb.GocflService_ValidateServer) error {
	if req.GetOcflPath() == "" && req.GetObjectPath() == "" {
		return status.Error(codes.InvalidArgument, "either ocfl_path or object_path is required")
	}

	ctx := stream.Context()
	var streamMu sync.Mutex
	sendLog := func(entry *pb.LogEntry) error {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream.Send(&pb.ValidateResponse{
			Payload: &pb.ValidateResponse_Log{
				Log: entry,
			},
		})
	}

	var ocflVer = version.Default
	var storageRootFS fs.FS
	var objFolder string

	if req.GetObjectPath() != "" {
		objFolder = writefs.RealPath(s.vfs, req.GetObjectPath())
	} else {
		srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
		subFS, err := fs.Sub(s.vfs, srPath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
		}
		storageRootFS = subFS

		v, err := util.GetStorageRootVersion(storageRootFS)
		if err == nil {
			ocflVer = v
		}
	}

	logger := s.newStreamLogger(ctx, ocflVer, sendLog)

	var valErrors []*pb.ValidationError
	var warnings []string

	if req.GetObjectId() == "" && req.GetObjectPath() == "" {
		// Storage Root Validation
		sr, err := ocfl.LoadStorageRoot(ctx, storageRootFS, req.GetExtensionParams(), nil, logger)
		if err != nil {
			streamMu.Lock()
			defer streamMu.Unlock()
			return stream.Send(&pb.ValidateResponse{
				Payload: &pb.ValidateResponse_Result{
					Result: &pb.ValidateResult{
						IsValid: false,
						Message: fmt.Sprintf("failed to load storage root: %v", err),
					},
				},
			})
		}
		defer func() { _ = sr.Close() }()

		if err := sr.Check(); err != nil {
			// Validation check recorded errors in logger
		}
	} else {
		// Object Validation
		if storageRootFS != nil && req.GetObjectId() != "" {
			sr, err := ocfl.LoadStorageRoot(ctx, storageRootFS, req.GetExtensionParams(), nil, logger)
			if err != nil {
				streamMu.Lock()
				defer streamMu.Unlock()
				return stream.Send(&pb.ValidateResponse{
					Payload: &pb.ValidateResponse_Result{
						Result: &pb.ValidateResult{
							IsValid: false,
							Message: fmt.Sprintf("failed to load storage root: %v", err),
						},
					},
				})
			}
			defer func() { _ = sr.Close() }()

			folder, err := sr.IdToFolder(req.GetObjectId())
			if err != nil {
				return status.Errorf(codes.Internal, "failed to get folder for id '%s': %v", req.GetObjectId(), err)
			}
			objFolder = writefs.RealPath(s.vfs, req.GetOcflPath()+"/"+folder)
		}

		objFS, err := fs.Sub(s.vfs, objFolder)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create subfs for object '%s': %v", objFolder, err)
		}

		obj, err := ocfl.LoadObject(ctx, objFS, req.GetExtensionParams(), logger)
		if err != nil {
			streamMu.Lock()
			defer streamMu.Unlock()
			return stream.Send(&pb.ValidateResponse{
				Payload: &pb.ValidateResponse_Result{
					Result: &pb.ValidateResult{
						IsValid: false,
						Message: fmt.Sprintf("failed to load object: %v", err),
					},
				},
			})
		}
		defer func() { _ = obj.Close() }()

		validator := obj.GetValidator()
		if validator != nil {
			defer func() { _ = validator.Close() }()
			if err := validator.Validate(); err != nil {
				// Validator recorded errors in logger
			}
		}
	}

	for _, vErr := range logger.ValidationErrors() {
		if vErr.Code.IsError() {
			valErrors = append(valErrors, &pb.ValidationError{
				Code:        string(vErr.Code),
				Description: vErr.Error(),
				Path:        vErr.Context,
			})
		} else if vErr.Code.IsWarning() {
			warnings = append(warnings, fmt.Sprintf("[%s] %s (%s)", vErr.Code, vErr.Error(), vErr.Context))
		}
	}

	isValid := len(valErrors) == 0

	msg := "validation successful"
	if !isValid {
		msg = fmt.Sprintf("validation found %d error(s)", len(valErrors))
	} else if len(warnings) > 0 {
		msg = fmt.Sprintf("validation successful with %d warning(s)", len(warnings))
	}

	streamMu.Lock()
	defer streamMu.Unlock()
	return stream.Send(&pb.ValidateResponse{
		Payload: &pb.ValidateResponse_Result{
			Result: &pb.ValidateResult{
				IsValid:  isValid,
				Message:  msg,
				Errors:   valErrors,
				Warnings: warnings,
			},
		},
	})
}
