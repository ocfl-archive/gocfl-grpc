package service

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	pb "github.com/ocfl-archive/gocfl-grpc/pkg/gocfl/proto"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/util"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CloseHandle releases an active StorageRoot, Object, or Updater handle.
func (s *GocflService) CloseHandle(ctx context.Context, req *pb.CloseHandleRequest) (*pb.CloseHandleResponse, error) {
	if req.GetHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "handle_id is required")
	}

	if err := s.handleManager.CloseHandle(req.GetHandleId()); err != nil {
		return nil, err
	}

	return &pb.CloseHandleResponse{
		Success: true,
		Message: fmt.Sprintf("handle '%s' successfully closed and released", req.GetHandleId()),
	}, nil
}

// KeepAlive renews the lease/TTL of an open handle.
func (s *GocflService) KeepAlive(ctx context.Context, req *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	if req.GetHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "handle_id is required")
	}

	extend := time.Duration(req.GetExtendSeconds()) * time.Second
	expiresAt, err := s.handleManager.KeepAlive(req.GetHandleId(), extend)
	if err != nil {
		return nil, err
	}

	return &pb.KeepAliveResponse{
		Success:       true,
		ExpiresAtUnix: expiresAt.Unix(),
	}, nil
}

// OpenStorageRoot opens an existing OCFL storage root and registers an active handle.
func (s *GocflService) OpenStorageRoot(ctx context.Context, req *pb.OpenStorageRootRequest) (*pb.StorageRootHandle, error) {
	if req.GetOcflPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "ocfl_path is required")
	}

	srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
	storageRootFS, closer, err := appendfs.Sub(s.vfs, srPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
	}

	ocflVer, err := util.GetStorageRootVersion(storageRootFS)
	if err != nil {
		ocflVer = version.Default
	}

	logger := s.newLogger(ctx, ocflVer)
	sr, err := ocfl.LoadStorageRoot(ctx, storageRootFS, req.GetExtensionParams(), nil, logger)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, status.Errorf(codes.Internal, "failed to load storage root at '%s': %v", req.GetOcflPath(), err)
	}
	sr.WithReadFS(storageRootFS)

	entry := s.handleManager.StorageRoots.Register(sr, storageRootFS, closer, "", map[string]string{
		"path": req.GetOcflPath(),
	})

	return &pb.StorageRootHandle{
		Id:            entry.ID,
		ExpiresAtUnix: entry.ExpiresAt.Unix(),
	}, nil
}

// InitStorageRootHandle initializes an OCFL storage root and registers an active handle.
func (s *GocflService) InitStorageRootHandle(ctx context.Context, req *pb.InitStorageRootHandleRequest) (*pb.StorageRootHandle, error) {
	if req.GetOcflPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "ocfl_path is required")
	}

	ocflVer := version.Default
	if req.GetOcflVersion() != "" {
		ocflVer = version.OCFLVersion(req.GetOcflVersion())
	}

	digest := checksum.DigestSHA512
	if req.GetDigest() != "" {
		digest = checksum.DigestAlgorithm(req.GetDigest())
	}

	logger := s.newLogger(ctx, ocflVer)

	srPath := writefs.RealPath(s.vfs, req.GetOcflPath())
	storageRootFS, closer, err := appendfs.Sub(s.vfs, srPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create subfs for storage root '%s': %v", srPath, err)
	}

	extFS := s.defaultStorageRootExtensionFS
	if req.GetDefaultStoragerootExtensions() != "" {
		extFS = os.DirFS(req.GetDefaultStoragerootExtensions())
	}

	sr, err := ocfl.InitStorageRoot(ctx, storageRootFS, extFS, ocflVer, digest, req.GetExtensionParams(), logger)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, status.Errorf(codes.Internal, "failed to initialize storage root: %v", err)
	}
	sr.WithReadFS(storageRootFS)

	entry := s.handleManager.StorageRoots.Register(sr, storageRootFS, closer, "", map[string]string{
		"path": req.GetOcflPath(),
	})

	return &pb.StorageRootHandle{
		Id:            entry.ID,
		ExpiresAtUnix: entry.ExpiresAt.Unix(),
	}, nil
}

// ListObjects lists object folders in an open storage root.
func (s *GocflService) ListObjects(ctx context.Context, req *pb.ListObjectsRequest) (*pb.ListObjectsResponse, error) {
	if req.GetStoragerootHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "storageroot_handle_id is required")
	}

	srEntry, err := s.handleManager.StorageRoots.Get(req.GetStoragerootHandleId())
	if err != nil {
		return nil, err
	}

	srEntry.mu.RLock()
	defer srEntry.mu.RUnlock()

	folders, err := srEntry.Resource.GetObjectFolders()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list object folders: %v", err)
	}

	var summaries []*pb.ObjectSummary
	for _, f := range folders {
		summaries = append(summaries, &pb.ObjectSummary{
			Folder: f,
		})
	}

	return &pb.ListObjectsResponse{
		Objects:       summaries,
		ObjectFolders: folders,
	}, nil
}

// GetStorageRootDetails retrieves metadata from an open storage root.
func (s *GocflService) GetStorageRootDetails(ctx context.Context, req *pb.GetStorageRootDetailsRequest) (*pb.StorageRootDetailsResponse, error) {
	if req.GetStoragerootHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "storageroot_handle_id is required")
	}

	srEntry, err := s.handleManager.StorageRoots.Get(req.GetStoragerootHandleId())
	if err != nil {
		return nil, err
	}

	srEntry.mu.RLock()
	defer srEntry.mu.RUnlock()

	folders, _ := srEntry.Resource.GetObjectFolders()

	return &pb.StorageRootDetailsResponse{
		OcflVersion:     string(srEntry.Resource.GetOCFLVersion()),
		DigestAlgorithm: string(srEntry.Resource.GetDigest()),
		ObjectFolders:   folders,
	}, nil
}

// OpenObject opens an existing OCFL object within a storage root or direct filesystem path.
func (s *GocflService) OpenObject(ctx context.Context, req *pb.OpenObjectRequest) (*pb.ObjectHandle, error) {
	var objFS appendfs.FS
	var closer io.Closer
	var parentSRID string
	var objectID = req.GetObjectId()

	logger := s.newLogger(ctx, version.Default)

	if req.GetStoragerootHandleId() != "" {
		if req.GetObjectId() == "" {
			return nil, status.Error(codes.InvalidArgument, "object_id is required when opening via storageroot_handle_id")
		}

		srEntry, err := s.handleManager.StorageRoots.Get(req.GetStoragerootHandleId())
		if err != nil {
			return nil, err
		}

		srEntry.mu.RLock()
		objFolder, err := srEntry.Resource.IdToFolder(req.GetObjectId())
		if err != nil {
			srEntry.mu.RUnlock()
			return nil, status.Errorf(codes.Internal, "failed to map id to folder: %v", err)
		}
		subFS, subCloser, err := appendfs.Sub(srEntry.FS, objFolder)
		srEntry.mu.RUnlock()

		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to open object subfs '%s': %v", objFolder, err)
		}

		objFS = subFS
		closer = subCloser
		parentSRID = req.GetStoragerootHandleId()
	} else if req.GetObjectPath() != "" {
		objPath := writefs.RealPath(s.vfs, req.GetObjectPath())
		subFS, subCloser, err := appendfs.Sub(s.vfs, objPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to open object subfs at '%s': %v", objPath, err)
		}
		objFS = subFS
		closer = subCloser
	} else {
		return nil, status.Error(codes.InvalidArgument, "either storageroot_handle_id or object_path is required")
	}

	obj, err := ocfl.LoadObject(ctx, objFS, req.GetExtensionParams(), logger)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, status.Errorf(codes.Internal, "failed to load object: %v", err)
	}
	obj.WithReadFS(objFS)

	if objectID == "" && obj.GetInventory() != nil {
		objectID = obj.GetInventory().GetID()
	}

	entry := s.handleManager.Objects.Register(obj, objFS, closer, parentSRID, map[string]string{
		"object_id": objectID,
	})

	return &pb.ObjectHandle{
		Id:            entry.ID,
		ExpiresAtUnix: entry.ExpiresAt.Unix(),
	}, nil
}

// InitObjectHandle initializes a new OCFL object and registers an active handle.
func (s *GocflService) InitObjectHandle(ctx context.Context, req *pb.InitObjectHandleRequest) (*pb.ObjectHandle, error) {
	if req.GetObjectId() == "" {
		return nil, status.Error(codes.InvalidArgument, "object_id is required")
	}

	var objFS appendfs.FS
	var closer io.Closer
	var parentSRID string
	var ocflVer = version.Default
	var digest = checksum.DigestSHA512

	if req.GetOcflVersion() != "" {
		ocflVer = version.OCFLVersion(req.GetOcflVersion())
	}
	if req.GetDigest() != "" {
		digest = checksum.DigestAlgorithm(req.GetDigest())
	}

	if req.GetStoragerootHandleId() != "" {
		srEntry, err := s.handleManager.StorageRoots.Get(req.GetStoragerootHandleId())
		if err != nil {
			return nil, err
		}

		srEntry.mu.RLock()
		if req.GetOcflVersion() == "" {
			ocflVer = srEntry.Resource.GetOCFLVersion()
		}
		if req.GetDigest() == "" {
			digest = srEntry.Resource.GetDigest()
		}

		objFolder, err := srEntry.Resource.IdToFolder(req.GetObjectId())
		if err != nil {
			srEntry.mu.RUnlock()
			return nil, status.Errorf(codes.Internal, "failed to map id to folder: %v", err)
		}
		subFS, subCloser, err := appendfs.Sub(srEntry.FS, objFolder)
		srEntry.mu.RUnlock()

		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to open object subfs '%s': %v", objFolder, err)
		}

		objFS = subFS
		closer = subCloser
		parentSRID = req.GetStoragerootHandleId()
	} else if req.GetObjectPath() != "" {
		objPath := writefs.RealPath(s.vfs, req.GetObjectPath())
		subFS, subCloser, err := appendfs.Sub(s.vfs, objPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to open object subfs at '%s': %v", objPath, err)
		}
		objFS = subFS
		closer = subCloser
	} else {
		return nil, status.Error(codes.InvalidArgument, "either storageroot_handle_id or object_path is required")
	}

	extFS := s.defaultObjectExtensionFS
	if req.GetDefaultObjectExtensions() != "" {
		extFS = os.DirFS(req.GetDefaultObjectExtensions())
	}

	logger := s.newLogger(ctx, ocflVer)
	obj, err := ocfl.InitObject(ctx, objFS, extFS, ocflVer, req.GetObjectId(), digest, req.GetExtensionParams(), logger)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, status.Errorf(codes.Internal, "failed to initialize object '%s': %v", req.GetObjectId(), err)
	}
	obj.WithReadFS(objFS)

	entry := s.handleManager.Objects.Register(obj, objFS, closer, parentSRID, map[string]string{
		"object_id": req.GetObjectId(),
	})

	return &pb.ObjectHandle{
		Id:            entry.ID,
		ExpiresAtUnix: entry.ExpiresAt.Unix(),
	}, nil
}

// GetInventory retrieves the full Inventory snapshot of an open object.
func (s *GocflService) GetInventory(ctx context.Context, req *pb.GetInventoryRequest) (*pb.InventoryResponse, error) {
	if req.GetObjectHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "object_handle_id is required")
	}

	objEntry, err := s.handleManager.Objects.Get(req.GetObjectHandleId())
	if err != nil {
		return nil, err
	}

	objEntry.mu.RLock()
	defer objEntry.mu.RUnlock()

	inv := objEntry.Resource.GetInventory()
	if inv == nil {
		return nil, status.Error(codes.NotFound, "inventory not found in object")
	}

	return &pb.InventoryResponse{
		Inventory: InventoryToProto(inv),
	}, nil
}

// ValidateObjectHandle validates an open OCFL object.
func (s *GocflService) ValidateObjectHandle(ctx context.Context, req *pb.ValidateObjectHandleRequest) (*pb.ValidateObjectHandleResponse, error) {
	if req.GetObjectHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "object_handle_id is required")
	}

	objEntry, err := s.handleManager.Objects.Get(req.GetObjectHandleId())
	if err != nil {
		return nil, err
	}

	objEntry.mu.RLock()
	defer objEntry.mu.RUnlock()

	validator := objEntry.Resource.GetValidator()
	var errs []*pb.ValidationError
	var warns []string
	isValid := true

	if validator != nil {
		defer func() { _ = validator.Close() }()
		if err := validator.Validate(); err != nil {
			isValid = false
			errs = append(errs, &pb.ValidationError{
				Code:        "VALIDATION_ERROR",
				Description: err.Error(),
			})
		}
	}

	msg := "object validation successful"
	if !isValid {
		msg = "object validation failed"
	}

	return &pb.ValidateObjectHandleResponse{
		IsValid:  isValid,
		Message:  msg,
		Errors:   errs,
		Warnings: warns,
	}, nil
}

// BeginUpdate starts a new version update session on an open object.
func (s *GocflService) BeginUpdate(ctx context.Context, req *pb.BeginUpdateRequest) (*pb.UpdaterHandle, error) {
	if req.GetObjectHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "object_handle_id is required")
	}

	objEntry, err := s.handleManager.Objects.Get(req.GetObjectHandleId())
	if err != nil {
		return nil, err
	}

	objEntry.mu.Lock()
	defer objEntry.mu.Unlock()

	userName := ""
	userAddress := ""
	if req.GetUser() != nil {
		userName = req.GetUser().GetName()
		userAddress = req.GetUser().GetAddress()
	}

	vw, err := objEntry.Resource.StartUpdate(req.GetMessage(), userName, userAddress, req.GetEcho())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin update session: %v", err)
	}

	updEntry := s.handleManager.Updaters.Register(vw, nil, nil, objEntry.ID, map[string]string{
		"object_handle_id": objEntry.ID,
	})

	return &pb.UpdaterHandle{
		Id:            updEntry.ID,
		ExpiresAtUnix: updEntry.ExpiresAt.Unix(),
	}, nil
}

type vfsSingleFileFS struct {
	vfs      fs.FS
	srcPath  string
	destPath string
}

func (f *vfsSingleFileFS) Open(name string) (fs.File, error) {
	cleanName := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(name)), "/")
	cleanDest := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(f.destPath)), "/")
	if cleanName == cleanDest || name == f.destPath {
		return f.vfs.Open(f.srcPath)
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

// AddFile adds a file from VFS path or content payload into the active version.
func (s *GocflService) AddFile(ctx context.Context, req *pb.AddFileRequest) (*pb.AddFileResponse, error) {
	if req.GetUpdaterHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "updater_handle_id is required")
	}

	srcPath := req.GetSrcPath()
	destPath := req.GetDestPath()
	hasContent := len(req.GetContent()) > 0

	if srcPath == "" && req.GetPath() != "" {
		if hasContent {
			destPath = req.GetPath()
		} else {
			srcPath = req.GetPath()
		}
	}
	if destPath == "" {
		if req.GetPath() != "" {
			destPath = req.GetPath()
		} else {
			destPath = srcPath
		}
	}

	if srcPath == "" && !hasContent {
		return nil, status.Error(codes.InvalidArgument, "src_path or content is required")
	}

	updEntry, err := s.handleManager.Updaters.Get(req.GetUpdaterHandleId())
	if err != nil {
		return nil, err
	}

	updEntry.mu.Lock()
	defer updEntry.mu.Unlock()

	area := req.GetArea()
	if area == "" {
		area = "content"
	}

	if hasContent {
		if err := updEntry.Resource.AddData(req.GetContent(), destPath, req.GetDeduplicate(), area, false, false); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to add file data '%s': %v", destPath, err)
		}
	} else {
		realSrcPath := writefs.RealPath(s.vfs, srcPath)
		// Check that source file can be opened in VFS
		f, err := s.vfs.Open(realSrcPath)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to open src_path '%s' in vfs: %v", srcPath, err)
		}
		_ = f.Close()

		adapter := &vfsSingleFileFS{
			vfs:      s.vfs,
			srcPath:  realSrcPath,
			destPath: destPath,
		}

		if err := updEntry.Resource.AddFile(adapter, destPath, req.GetDeduplicate(), area, false, false); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to add file '%s' (src: '%s'): %v", destPath, srcPath, err)
		}
	}

	return &pb.AddFileResponse{
		Success: true,
		Message: fmt.Sprintf("file '%s' added successfully", destPath),
	}, nil
}

// AddFolder adds all files from a VFS path to an active updater.
func (s *GocflService) AddFolder(ctx context.Context, req *pb.AddFolderRequest) (*pb.AddFolderResponse, error) {
	if req.GetUpdaterHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "updater_handle_id is required")
	}
	if req.GetSrcPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "src_path is required")
	}

	updEntry, err := s.handleManager.Updaters.Get(req.GetUpdaterHandleId())
	if err != nil {
		return nil, err
	}

	srcFS, err := fs.Sub(s.vfs, writefs.RealPath(s.vfs, req.GetSrcPath()))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to open src_path '%s': %v", req.GetSrcPath(), err)
	}

	updEntry.mu.Lock()
	defer updEntry.mu.Unlock()

	if err := updEntry.Resource.AddFolder(srcFS, req.GetDeduplicate(), req.GetArea()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add folder '%s': %v", req.GetSrcPath(), err)
	}

	return &pb.AddFolderResponse{
		Success: true,
		Message: fmt.Sprintf("folder '%s' added successfully", req.GetSrcPath()),
	}, nil
}

// DeleteFile removes a file from the active version state.
func (s *GocflService) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if req.GetUpdaterHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "updater_handle_id is required")
	}
	if req.GetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}

	updEntry, err := s.handleManager.Updaters.Get(req.GetUpdaterHandleId())
	if err != nil {
		return nil, err
	}

	updEntry.mu.Lock()
	defer updEntry.mu.Unlock()

	if err := updEntry.Resource.DeleteFile(req.GetPath(), req.GetDigest()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete file '%s': %v", req.GetPath(), err)
	}

	return &pb.DeleteFileResponse{
		Success: true,
		Message: fmt.Sprintf("file '%s' marked for deletion", req.GetPath()),
	}, nil
}

// RenameFile renames a file within the active version state.
func (s *GocflService) RenameFile(ctx context.Context, req *pb.RenameFileRequest) (*pb.RenameFileResponse, error) {
	if req.GetUpdaterHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "updater_handle_id is required")
	}
	if req.GetSourcePath() == "" || req.GetDestPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_path and dest_path are required")
	}

	updEntry, err := s.handleManager.Updaters.Get(req.GetUpdaterHandleId())
	if err != nil {
		return nil, err
	}

	updEntry.mu.Lock()
	defer updEntry.mu.Unlock()

	if err := updEntry.Resource.RenameFile(req.GetSourcePath(), req.GetDestPath(), req.GetDigest()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rename file from '%s' to '%s': %v", req.GetSourcePath(), req.GetDestPath(), err)
	}

	return &pb.RenameFileResponse{
		Success: true,
		Message: fmt.Sprintf("file renamed from '%s' to '%s'", req.GetSourcePath(), req.GetDestPath()),
	}, nil
}

// CommitUpdate commits the active version update and closes the updater.
func (s *GocflService) CommitUpdate(ctx context.Context, req *pb.CommitUpdateRequest) (*pb.CommitUpdateResponse, error) {
	if req.GetUpdaterHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "updater_handle_id is required")
	}

	updEntry, err := s.handleManager.Updaters.Get(req.GetUpdaterHandleId())
	if err != nil {
		return nil, err
	}

	updEntry.mu.Lock()
	defer updEntry.mu.Unlock()

	if err := updEntry.Resource.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit update: %v", err)
	}

	objHandleID := updEntry.ParentID
	s.handleManager.Updaters.Remove(req.GetUpdaterHandleId())

	var headVer string
	var pbInv *pb.Inventory
	if objHandleID != "" {
		objEntry, err := s.handleManager.Objects.Get(objHandleID)
		if err == nil && objEntry != nil {
			objEntry.mu.RLock()
			if objEntry.Resource != nil && objEntry.Resource.GetInventory() != nil {
				if head := objEntry.Resource.GetInventory().GetHead(); head != nil {
					headVer = head.String()
				}
				pbInv = InventoryToProto(objEntry.Resource.GetInventory())
			}
			objEntry.mu.RUnlock()
		}
	}

	return &pb.CommitUpdateResponse{
		Success:      true,
		Message:      "update committed successfully",
		HeadVersion:  headVer,
		NewInventory: pbInv,
	}, nil
}

// RollbackUpdate cancels the active update and releases staging resources.
func (s *GocflService) RollbackUpdate(ctx context.Context, req *pb.RollbackUpdateRequest) (*pb.RollbackUpdateResponse, error) {
	if req.GetUpdaterHandleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "updater_handle_id is required")
	}

	if err := s.handleManager.CloseHandle(req.GetUpdaterHandleId()); err != nil {
		return nil, err
	}

	return &pb.RollbackUpdateResponse{
		Success: true,
		Message: "update rolled back and staging resources released",
	}, nil
}
