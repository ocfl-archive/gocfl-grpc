package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleResource defines the interface constraint that all handle-managed entities must satisfy.
// Every active handle represents a stateful server-side resource that requires deterministic
// cleanup (closing files, releasing locks, cleaning temporary staging buffers) upon lease expiration
// or explicit termination.
type HandleResource interface {
	io.Closer
}

// HandleEntry encapsulates a typed resource with lifecycle tracking, concurrency controls, and cleanup handlers.
type HandleEntry[T HandleResource] struct {
	ID        string
	Resource  T
	FS        appendfs.FS
	Closer    io.Closer
	ParentID  string
	Metadata  map[string]string
	ExpiresAt time.Time
	mu        sync.RWMutex
}

// Touch extends the expiration time of the handle entry.
func (e *HandleEntry[T]) Touch(ttl time.Duration) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ExpiresAt = time.Now().Add(ttl)
	return e.ExpiresAt
}

// IsExpired checks whether the handle has exceeded its TTL.
func (e *HandleEntry[T]) IsExpired(now time.Time) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return now.After(e.ExpiresAt)
}

// Close releases the underlying resource and any associated filesystem/closers safely.
func (e *HandleEntry[T]) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var firstErr error
	var zero T
	if any(e.Resource) != nil {
		if err := e.Resource.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.Resource = zero
	}
	if e.Closer != nil {
		if err := e.Closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.Closer = nil
	}
	e.FS = nil
	return firstErr
}

// ClosableStore defines the common interface for all typed handle stores, enabling unified
// lifecycle, keep-alive, and eviction management across different resource types.
type ClosableStore interface {
	Has(id string) bool
	Touch(id string, extend time.Duration) (time.Time, bool, error)
	CloseHandle(id string) (bool, error)
	EvictExpired(now time.Time) int
	CloseAll() error
}

// HandleStore provides thread-safe storage, TTL tracking, and lifecycle operations for a specific HandleResource type.
type HandleStore[T HandleResource] struct {
	mu         sync.RWMutex
	prefix     string
	defaultTTL time.Duration
	entries    map[string]*HandleEntry[T]
	logger     zLogger.ZLogger
}

// NewHandleStore creates a new generic HandleStore for a specific HandleResource type.
func NewHandleStore[T HandleResource](prefix string, defaultTTL time.Duration, logger zLogger.ZLogger) *HandleStore[T] {
	return &HandleStore[T]{
		prefix:     prefix,
		defaultTTL: defaultTTL,
		entries:    make(map[string]*HandleEntry[T]),
		logger:     logger,
	}
}

func generateHandleID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}

// Register adds a new resource to the store and returns its HandleEntry.
func (s *HandleStore[T]) Register(res T, fsys appendfs.FS, closer io.Closer, parentID string, meta map[string]string) *HandleEntry[T] {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := generateHandleID(s.prefix)
	entry := &HandleEntry[T]{
		ID:        id,
		Resource:  res,
		FS:        fsys,
		Closer:    closer,
		ParentID:  parentID,
		Metadata:  meta,
		ExpiresAt: time.Now().Add(s.defaultTTL),
	}
	s.entries[id] = entry

	if s.logger != nil {
		s.logger.Info().Str("handle", id).Str("type", s.prefix).Msg("registered handle")
	}
	return entry
}

// Get retrieves an active handle entry and automatically touches its TTL.
func (s *HandleStore[T]) Get(id string) (*HandleEntry[T], error) {
	s.mu.RLock()
	entry, ok := s.entries[id]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s handle '%s' not found or expired", s.prefix, id)
	}

	entry.Touch(s.defaultTTL)
	return entry, nil
}

// Has checks if a handle ID exists in this store.
func (s *HandleStore[T]) Has(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[id]
	return ok
}

// Touch extends the expiration time of a handle in this store.
func (s *HandleStore[T]) Touch(id string, extend time.Duration) (time.Time, bool, error) {
	s.mu.RLock()
	entry, ok := s.entries[id]
	s.mu.RUnlock()

	if !ok {
		return time.Time{}, false, nil
	}

	newExpiry := entry.Touch(extend)
	return newExpiry, true, nil
}

// Remove deletes an entry from the store without closing it.
func (s *HandleStore[T]) Remove(id string) (*HandleEntry[T], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[id]
	if ok {
		delete(s.entries, id)
	}
	return entry, ok
}

// CloseHandle removes and closes an entry from the store.
func (s *HandleStore[T]) CloseHandle(id string) (bool, error) {
	s.mu.Lock()
	entry, ok := s.entries[id]
	if ok {
		delete(s.entries, id)
	}
	s.mu.Unlock()

	if !ok {
		return false, nil
	}

	if s.logger != nil {
		s.logger.Info().Str("handle", id).Str("type", s.prefix).Msg("closing handle")
	}
	return true, entry.Close()
}

// EvictExpired removes and closes all expired entries in this store.
func (s *HandleStore[T]) EvictExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	evicted := 0
	for id, entry := range s.entries {
		if entry.IsExpired(now) {
			delete(s.entries, id)
			if s.logger != nil {
				s.logger.Info().Str("handle", id).Str("type", s.prefix).Msg("evicting expired handle")
			}
			_ = entry.Close()
			evicted++
		}
	}
	return evicted
}

// CloseAll releases all entries in this store.
func (s *HandleStore[T]) CloseAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for id, entry := range s.entries {
		delete(s.entries, id)
		if err := entry.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// HandleManager coordinates all typed handle stores and runs background lease cleanup.
type HandleManager struct {
	StorageRoots *HandleStore[storageroot.StorageRoot]
	Objects      *HandleStore[object.Object]
	Updaters     *HandleStore[object.VersionWriter]
	stores       []ClosableStore
	defaultTTL   time.Duration
	stopChan     chan struct{}
	stopOnce     sync.Once
	logger       zLogger.ZLogger
}

// NewHandleManager creates and starts a new generic HandleManager.
func NewHandleManager(defaultTTL time.Duration, logger zLogger.ZLogger) *HandleManager {
	if defaultTTL <= 0 {
		defaultTTL = 15 * time.Minute
	}

	srStore := NewHandleStore[storageroot.StorageRoot]("sr", defaultTTL, logger)
	objStore := NewHandleStore[object.Object]("obj", defaultTTL, logger)
	updStore := NewHandleStore[object.VersionWriter]("upd", defaultTTL, logger)

	hm := &HandleManager{
		StorageRoots: srStore,
		Objects:      objStore,
		Updaters:     updStore,
		stores:       []ClosableStore{updStore, objStore, srStore}, // eviction order: updaters -> objects -> storage roots
		defaultTTL:   defaultTTL,
		stopChan:     make(chan struct{}),
		logger:       logger,
	}

	go hm.cleanupRoutine()
	return hm
}

func (hm *HandleManager) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.evictExpired()
		case <-hm.stopChan:
			return
		}
	}
}

func (hm *HandleManager) evictExpired() {
	now := time.Now()
	for _, store := range hm.stores {
		store.EvictExpired(now)
	}
}

// KeepAlive extends the lease of any active handle across all stores.
func (hm *HandleManager) KeepAlive(handleID string, extend time.Duration) (time.Time, error) {
	if extend <= 0 {
		extend = hm.defaultTTL
	}

	for _, store := range hm.stores {
		if exp, ok, err := store.Touch(handleID, extend); ok {
			return exp, err
		}
	}

	return time.Time{}, status.Errorf(codes.NotFound, "handle '%s' not found or expired", handleID)
}

// CloseHandle releases an active handle across all stores.
func (hm *HandleManager) CloseHandle(handleID string) error {
	for _, store := range hm.stores {
		if ok, err := store.CloseHandle(handleID); ok {
			return err
		}
	}

	return status.Errorf(codes.NotFound, "handle '%s' not found or already closed", handleID)
}

// Close closes all active handles across all stores and terminates background routines.
func (hm *HandleManager) Close() {
	hm.stopOnce.Do(func() {
		close(hm.stopChan)
	})

	for _, store := range hm.stores {
		_ = store.CloseAll()
	}
}
